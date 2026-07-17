use "time"
use "random"
use "debug"

actor Node[A: Any #share] is RaftNode[A]
  let _name: String val
  var _state: NodeState = Follower
  var _term: USize = 0
  var _election_votes: U16 = 0
  var _commit_votes: U16 = 0

  let _log: Array[A] ref = []
  var _staged_entries: (Array[A] val | None) = None
  var _batch_entries: Array[A] ref = []

  var _nodes: Array[Node[A] tag] val = []
  let _timers: Timers = Timers
  var _rand: Rand
  var _election_timer: (Timer tag | None) = None
  var _heartbeat_timer: (Timer tag | None) = None

  new create(name: String val) =>
    _name = name
    _rand = Rand.from_u64(_name.hash64())
    restart_election_timer()

  be set_nodes(nodes: Array[Node[A] tag] val) =>
    _nodes = nodes

  fun ref restart_election_timer() =>
    match _election_timer
    | let timer: Timer tag => _timers.cancel(timer)
    | None => None
    end
    let timeout_ms = 150 + ((150 * U64.from[U8](_rand.u8())) / 255)
    let timer: Timer iso = Timer(ElectionTimeoutHandler[A](this), timeout_ms * 1_000_000)
    _election_timer = timer
    _timers(consume timer)

  be begin_election() =>
    _state = Candidate
    _term = _term + 1
    _election_votes = 1
    for node in _nodes.values() do
      if not (node is this) then node.request_vote(this) end
    end

  be request_vote(candidate: RaftNode[A] tag) =>
    restart_election_timer()
    _term = _term + 1
    candidate.vote(_term)

  be vote(term: USize) =>
    _election_votes = _election_votes + 1
    let majority = _election_votes > (U16.from[USize](_nodes.size()) / 2)
    let term_matches = term == _term
    let is_candidate = _state is Candidate
    if is_candidate and majority and term_matches then
      _state = Leader
      let timer = Timer(HeartbeatTimeoutHandler[A](this), 50_000_000, 50_000_000)
      _heartbeat_timer = timer
      _timers(consume timer)
    end

  be stage_entries(leader: RaftNode[A] tag, entries: Array[A] val) =>
    match \exhaustive\ _state
    | Leader =>
      for node in _nodes.values() do
        if not (node is this) then node.stage_entries(this, entries) end
      end
    | Candidate => Debug("Candidate staging entries?")
    | Follower => Debug("Staging requested")
    end
    restart_election_timer()

    _staged_entries = entries
    leader.acknowledge_staged(entries)

  be acknowledge_staged(staged: Array[A] val) =>
    let majority = _commit_votes > (U16.from[USize](_nodes.size()) / 2)
    if majority then
      for node in _nodes.values() do node.commit_staged(staged) end
    end

  be commit_staged(staged: Array[A] val) =>
    match \exhaustive\ _staged_entries
    | let entries: Array[A] val => _log.concat(entries.values())
    | None => Debug("No staged entries!")
    end

  be heartbeat() =>
    match \exhaustive\ _state
    | Leader =>
      if _batch_entries.size() != 0 then
        for node in _nodes.values() do
          if not (node is this) then node.heartbeat() end
        end
      else
        let entries: Array[A] val = recover Array[A] end
        stage_entries(this, entries)
      end
    else
      restart_election_timer()
    end
