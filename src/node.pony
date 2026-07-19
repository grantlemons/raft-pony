use "time"
use "random"
use "debug"
use "itertools"

actor RaftNode[A: Any val]
  let name: String val
  var state: NodeState[A] = FollowerState[A]
  let gateway: RaftGateway[A]
  let nodes: Array[RaftNode[A] tag] = []

  let _timers: Timers = Timers
  var _rand: Rand
  var _election_timer: Timer tag

  let log: Array[A] ref = []
  let log_terms: Array[Term val] ref = []
  let log_votes: Array[Votes val] ref = []

  var current_term: Term = 0
  var voted_for: (RaftNode[A] tag | None) = None
  var commit_index: LogIndex = 0
  var last_applied: LogIndex = 0

  new create(gateway': RaftGateway[A], name': String val) =>
    gateway = gateway'
    name = name'
    _rand = Rand.from_u64(name'.hash64())

    let timeout_ms = 150 + ((150 * U64.from[U8](_rand.u8())) / 255)
    let timer: Timer iso = Timer(ElectionTimeoutHandler[A](this), timeout_ms * 1_000_000)
    _election_timer = timer
    _timers(consume timer)

  be add_nodes(nodes': ReadSeq[RaftNode[A] tag] val) => nodes.append(nodes')

  fun ref restart_election_timer() =>
    _timers.cancel(_election_timer)
    let timeout_ms = 150 + ((150 * U64.from[U8](_rand.u8())) / 255)
    let timer = Timer(ElectionTimeoutHandler[A](this), timeout_ms * 1_000_000)
    _election_timer = timer
    _timers(consume timer)

  fun get_last_log_idx(): LogIndex => log.size() - 1
  fun get_last_log_term(): Term => try log_terms(get_last_log_idx())? else 0 end
  fun get_log_term(idx: LogIndex): (Term | None) => try log_terms(idx)? end
  fun get_log_votes(idx: LogIndex): (Votes | None) => try log_votes(idx)? end

  // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
  fun ref check_superceded(term: Term) =>
    if term > current_term then
      current_term = term
      match state
      | let _: FollowerState[A] => None
      else
        state = FollowerState[A]
      end
    end

  // Apply an input in the log to the managed state machine
  be apply_input(idx: LogIndex) =>
    last_applied = idx

  // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
  be commit() =>
    while commit_index > last_applied do
      apply_input(last_applied + 1)
    end

  be become_candidate() =>
    Debug(name + ": Became candidate")
    state = CandidateState[A](this, nodes.values())

  be process_commands(commands: (ReadSeq[A] val | None) = None) =>
    state.process_commands(this, commands)

  be append_reply(
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex = -1
  ) =>
    restart_election_timer()
    check_superceded(term)
    state.append_reply(this, follower_id, term, success, match_index)
    commit()

  be vote_reply(term: Term, vote_granted: Bool) =>
    restart_election_timer()
    check_superceded(term)
    state.vote_reply(this, term, vote_granted)

  be append(
    leader: RaftNode[A] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: (Array[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    restart_election_timer()
    check_superceded(term)
    state.append(this, leader, follower_id, term, prev_log_index, prev_log_term, entries, leader_commit_index)
    commit()

  be request_vote(
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    restart_election_timer()
    check_superceded(term)
    state.request_vote(this, candidate, term, last_log_index, last_log_term)
