use "time"
use "random"
use "debug"
use "itertools"
use "states"

trait StateMachine[A: Any val]
  new tag create()
  be apply(input: A)

actor RaftNode[A: Any val, M: StateMachine[A]]
  let name: String val
  var state: NodeState[A, M] = FollowerState[A, M]
  let gateway: RaftCluster[A, M]
  var nodes: Array[RaftNode[A, M] tag] val = []

  let _timers: Timers = Timers
  var rand: Rand
  var _election_timer: Timer tag

  let log: Array[A] ref = []
  let log_terms: Array[Term val] ref = []
  let log_votes: Array[Votes val] ref = []

  var current_term: Term = 0
  var voted_for: (RaftNode[A, M] tag | None) = None
  var commit_index: LogIndex = 0
  var last_applied: LogIndex = 0

  let _state_machine: StateMachine[A] tag

  new create(gateway': RaftCluster[A, M], state_machine: StateMachine[A] tag, name': String val) =>
    gateway = gateway'
    name = name'
    rand = Rand.from_u64(name'.hash64())
    _state_machine = state_machine

    let timeout_ms = 150 + ((150 * U64.from[U8](rand.u8())) / 255)
    let timer: Timer iso = Timer(ElectionTimeoutHandler[A, M](this), timeout_ms * 1_000_000)
    _election_timer = timer
    _timers(consume timer)

  be dispose() =>
    state.dispose()
    nodes = []
    _timers.cancel(_election_timer)

  be set_nodes(nodes': Array[RaftNode[A, M] tag] val) => nodes = nodes'

  fun ref restart_election_timer() =>
    _timers.cancel(_election_timer)
    let timeout_ms = 150 + ((150 * U64.from[U8](rand.u8())) / 255)
    let timer = Timer(ElectionTimeoutHandler[A, M](this), timeout_ms * 1_000_000)
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
      voted_for = None
      match state
      | let _: FollowerState[A, M] => None
      else
        state = FollowerState[A, M]
      end
    end

  // Apply an input in the log to the managed state machine
  be apply_input(idx: LogIndex) =>
    last_applied = idx
    try
      _state_machine.apply(log(idx)?)
    else Debug("Unable to apply to state machine!")
    end

  // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
  be commit() =>
    while commit_index > last_applied do
      apply_input(last_applied + 1)
    end

  be become_candidate() =>
    Debug(name + ": Became candidate")
    state = CandidateState[A, M](this, nodes.values())

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
    leader: RaftNode[A, M] tag,
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
    candidate: RaftNode[A, M] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    restart_election_timer()
    check_superceded(term)
    state.request_vote(this, candidate, term, last_log_index, last_log_term)
