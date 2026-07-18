use "time"
use "random"
use "debug"
use "itertools"

actor RaftNode[A: Any val]
  let name: String val
  var state: NodeState[A] = FollowerState[A]

  let _timers: Timers = Timers
  var _rand: Rand
  var _election_timer: (Timer tag | None) = None
  var _heartbeat_timer: (Timer tag | None) = None

  let log: Array[A] ref = []
  let log_terms: Array[Term val] ref = []
  let log_votes: Array[Votes val] ref = []

  var current_term: Term = 0
  var voted_for: (RaftNode[A] tag | None) = None
  var commit_index: LogIndex = 0
  var last_applied: LogIndex = 0

  new create(name': String val) =>
    name = name'
    _rand = Rand.from_u64(name'.hash64())
    restart_election_timer()

  fun ref restart_election_timer() =>
    match _election_timer
    | let timer: Timer tag => _timers.cancel(timer)
    | None => None
    end
    let timeout_ms = 150 + ((150 * U64.from[U8](_rand.u8())) / 255)
    //let timer: Timer iso = Timer(ElectionTimeoutHandler[A](this), timeout_ms * 1_000_000)
    //_election_timer = timer
    //_timers(consume timer)

  fun get_last_log_idx(): LogIndex => log.size() - 1
  fun get_last_log_term(): Term => try log_terms(get_last_log_idx())? else 0 end
  fun get_log_term(idx: LogIndex): (Term | None) => try log_terms(idx)? end
  fun get_log_votes(idx: LogIndex): (Votes | None) => try log_votes(idx)? end

  // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
  fun ref check_superceded(term: Term) =>
    if term > current_term then
      current_term = term
    end
    match state
    | let _: FollowerState[A] => None
    else
      state = FollowerState[A]
    end

  // Apply an input in the log to the managed state machine
  be apply_input(idx: LogIndex) =>
    last_applied = idx

  // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
  be commit() =>
    while commit_index > last_applied do
      apply_input(last_applied + 1)
    end

  be append_reply(term: Term, success: Bool) =>
    check_superceded(term)
    state.append_reply(this, term, success)
    commit()

  be vote_reply(term: Term, vote_granted: Bool) =>
    check_superceded(term)
    state.vote_reply(this, term, vote_granted)

  be append(
    leader: RaftNode[A] tag,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Array[A] val,
    leader_commit_index: LogIndex
  ) =>
    check_superceded(term)
    state.append(this, leader, term, prev_log_index, prev_log_term, entries, leader_commit_index)
    commit()

  be request_vote(
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    check_superceded(term)
    state.request_vote(this, candidate, term, last_log_index, last_log_term)
