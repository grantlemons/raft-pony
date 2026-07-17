use "time"
use "random"
use "debug"
use "itertools"

class LeaderState[A: Any val] is NodeState[A]
  let _followers: Array[RaftNode[A] tag] ref
  let _follower_next_indexes: Array[LogIndex val] ref
  let _follower_match_indexes: Array[LogIndex val] ref

  new create(parent: RaftNode[A] ref, nodes: Array[RaftNode[A] tag] val) =>
    _followers = Iter[RaftNode[A] tag](nodes.values())
      .filter({(node: RaftNode[A] tag) => not (node is parent)})
      .collect(Array[RaftNode[A] tag])
    _follower_next_indexes = Iter[RaftNode[A] tag](_followers.values())
      .map[LogIndex val]({(node: RaftNode[A] tag) => parent._get_last_log_idx() + 1})
      .collect(Array[LogIndex val])
    _follower_match_indexes = Iter[RaftNode[A] tag](_followers.values())
      .map[LogIndex val]({(node: RaftNode[A] tag) => 0})
      .collect(Array[LogIndex val])

  fun ref append_reply(term: Term, success: Bool) =>
    None
  fun ref vote_reply(term: Term, vote_granted: Bool) =>
    None

class CandidateState[A: Any val] is NodeState[A]
  fun ref append_reply(term: Term, success: Bool) =>
    None
  fun ref vote_reply(term: Term, vote_granted: Bool) =>
    None

class FollowerState[A: Any val] is NodeState[A]
  fun ref append_reply(term: Term, success: Bool) =>
    None
  fun ref vote_reply(term: Term, vote_granted: Bool) =>
    None

actor RaftNode[A: Any val]
  let _name: String val
  var _state: NodeState[A] = FollowerState[A]

  let _timers: Timers = Timers
  var _rand: Rand
  var _election_timer: (Timer tag | None) = None
  var _heartbeat_timer: (Timer tag | None) = None

  let _log: Array[A] ref = []
  let _log_terms: Array[Term val] ref = []
  let _log_votes: Array[Votes val] ref = []

  var _current_term: Term = 0
  var _voted_for: (RaftNode[A] tag | None) = None
  var _commit_index: LogIndex = 0
  var _last_applied: LogIndex = 0

  new create(name: String val) =>
    _name = name
    _rand = Rand.from_u64(_name.hash64())
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

  fun ref _get_log(): Array[A] ref! => _log
  fun _get_current_term(): Term => _current_term
  fun _get_voted_for(): (RaftNode[A] tag! | None) => _voted_for
  fun _get_commit_index(): LogIndex => _commit_index
  fun _get_last_applied(): LogIndex => _last_applied
  fun _get_last_log_idx(): LogIndex => _log.size() - 1
  fun _get_log_term(idx: LogIndex): (Term | None) => try _log_terms(idx)? end
  fun _get_log_votes(idx: LogIndex): (Votes | None) => try _log_votes(idx)? end

  fun ref _set_current_term(term: Term) => _current_term = term
  fun ref _set_voted_for(voted_for: (RaftNode[A] tag! | None)) => _voted_for = voted_for
  fun ref _set_commit_index(idx: LogIndex) => _commit_index = idx
  fun ref _set_last_applied(idx: LogIndex) => _last_applied = idx

  be append_reply(term: Term, success: Bool) => _state.append_reply(term, success)
  be vote_reply(term: Term, vote_granted: Bool) => _state.vote_reply(term, vote_granted)

  fun ref append(
    leader: RaftNode[A] tag,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Array[A] val,
    leader_commit_index: LogIndex
  ) =>
    _state.append(
      this,
      leader,
      term,
      prev_log_index,
      prev_log_term,
      entries,
      leader_commit_index
    )


  fun ref request_vote(
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    _state.request_vote(
      this,
      candidate,
      term,
      last_log_index,
      last_log_term
    )
