type Term is USize
type LogIndex is USize
type Votes is USize
trait NodeState[A: Any val]
  fun ref append(
    node: RaftNode[A] ref, 
    leader: RaftNode[A] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: (ReadSeq[A] val | None) = None,
    leader_commit_index: LogIndex
  ) => None

  fun ref request_vote(
    node: RaftNode[A] ref, 
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) => None

  fun ref append_reply(
    node: RaftNode[A] ref,
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex
  ) => None

  fun ref vote_reply(
    node: RaftNode[A] ref,
    term: Term,
    vote_granted: Bool
  ) => None

  fun ref process_commands(
    node: RaftNode[A] ref,
    commands: (ReadSeq[A] val | None) = None
  ) => None
