use ".."

trait NodeState[A: Any val, M: StateMachine[A]]
  fun ref append(
    node: RaftNode[A, M] ref, 
    leader: RaftNode[A, M] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: (ReadSeq[A] val | None) = None,
    leader_commit_index: LogIndex
  ) => None

  fun ref request_vote(
    node: RaftNode[A, M] ref, 
    candidate: RaftNode[A, M] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) => None

  fun ref append_reply(
    node: RaftNode[A, M] ref,
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex
  ) => None

  fun ref vote_reply(
    node: RaftNode[A, M] ref,
    term: Term,
    vote_granted: Bool
  ) => None

  fun ref process_commands(
    node: RaftNode[A, M] ref,
    commands: (ReadSeq[A] val | None) = None
  ) => None

  fun ref dispose() => None
