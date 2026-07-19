use ".."
use "itertools"

class CandidateState[A: Any val] is NodeState[A]
  let _voters: Array[RaftNode[A] tag] ref
  var _votes: Votes = 0

  new create(parent: RaftNode[A] ref, nodes: Iterator[RaftNode[A] tag] ref) =>
    _voters = Iter[RaftNode[A] tag](nodes)
      .filter({(node: RaftNode[A] tag) => not (node is parent)})
      .collect(Array[RaftNode[A] tag])
    begin_election(parent)

  // On conversion to candidate, start election:
  // - Increment currentTerm
  // - Vote for self
  // - Reset election timer
  // - Send RequestVote RPCs to all other servers
  fun ref begin_election(node: RaftNode[A] ref) =>
    node.current_term = node.current_term + 1
    node.voted_for = node
    _votes = 1
    node.restart_election_timer()
    for voter in _voters.values() do
      voter.request_vote(
        node,
        node.current_term,
        node.get_last_log_idx(),
        node.get_last_log_term()
      )
    end

  // If AppendEntries RPC received from new leader: convert to follower
  fun ref append(
    node: RaftNode[A] ref,
    leader: RaftNode[A] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: (ReadSeq[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    // Consider a new leader to be one of the current term or later.
    // This accounts for the current term being the previous term + 1
    if term >= node.current_term then
      node.state = FollowerState[A]
    else
      leader.append_reply(follower_id, node.current_term, false)
    end

  // If votes received from majority of servers: become leader
  fun ref vote_reply(node: RaftNode[A] ref, term: Term, vote_granted: Bool) =>
    if vote_granted then _votes = _votes + 1 end

    let consensus: Bool = _votes >= (((Votes.from[USize](_voters.size()) + 1) / 2) + 1)
    if consensus then
      node.state = LeaderState[A](node, _voters.values())
    end
