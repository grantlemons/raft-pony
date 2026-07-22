use ".."
use "itertools"
use "debug"

class CandidateState[A: Any val, M: StateMachine[A]] is NodeState[A, M]
  let _voters: Array[RaftNode[A, M] tag] ref
  var _votes: Votes = 0

  new create(parent: RaftNode[A, M] ref, nodes: Iterator[RaftNode[A, M] tag] ref) =>
    _voters = Iter[RaftNode[A, M] tag](nodes)
      .filter({(node: RaftNode[A, M] tag) => not (node is parent)})
      .collect(Array[RaftNode[A, M] tag])
    begin_election(parent)

  // On conversion to candidate, start election:
  // - Increment currentTerm
  // - Vote for self
  // - Reset election timer
  // - Send RequestVote RPCs to all other servers
  fun ref begin_election(node: RaftNode[A, M] ref) =>
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
    node: RaftNode[A, M] ref,
    leader: RaftNode[A, M] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: (Term | Empty),
    entries: (ReadSeq[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    // Consider a new leader to be one of the current term or later.
    // This accounts for the current term being the previous term + 1
    match term
    | let term': USize =>
      if node.current_term <= term' then
        Debug(node.name + ": Converting to follower! Term: " + term.string())
        node.state = FollowerState[A, M]
        
        // rerun on new follower
        node.state.append(
          node,
          leader,
          follower_id,
          term,
          prev_log_index,
          prev_log_term,
          entries,
          leader_commit_index
        )
      else
        leader.append_reply(follower_id, node.current_term, false)
      end
    end

  // If votes received from majority of servers: become leader
  fun ref vote_reply(node: RaftNode[A, M] ref, term: Term, vote_granted: Bool) =>
    if node.rand.u8() == 0 then
      Debug(node.name + ": Simulating dropped vote reply")
      return
    end
    if vote_granted then _votes = _votes + 1 end

    let consensus: Bool = _votes >= (((Votes.from[USize](_voters.size()) + 1) / 2) + 1)
    if consensus then
      node.state = LeaderState[A, M](node, _voters.values())
    end
