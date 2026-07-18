use "time"
use "random"
use "debug"
use "itertools"
use "collections"

class LeaderState[A: Any val] is NodeState[A]
  let _followers: Array[(RaftNode[A] tag, LogIndex, LogIndex)] ref

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
  // Repeat during idle periods to prevent election timeouts
  new create(parent: RaftNode[A] ref, nodes: Iterator[RaftNode[A] tag] ref) =>
    _followers = Iter[RaftNode[A] tag](nodes)
      .filter({(node: RaftNode[A] tag) => not (node is parent)})
      .map[(RaftNode[A] tag, LogIndex, LogIndex)]({
        (node: RaftNode[A] tag) => (node, parent.get_last_log_idx() + 1, 0)
      })
      .collect(Array[(RaftNode[A] tag, LogIndex, LogIndex)])
    process_commands(parent)

  // TODO: Respond after applying to state machine
  // TODO: Tie replies to follower index
  fun ref process_commands(node: RaftNode[A] ref, commands: Array[A] val = []) =>
    node.log.concat(commands.values())

    for (follower_id, follower_info) in _followers.pairs() do
      send_update(node, follower_id, follower_info)
    end

  // If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
  // - If successful: update nextIndex and matchIndex for follower
  // - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
  fun ref send_update(
    node: RaftNode[A] ref,
    follower_id: USize,
    follower_info: (RaftNode[A] tag, LogIndex, LogIndex)
  ) =>
    (let follower, let next_index, let match_index) = follower_info

    let sendable_entries: Array[A] iso = Array[A](node.log.size() - next_index)
    if node.get_last_log_idx() > next_index then
      let entries = node.log.slice(next_index)
      for entry in entries.values() do
        sendable_entries.push(entry)
      end
    end

    follower.append(
      node,
      node.current_term,
      node.get_last_log_idx(),
      node.get_last_log_term(),
      consume sendable_entries,
      node.commit_index
    )

  // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
  fun ref append_reply(node: RaftNode[A] ref, term: Term, success: Bool) =>
    let id: USize = 0
    try
      (let follower: RaftNode[A] tag, let next_index: LogIndex, let match_index: LogIndex) = _followers(id)?
      if success then
        None // TODO: Update nextIndex and matchIndex for follower
        let new_next_idx: LogIndex = 0
        let new_match_idx: LogIndex = 0
        _followers.update(id, (follower, new_next_idx, new_match_idx))?
      else
        let new_follower_info = (follower, next_index - 1, match_index)
        _followers.update(id, new_follower_info)?
        send_update(node, id, new_follower_info)
      end
    end

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
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Array[A] val,
    leader_commit_index: LogIndex
  ) =>
    // Consider a new leader to be one of the current term or later.
    // This accounts for the current term being the previous term + 1
    if term >= node.current_term then
      if leader is node then Debug("ERROR: Append message sent to self!") end
      node.state = FollowerState[A]
    else
      leader.append_reply(node.current_term, false)
    end

  // If votes received from majority of servers: become leader
  fun ref vote_reply(node: RaftNode[A] ref, term: Term, vote_granted: Bool) =>
    _votes = _votes + 1

    let consensus: Bool = _votes >= (((Votes.from[USize](_voters.size()) + 1) / 2) + 1)
    if consensus then
      node.state = LeaderState[A](node, _voters.values())
    end

class FollowerState[A: Any val] is NodeState[A]
  fun ref append(
    node: RaftNode[A] ref,
    leader: RaftNode[A] tag,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Array[A] val,
    leader_commit_index: LogIndex
  ) =>
    if term < node.current_term then
      leader.append_reply(node.current_term, false)
      return
    end

    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    match node.get_log_term(prev_log_index)
    | None
    | let term': Term if term' != prev_log_term =>
      leader.append_reply(node.current_term, false)
      return
    else
      None
    end

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    for idx in Range(node.log.size(), node.log.size() + entries.size()) do
      match node.get_log_term(idx)
      | let log_term: Term if log_term != term =>
        node.log.truncate(idx)
      end
    end

    // Append any new entries not already in the log
    let num_already_included = node.get_last_log_idx() - prev_log_index
    entries.copy_to(
      node.log,
      num_already_included, // 0 offset by number to skip
      node.log.size(), // dst idx
      entries.size() - num_already_included
    )

    if leader_commit_index > node.commit_index then
      node.commit_index = leader_commit_index.min(node.get_last_log_idx())
    end

    leader.append_reply(node.current_term, true)

  fun ref request_vote(
    node: RaftNode[A] ref,
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    if term < node.current_term then
      candidate.vote_reply(node.current_term, false)
      return
    end

    // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
    let not_voted = node.voted_for is None
    let up_to_date = last_log_index >= node.get_last_log_idx()
    if not_voted or up_to_date then
      candidate.vote_reply(node.current_term, true)
    else
      candidate.vote_reply(node.current_term, false)
    end
