use "time"
use "random"
use "debug"
use "itertools"
use "collections"
use "assert"

class LeaderState[A: Any val] is NodeState[A]
  let _followers: Array[(RaftNode[A] tag, LogIndex, LogIndex)] ref
  let _timers: Timers = Timers
  var _heartbeat_timer: Timer tag

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
  // Repeat during idle periods to prevent election timeouts
  new create(parent: RaftNode[A] ref, nodes: Iterator[RaftNode[A] tag] ref) =>
    _followers = Iter[RaftNode[A] tag](nodes)
      .filter({(node: RaftNode[A] tag) => not (node is parent)})
      .map[(RaftNode[A] tag, LogIndex, LogIndex)]({
        (node: RaftNode[A] tag) => (node, parent.get_last_log_idx() + 1, 0)
      })
      .collect(Array[(RaftNode[A] tag, LogIndex, LogIndex)])

    let timer: Timer iso = Timer(HeartbeatHandler[A](parent), 100_000_000, 100_000_000)
    _heartbeat_timer = timer
    _timers(consume timer)

    process_commands(parent)
    parent.gateway.set_leader(parent)

  fun ref restart_heartbeat_timer(node: RaftNode[A] ref) =>
    _timers.cancel(_heartbeat_timer)
    let timer = Timer(HeartbeatHandler[A](node), 75_000_000, 75_000_000)
    _heartbeat_timer = timer
    _timers(consume timer)

  // TODO: Respond after applying to state machine
  fun ref process_commands(
    node: RaftNode[A] ref,
    commands: (ReadSeq[A] val | None) = None
  ) =>
    match commands
    | let arr: ReadSeq[A] val =>
      node.log.append(arr)
      node.log_terms.concat(Iter[Term].repeat_value(node.current_term).take(arr.size()))
    end

    for (follower_id, follower_info) in _followers.pairs() do
      _send_update(node, follower_id, follower_info)
    end

  // If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
  // - If successful: update nextIndex and matchIndex for follower
  // - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
  fun ref _send_update(
    node: RaftNode[A] ref,
    follower_id: USize,
    follower_info: (RaftNode[A] tag, LogIndex, LogIndex)
  ) =>
    (let follower, let next_index, let match_index) = follower_info

    let sendable_entries =
      if node.log.size() > next_index then
        let s: Array[A] iso = Array[A](node.log.size() - next_index)
        if node.get_last_log_idx() >= next_index then
          let entries = node.log.slice(next_index)
          for entry in entries.values() do
            s.push(entry)
          end
        end
        consume s
      else None
      end

    let prev_index = next_index - 1
    let prev_term = try node.log_terms(prev_index)? else -1 end
    follower.append(
      node,
      follower_id,
      node.current_term,
      prev_index,
      prev_term,
      consume sendable_entries,
      node.commit_index
    )

  // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
  fun ref append_reply(
    node: RaftNode[A] ref,
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex
  ) =>
    try
      (
        let follower: RaftNode[A] tag,
        let next_index: LogIndex,
        let match_index': LogIndex
      ) = _followers(follower_id)?
      if success then
        _followers.update(follower_id, (follower, match_index + 1, match_index))?
      else
        let new_follower_info = (follower, LogIndex.min_value().max(next_index - 1), match_index')
        _followers.update(follower_id, new_follower_info)?
        _send_update(node, follower_id, new_follower_info)
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
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: (ReadSeq[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    try
      Assert(not (leader is node), "Append message sent to self!")?

      // Consider a new leader to be one of the current term or later.
      // This accounts for the current term being the previous term + 1
      if term >= node.current_term then
        node.state = FollowerState[A]
      else
        leader.append_reply(follower_id, node.current_term, false)
      end
    end

  // If votes received from majority of servers: become leader
  fun ref vote_reply(node: RaftNode[A] ref, term: Term, vote_granted: Bool) =>
    if vote_granted then _votes = _votes + 1 end

    let consensus: Bool = _votes >= (((Votes.from[USize](_voters.size()) + 1) / 2) + 1)
    if consensus then
      node.state = LeaderState[A](node, _voters.values())
    end

class FollowerState[A: Any val] is NodeState[A]
  fun ref append(
    node: RaftNode[A] ref,
    leader: RaftNode[A] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries': (ReadSeq[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    if term < node.current_term then
      leader.append_reply(follower_id, node.current_term, false)
      return
    end
    let entries =
      match entries'
      | let seq: ReadSeq[A] val if seq.size() != 0 => seq
      else
        leader.append_reply(follower_id, node.current_term, true, prev_log_index)
        return
      end

    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    if prev_log_index != -1 then
      match node.get_log_term(prev_log_index)
      | None =>
        leader.append_reply(follower_id, node.current_term, false)
        Debug(node.name + ": Failed to append, entry at prevLogIndex ("+prev_log_index.string()+") null.")
        return
      | let term': Term if term' != prev_log_term =>
        leader.append_reply(follower_id, node.current_term, false)
        Debug(node.name + ": Failed to append, term at prevLogIndex ("+prev_log_index.string()+") wrong. Term: " + term'.string() + " expected " + term.string())
        return
      end
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
    node.log.append(entries, num_already_included)
    node.log_terms.concat(Iter[Term].repeat_value(term).take(entries.size() - num_already_included))

    if leader_commit_index > node.commit_index then
      node.commit_index = leader_commit_index.min(node.get_last_log_idx())
    end

    leader.append_reply(follower_id, node.current_term, true, prev_log_index + entries.size())
    Debug(node.name + ": Success! #Entries: " + entries.size().string() + " New match: " + (prev_log_index + entries.size()).string())

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
