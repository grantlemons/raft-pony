use "time"
use "collections"

type Term is USize
type LogIndex is USize
type Votes is U16
trait NodeState[A: Any val]
  fun tag append(
    node: RaftNode[A] ref, 
    leader: RaftNode[A] tag,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Array[A] val,
    leader_commit_index: LogIndex
  ) =>
    let current_term = node._get_current_term()
    if term < current_term then
      leader.append_reply(current_term, false)
    end

    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    match node._get_log_term(prev_log_index)
    | None
    | let idx: LogIndex if idx != prev_log_term =>
      leader.append_reply(current_term, false)
    else
      None
    end

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    let log: Array[A] ref! = node._get_log()
    for idx in Range(log.size(), log.size() + entries.size()) do
      match node._get_log_term(idx)
      | let log_term: Term if log_term != term =>
        log.truncate(idx)
      end
    end

    // Append any new entries not already in the log
    let num_already_included = node._get_last_log_idx() - prev_log_index
    entries.copy_to(
      log,
      num_already_included, // 0 offset by number to skip
      log.size(), // dst idx
      entries.size() - num_already_included
    )

    if leader_commit_index > node._get_commit_index() then
      node._set_commit_index(leader_commit_index.min(node._get_last_log_idx()))
    end

    leader.append_reply(current_term, true)

  fun tag request_vote(
    node: RaftNode[A] ref!, 
    candidate: RaftNode[A] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: Term
  ) =>
    let current_term = node._get_current_term()
    if term < current_term then
      candidate.vote_reply(current_term, false)
      return
    end

    // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
    let not_voted = node._get_voted_for() is None
    let up_to_date = last_log_index >= node._get_last_log_idx()
    if not_voted or up_to_date then
      candidate.vote_reply(current_term, true)
    else
      candidate.vote_reply(current_term, false)
    end

  fun tag append_reply(term: Term, success: Bool)
  fun tag vote_reply(term: Term, vote_granted: Bool)
