use ".."
use "debug"
use "collections"
use "itertools"

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
      node.voted_for = candidate
      candidate.vote_reply(node.current_term, true)
    else
      candidate.vote_reply(node.current_term, false)
    end
