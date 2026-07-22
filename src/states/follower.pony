use ".."
use "debug"
use "collections"
use "itertools"

class FollowerState[A: Any val, M: StateMachine[A]] is NodeState[A, M]
  fun ref append(
    node: RaftNode[A, M] ref,
    leader: RaftNode[A, M] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: (Term | Empty),
    entries': (ReadSeq[A] val | None),
    leader_commit_index: LogIndex
  ) =>
    if node.rand.u8() == 0 then
      Debug(node.name + ": Simulating dropped append message")
      return
    end
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
    match (prev_log_index, node.get_log_entry(prev_log_index))
    | (let _: USize, None) =>
      leader.append_reply(follower_id, node.current_term, false)
      Debug(node.name + ": Failed to append, entry at prevLogIndex ("+prev_log_index.string()+") null.")
      return
    | (let _: USize, let term': Term) if EmptyFuns.ne(term', prev_log_term) =>
      leader.append_reply(follower_id, node.current_term, false)
      Debug(node.name + ": Failed to append, term at prevLogIndex ("+prev_log_index.string()+") wrong. Term: " + term'.string() + " expected " + term.string())
      return
    end

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    for idx in Range(node.log.size(), node.log.size() + entries.size()) do
      match node.get_log_term(idx)
      | let log_term: Term if log_term != term =>
        node.log.truncate(idx)
      end
    end

    // Append any new entries not already in the log
    let num_already_included = 
      match prev_log_index
      | let prev_idx': USize => prev_idx' + 1
      else 0
      end
    node.log.append(entries, num_already_included)
    node.log_terms.concat(Iter[Term].repeat_value(term).take(entries.size() - num_already_included))

    if EmptyFuns.gt(leader_commit_index, node.commit_index) then
      node.commit_index = EmptyFuns.min(leader_commit_index, node.get_last_log_idx())
    end

    leader.append_reply(follower_id, node.current_term, true, prev_log_index + entries.size())
    Debug(node.name + ": Success! #Entries: " + entries.size().string() + " New match: " + (prev_log_index + entries.size()).string())

  fun ref request_vote(
    node: RaftNode[A, M] ref,
    candidate: RaftNode[A, M] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: (Term | Empty)
  ) =>
    if node.rand.u8() == 0 then
      Debug(node.name + ": Simulating dropped vote message")
      return
    end
    if term < node.current_term then
      candidate.vote_reply(node.current_term, false)
      return
    end

    // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
    let not_voted_for_other = (node.voted_for is None) or (node.voted_for is candidate)
    let index_up_to_date = EmptyFuns.ge(last_log_index, node.get_last_log_idx())
    let term_up_to_date = EmptyFuns.ge(last_log_term, node.get_last_log_term())

    Debug(node.name + ": Follower last term = " + node.get_last_log_term().string() + " Candidate last term = " + last_log_term.string())
    if not_voted_for_other and index_up_to_date and term_up_to_date then
      node.voted_for = candidate
      candidate.vote_reply(node.current_term, true)
    else
      candidate.vote_reply(node.current_term, false)
    end
