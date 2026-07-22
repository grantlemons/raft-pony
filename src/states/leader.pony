use ".."
use "time"
use "itertools"
use "debug"
use "collections"

class LeaderState[A: Any val, M: StateMachine[A]] is NodeState[A, M]
  let _followers: Array[(RaftNode[A, M] tag, USize, LogIndex)] ref
  let _timers: Timers = Timers
  var _heartbeat_timer: Timer tag

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
  // Repeat during idle periods to prevent election timeouts
  new create(parent: RaftNode[A, M] ref, nodes: Iterator[RaftNode[A, M] tag] ref) =>
    _followers = Iter[RaftNode[A, M] tag](nodes)
      .filter({(node: RaftNode[A, M] tag) => not (node is parent)})
      .map[(RaftNode[A, M] tag, USize, LogIndex)]({
        (node: RaftNode[A, M] tag) => (node, parent.get_last_log_idx() + 1, Empty)
      })
      .collect(Array[(RaftNode[A, M] tag, USize, LogIndex)])

    let timer: Timer iso = Timer(HeartbeatHandler[A, M](parent), 100_000_000, 100_000_000)
    _heartbeat_timer = timer
    _timers(consume timer)

    process_commands(parent)
    parent.gateway.set_leader(parent)

  fun dispose() =>
    _timers.cancel(_heartbeat_timer)

  fun ref restart_heartbeat_timer(node: RaftNode[A, M] ref) =>
    _timers.cancel(_heartbeat_timer)
    let timer = Timer(HeartbeatHandler[A, M](node), 75_000_000, 75_000_000)
    _heartbeat_timer = timer
    _timers(consume timer)

  // TODO: Respond after applying to state machine
  fun ref process_commands(
    node: RaftNode[A, M] ref,
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
    node: RaftNode[A, M] ref,
    follower_id: USize,
    follower_info: (RaftNode[A, M] tag, USize, LogIndex)
  ) =>
    (let follower, let next_index, let match_index) = follower_info

    let sendable_entries =
      if node.log.size() > next_index then
        let s: Array[A] iso = Array[A](node.log.size() - next_index)
        if EmptyFuns.ge(node.get_last_log_idx(), next_index) then
          let entries = node.log.slice(next_index)
          for entry in entries.values() do
            s.push(entry)
          end
        end
        consume s
      else None
      end

    let prev_index: LogIndex = if next_index == 0 then Empty else next_index - 1 end
    let prev_term: (Term | Empty) = 
      match prev_index
      | Empty => Empty
      else
        match node.get_log_term(prev_index)
        | let term: Term => term
        | None =>
          Debug(node.name + ": ERROR empty log term for non-empty index " + prev_index.string())
          return
        end
      end

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
    node: RaftNode[A, M] ref,
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex
  ) =>
    if node.rand.u8() == 0 then
      Debug(node.name + ": Simulating dropped append reply")
      return
    end
    (
      let follower: RaftNode[A, M] tag,
      let next_index: USize,
      let match_index': LogIndex
    ) = try _followers(follower_id)?
        else
          Debug(node.name + ": ERROR cannot find follower at index " + follower_id.string())
          return
        end
    if success then
      try _followers.update(follower_id, (follower, match_index + 1, match_index))?
      else
        Debug(node.name + ": ERROR cannot update follower at index " + follower_id.string())
        return
      end

      // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N 

      // The N cannot become larger than new match_index update
      match match_index
      | let match_index'': USize =>
        for n in Reverse(match_index'', node.commit_index + 1) do
          let num_greater: Votes = 
            Iter[(RaftNode[A, M] tag, USize, LogIndex)](_followers.values())
              .map[LogIndex]({(f) => f._3}) // match indexes
              .filter({(idx) => EmptyFuns.ge(idx, n)})
              .count() + 1
          let consensus: Bool = num_greater >= (((Votes.from[USize](_followers.size()) + 1) / 2) + 1)
          let is_current_term: Bool =
            try node.log_terms(n)? == node.current_term
            else
              Debug(node.name + ": ERROR "+ n.string() +" is not a valid index in log terms!")
              return
            end
          if consensus and is_current_term then
            node.commit_index = n
            Debug(node.name + ": COMMIT INDEX = " + n.string())
            Debug(
              Iter[(RaftNode[A, M] tag, USize, LogIndex)](_followers.values())
                .map[LogIndex]({(f) => f._3}) // match indexes
                .collect(Array[LogIndex])
            )
            return // SUCCESS!
          end
        end
      end
    else
      Debug(node.name + ": Decrementing next index to " + (next_index.max(1) - 1).string())
      let new_follower_info = (follower, next_index.max(1) - 1, match_index')
      try _followers.update(follower_id, new_follower_info)?
      else
        Debug(node.name + ": ERROR cannot update follower at index " + follower_id.string())
        return
      end
      _send_update(node, follower_id, new_follower_info)
    end
