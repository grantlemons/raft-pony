use ".."
use "time"
use "itertools"
use "debug"

class LeaderState[A: Any val, M: StateMachine[A]] is NodeState[A, M]
  let _followers: Array[(RaftNode[A, M] tag, LogIndex, LogIndex)] ref
  let _timers: Timers = Timers
  var _heartbeat_timer: Timer tag

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
  // Repeat during idle periods to prevent election timeouts
  new create(parent: RaftNode[A, M] ref, nodes: Iterator[RaftNode[A, M] tag] ref) =>
    _followers = Iter[RaftNode[A, M] tag](nodes)
      .filter({(node: RaftNode[A, M] tag) => not (node is parent)})
      .map[(RaftNode[A, M] tag, LogIndex, LogIndex)]({
        (node: RaftNode[A, M] tag) => (node, parent.get_last_log_idx() + 1, 0)
      })
      .collect(Array[(RaftNode[A, M] tag, LogIndex, LogIndex)])

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
    follower_info: (RaftNode[A, M] tag, LogIndex, LogIndex)
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
    try
      (
        let follower: RaftNode[A, M] tag,
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
