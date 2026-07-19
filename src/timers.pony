use "time"
use "debug"

class ElectionTimeoutHandler[A: Any val] is TimerNotify
  let _parent: RaftNode[A] tag
  new iso create(parent: RaftNode[A] tag) => _parent = parent

  fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
    Debug("Election timeout!")
    _parent.become_candidate()
    false

class HeartbeatHandler[A: Any val] is TimerNotify
  let _parent: RaftNode[A] tag
  new iso create(parent: RaftNode[A] tag) => _parent = parent

  fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
    Debug("Heartbeat!")
    _parent.process_commands([])
    true
