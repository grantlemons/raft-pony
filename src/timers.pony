use "time"
use "debug"

class ElectionTimeoutHandler[A: Any val, M: StateMachine[A]] is TimerNotify
  let _parent: RaftNode[A, M] tag
  new iso create(parent: RaftNode[A, M] tag) => _parent = parent

  fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
    _parent.become_candidate()
    Debug("Election timer!")
    false

class HeartbeatHandler[A: Any val, M: StateMachine[A]] is TimerNotify
  let _parent: RaftNode[A, M] tag
  new iso create(parent: RaftNode[A, M] tag) => _parent = parent

  fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
    _parent.process_commands(None)
    Debug("Heartbeat!")
    true
