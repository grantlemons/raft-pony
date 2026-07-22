use "time"
use "collections"
use "itertools"
use "debug"

actor DummyStateMachine[A: Stringable val] is StateMachine[A]
  new create() => None
  be apply(input: A) => Debug("Input: " + input.string())

actor Main
  new create(env: Env) =>
    let cluster = RaftCluster[U64, DummyStateMachine[U64]](10)

    let commands_per_sec = U64(10)
    let timers = Timers
    timers(
      Timer(
        object is TimerNotify
          var index: U64 = 0
          fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
            let old_index = index = index + 1
            cluster.process_commands([old_index])
            true
        end,
        0,
        1_000_000_000 / commands_per_sec
      )
    )
