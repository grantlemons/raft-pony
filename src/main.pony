use "time"
use "collections"
use "itertools"

actor Main
  new create(env: Env) =>
    let cluster = RaftCluster[U64](50)
    cluster.scale_to(10)

    let commands_per_sec = U64(50)
    let timers = Timers
    timers(
      Timer(
        object is TimerNotify
          fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
            cluster.process_commands([count])
            true
        end,
        0,
        1_000_000_000 / commands_per_sec
      )
    )

    cluster.dispose()
    timers.dispose()
