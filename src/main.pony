use "time"
use "collections"
use "itertools"

actor Main
  new create(env: Env) =>
    let gateway = RaftGateway[U64]
    let nodes = recover val
      let nd = Array[RaftNode[U64] tag]
      for i in Range(0, 100) do
        nd.push(RaftNode[U64](gateway, i.string()))
      end
      nd
    end
    for node in nodes.values() do
      node.add_nodes(nodes)
    end

    let commands_per_sec = U64(10)
    let timers = Timers
    timers(
      Timer(
        object is TimerNotify
          fun ref apply(timer: Timer ref, count: U64 val): Bool val =>
            gateway.process_commands([count])
            true
        end,
        0,
        1_000_000_000 / commands_per_sec
      )
    )
