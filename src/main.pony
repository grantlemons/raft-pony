use "collections"
use "itertools"

actor Main
  new create(env: Env) =>
    let gateway = RaftGateway[U8]
    let nodes = recover val
      let nd = Array[RaftNode[U8] tag]
      for i in Range(0, 3) do
        nd.push(RaftNode[U8](gateway, i.string()))
      end
      nd
    end
    for node in nodes.values() do
      node.add_nodes(nodes)
    end
    gateway.process_commands([0; 1; 2; 3])
