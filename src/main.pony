use "collections"

actor Main
  new create(env: Env) =>
    let nodes = recover val
      let nd = Array[Node[U8] tag]
      for i in Range(0, 3) do
        nd.push(Node[U8](i.string()))
      end
      nd
    end
    for node in nodes.values() do
      node.set_nodes(nodes)
    end
