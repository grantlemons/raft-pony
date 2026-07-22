use "collections"
use "debug"

actor RaftCluster[A: Any val, M: StateMachine[A]]
  var _leader: (RaftNode[A, M] tag | None) = None
  var _command_queue: Array[A] iso = []
  var _nodes: Array[RaftNode[A, M] tag] ref = []

  new create(size: USize) =>
    for i in Range(0, size) do
      _nodes.push(RaftNode[A, M](this, M.create(), i.string()))
    end
    update_nodes()

  be dispose() =>
    _leader = None
    for node in _nodes.values() do node.dispose() end
    _command_queue.clear()
    _nodes.clear()

  fun update_nodes() =>
    let nodes: Array[RaftNode[A, M] tag] iso = nodes.create()
    for node in _nodes.values() do
      nodes.push(node)
    end
    let nodes': Array[RaftNode[A, M] tag] val = consume nodes
    for node in nodes'.values() do
      node.set_nodes(nodes')
    end

  be scale_to(size: USize) =>
    for node in _nodes.values() do
      node.set_nodes([])
    end
    while size > _nodes.size() do
      _nodes.push(RaftNode[A, M](this, M.create(), _nodes.size().string()))
    end
    if size == 0 then
      _leader = None
      _nodes.clear()
    end

    // downsize
    try
      while size < _nodes.size() do
        let last_index = _nodes.size() - 1
        if _nodes(last_index)? is _leader then
          _nodes(last_index - 1)?.dispose()
          _nodes.delete(last_index - 1)?
        else
          _nodes(last_index)?.dispose()
          _nodes.pop()?
        end
      end
    end
    Debug("Scaled to " + _nodes.size().string())
    update_nodes()

  fun ref send_commands() =>
    match _leader
    | let leader: RaftNode[A, M] tag =>
        leader.process_commands(_command_queue = [])
    end

  be set_leader(leader: RaftNode[A, M] tag) =>
    _leader = leader
    send_commands()

  be process_commands(commands: ReadSeq[A] val) =>
    _command_queue.append(commands)
    send_commands()
