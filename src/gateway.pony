use "collections"

actor RaftGateway[A: Any val]
  var _leader: (RaftNode[A] tag | None) = None
  var _command_queue: Array[A] iso = []

  be set_leader(leader: RaftNode[A] tag) => _leader = leader
  be process_commands(commands: Array[A] val) =>
    _command_queue.append(commands)
    match _leader
    | let leader: RaftNode[A] tag =>
        leader.process_commands(_command_queue = [])
    else None
    end
