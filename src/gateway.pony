use "collections"
use "debug"

actor RaftGateway[A: Any val]
  var _leader: (RaftNode[A] tag | None) = None
  var _command_queue: Array[A] iso = []

  fun ref send_commands() =>
    match _leader
    | let leader: RaftNode[A] tag =>
        leader.process_commands(_command_queue = [])
    end

  be set_leader(leader: RaftNode[A] tag) =>
    _leader = leader
    send_commands()

  be process_commands(commands: ReadSeq[A] val) =>
    _command_queue.append(commands)
    send_commands()
