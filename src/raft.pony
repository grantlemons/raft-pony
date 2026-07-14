use "time"

primitive Follower
primitive Candidate
primitive Leader
type NodeState is (Follower | Candidate | Leader)

interface RaftNode[A]
  new from_leader(leader: Array[A] val)
  be begin_election()
  be request_vote(candidate: RaftNode[A] tag)
  be vote(term: USize)
  be stage_entries(leader: RaftNode[A] tag, entries: Array[A] val)
  be acknowledge_staged(staged: Array[A] val)
  be commit_staged(staged: Array[A] val)
  be heartbeat()

class ElectionTimeoutHandler[A] is TimerNotify
  let _node: RaftNode[A] tag

  new create(node: RaftNode[A] tag) => _node = node
  fun apply(timer: Timer, count: U64): Bool =>
    _node.begin_election()
    false

class HeartbeatTimeoutHandler[A] is TimerNotify
  let _leader: RaftNode[A] tag

  new create(leader: RaftNode[A] tag) => _leader = leader
  fun apply(timer: Timer, count: U64): Bool =>
    _leader.heartbeat()
    false
