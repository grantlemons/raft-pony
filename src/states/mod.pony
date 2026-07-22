use ".."

class val Empty is Stringable
  fun box string(): String iso^ => "Empty".string()
  fun add(value: USize val): USize => value - 1
  fun eq(other: Empty): Bool => true

primitive EmptyFuns
  fun map[A: Any val, B: Any val = A](value: (A | Empty), func: { (A): B } val): (B | Empty) =>
    match value
    | let value': A => func(value')
    else Empty
    end
  fun combine[A: Any val, B: Any val](
    a: (A | Empty),
    b: (A | Empty),
    func: { (A, A): B } val
  ): (B | Empty) =>
    EmptyFuns.map[A, (B | Empty)](a, {
      (a')(func) => EmptyFuns.map[A, B](b, {
        (b')(func) => func(a', b')
      })
    })
  fun add(a: LogIndex, b: LogIndex): (USize | Empty) =>
    combine[USize, USize](a, b, { (av: USize, bv: USize) => av.add(bv) })
  fun sub(a: LogIndex, b: LogIndex): (USize | Empty) =>
    combine[USize, USize](a, b, { (av: USize, bv: USize) => av.sub(bv) })
  fun lt(a: LogIndex, b: LogIndex): Bool =>
    match (a, b)
    | (let a': USize, let b': USize) => a' < b'
    | (let _: Empty, let _: USize) => true
    else false
    end
  fun le(a: LogIndex, b: LogIndex): Bool =>
    match (a, b)
    | (let a': USize, let b': USize) => a' <= b'
    | (let _: Empty, let _: USize) => true
    else false
    end
  fun gt(a: LogIndex, b: LogIndex): Bool =>
    match (a, b)
    | (let a': USize, let b': USize) => a' > b'
    | (let _: USize, let _: Empty) => true
    else false
    end
  fun ge(a: LogIndex, b: LogIndex): Bool =>
    match (a, b)
    | (let a': USize, let b': USize) => a' >= b'
    | (let _: USize, let _: Empty) => true
    else false
    end
  fun eq(a: LogIndex, b: LogIndex): Bool =>
    match (a, b)
    | (let a': USize, let b': USize) => a' == b'
    | (let _: Empty, let _: Empty) => true
    else false
    end
  fun ne(a: LogIndex, b: LogIndex): Bool => not eq(a, b)
  fun compare(a: LogIndex, b: LogIndex): Compare =>
    if eq(a, b) then
      Equal
    elseif lt(a, b) then
      Less
    else
      Greater
    end
  fun min(a: LogIndex, b: LogIndex): LogIndex =>
    match compare(a, b)
    | Less => a
    else
     b
    end
  fun max(a: LogIndex, b: LogIndex): LogIndex =>
    match compare(a, b)
    | Greater => a
    else
     b
    end

type Term is USize
type LogIndex is (USize | Empty)
type Votes is USize
trait NodeState[A: Any val, M: StateMachine[A]]
  fun ref append(
    node: RaftNode[A, M] ref, 
    leader: RaftNode[A, M] tag,
    follower_id: USize,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: (Term | Empty),
    entries: (ReadSeq[A] val | None) = None,
    leader_commit_index: LogIndex
  ) => None

  fun ref request_vote(
    node: RaftNode[A, M] ref, 
    candidate: RaftNode[A, M] tag,
    term: Term,
    last_log_index: LogIndex,
    last_log_term: (Term | Empty)
  ) => None

  fun ref append_reply(
    node: RaftNode[A, M] ref,
    follower_id: USize,
    term: Term,
    success: Bool,
    match_index: LogIndex
  ) => None

  fun ref vote_reply(
    node: RaftNode[A, M] ref,
    term: Term,
    vote_granted: Bool
  ) => None

  fun ref process_commands(
    node: RaftNode[A, M] ref,
    commands: (ReadSeq[A] val | None) = None
  ) => None

  fun ref dispose() => None
