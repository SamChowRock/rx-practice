import assert from 'node:assert/strict'
import {
  AsyncSubject, BehaviorSubject, Observable, Subject, VirtualTimeScheduler,
  connectable, defer, distinctUntilChanged, finalize, interval, map, share, take, tap
} from 'rxjs'

// 题 A
function rehearseEnsemble() {
  const clock = new VirtualTimeScheduler()
  let starts = 0, stops = 0
  const conducted: string[] = []
  const sign = new BehaviorSubject<number | null>(null)
  const pulse$ = defer(() => {
    starts++
    return interval(2, clock).pipe(map(n => n % 4 + 1), finalize(() => stops++))
  }).pipe(tap(beat => {
    conducted.push(clock.now() + '/' + beat)
    sign.next(beat)
  }))
  const baton = connectable(pulse$, { connector: () => new Subject<number>() })
  const cello: string[] = [], flute: string[] = [], guest: string[] = []
  function listen(source: Observable<number>, heard: string[]) {
    return source.subscribe(beat => heard.push(clock.now() + '/' + beat))
  }
  const first = listen(baton, cello)
  clock.schedule(() => {
    const performance = baton.connect()
    clock.schedule(() => performance.unsubscribe(), 9) // 起奏后九刻，即第十刻
  }, 1)
  clock.schedule(() => {
    const second = listen(baton, flute)
    clock.schedule(() => second.unsubscribe(), 2) // 第六刻离席
  }, 4)
  clock.schedule(() => first.unsubscribe(), 6)
  clock.schedule(() => {
    assert.equal(sign.value, 3)
    listen(baton, guest)
    assert.equal(guest.length, 0)
  }, 8)
  clock.flush()
  assert.deepEqual(conducted, ['3/1','5/2','7/3','9/4'])
  assert.deepEqual(cello, ['3/1','5/2'])
  assert.deepEqual(flute, ['5/2'])
  assert.deepEqual(guest, ['9/4'])
  assert.deepEqual([starts, stops], [1,1])

  const localClock = new VirtualTimeScheduler()
  const local = interval(2, localClock).pipe(map(n => n % 4 + 1))
  const wrong: string[] = []
  localClock.schedule(() => local.pipe(take(1)).subscribe(b =>
    wrong.push(localClock.now() + '/' + b)), 4)
  localClock.flush()
  assert.deepEqual(wrong, ['6/1'])

  let passes = 0
  const score = defer(() => { passes++; return [1,2,3,4] }).pipe(share())
  const left: number[] = [], right: number[] = []
  score.subscribe(b => left.push(b))
  score.subscribe(b => right.push(b))
  assert.deepEqual(left, right)
  assert.equal(passes, 2)
  console.log('合奏通过', { cello, flute, guest, starts, stops })
}

// 题 B
type Seat = '青' | '红'
type Board = { turn: Seat; count: number; hands: Record<Seat, readonly string[]> }

function makeTable() {
  const state = new BehaviorSubject<Board>({ turn: '青', count: 0, hands: { 青: [], 红: [] } })
  const bell = new Subject<string>()
  const result = new AsyncSubject<number>()
  let ended = false
  function draw(seat: Seat, tile: string) {
    const old = state.value
    if (ended || old.turn !== seat) return
    const turn = seat === '青' ? '红' : '青'
    const hands = { ...old.hands, [seat]: [...old.hands[seat], tile] }
    state.next({ turn, count: old.count + 1, hands })
    bell.next(seat + '摸牌')
  }
  function finish(score: number) {
    if (ended) return
    ended = true
    result.next(score)
    result.complete()
    bell.complete()
    state.complete()
  }
  return {
    publicBoard: state.pipe(map(b => b.turn + '/' + b.count)),
    hand: (seat: Seat) => state.pipe(map(b => b.hands[seat]), distinctUntilChanged()),
    sounds: bell.asObservable(), finalScore: result.asObservable(), draw, finish
  }
}

function playTable() {
  const table = makeTable()
  const boards: string[] = [], hands: (readonly string[])[] = [], sounds: string[] = []
  table.draw('青','竹')
  table.draw('红','月')
  table.publicBoard.subscribe(b => boards.push(b))
  table.hand('青').subscribe(h => hands.push(h))
  table.sounds.subscribe(e => sounds.push(e))
  assert.deepEqual([boards,hands,sounds], [['青/2'],[['竹']],[]])
  table.draw('红','星') // 未轮到红
  table.draw('青','松')
  table.draw('红','星')
  assert.deepEqual(boards, ['青/2','红/3','青/4'])
  assert.deepEqual(hands, [['竹'],['竹','松']])
  assert.deepEqual(sounds, ['青摸牌','红摸牌'])
  const early: number[] = [], late: number[] = []
  table.finalScore.subscribe(n => early.push(n))
  assert.deepEqual(early, [])
  table.finish(7)
  table.draw('青','云')
  table.finalScore.subscribe(n => late.push(n))
  assert.deepEqual([early,late], [[7],[7]])
  assert.equal(boards.length, 3)
  assert.equal(sounds.length, 2)
  const fresh: string[] = []
  makeTable().publicBoard.subscribe(b => fresh.push(b))
  assert.deepEqual(fresh, ['青/0'])
  console.log('牌桌恢复通过', { boards, hands, sounds, late, fresh })
}

const part = process.argv[2]
if (!part || part === 'A') rehearseEnsemble()
if (!part || part === 'B') playTable()
if (part && part !== 'A' && part !== 'B') throw Error('请选择 A 或 B')
