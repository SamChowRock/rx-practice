import assert from 'node:assert/strict'
import {
  BehaviorSubject, Subject, combineLatest, forkJoin, map, merge,
  take, withLatestFrom, zip
} from 'rxjs'

// 题 A
function rehearseSubtitles() {
  const cues = [
    { start: 0, end: 2, zh: '门开', en: 'OPEN' },
    { start: 2, end: 5, zh: '进来', en: 'ENTER' },
    { start: 7, end: 9, zh: '请坐', en: 'SIT' }
  ]
  type Language = 'zh' | 'en'
  function textAt(position: number, lang: Language) {
    const cue = cues.find(c => c.start <= position && position < c.end)
    return cue?.[lang] ?? '熄'
  }
  const position$ = new Subject<number>()
  const language$ = new BehaviorSubject<Language>('zh')
  const projected: [number, string][] = [], proof: [number, string][] = []
  let tick = 0
  position$.pipe(withLatestFrom(language$), map(([p, l]) => textAt(p, l)))
    .subscribe(word => projected.push([tick, word]))
  combineLatest([position$, language$]).pipe(map(([p, l]) => textAt(p, l)))
    .subscribe(word => proof.push([tick, word]))
  function report(p: number) { tick++; position$.next(p) }
  function change(l: Language) { tick++; language$.next(l) }
  report(0); report(2); change('en'); report(5)
  report(8); report(1); change('zh'); report(2)
  assert.deepEqual(projected, [[1,'门开'],[2,'进来'],[4,'熄'],[5,'SIT'],[6,'OPEN'],[8,'进来']])
  assert.deepEqual(proof, [[1,'门开'],[2,'进来'],[3,'ENTER'],[4,'熄'],
    [5,'SIT'],[6,'OPEN'],[7,'门开'],[8,'进来']])

  const chinese$ = new Subject<number>(), english$ = new Subject<number>()
  const paired: number[][] = [], ready: number[] = []
  zip(chinese$, english$).subscribe(pair => paired.push(pair))
  const drafts = new Map<number, Set<Language>>()
  merge(chinese$.pipe(map(id => ({ id, lang: 'zh' as const }))),
    english$.pipe(map(id => ({ id, lang: 'en' as const })))).subscribe(({ id, lang }) => {
    const received = drafts.get(id) ?? new Set<Language>()
    received.add(lang)
    drafts.set(id, received)
    if (received.size === 2) { ready.push(id); drafts.delete(id) }
  })
  for (const id of [1,2,3]) chinese$.next(id)
  for (const id of [2,1,3]) english$.next(id)
  assert.deepEqual(paired, [[1,2],[2,1],[3,3]])
  assert.deepEqual(ready, [2,1,3])
  console.log('字幕排练通过', projected, '齐稿段号', ready)
}

// 题 B
function weighDough() {
  const flour$ = new Subject<number>(), water$ = new Subject<number>()
  const flourPedal$ = new Subject<void>(), waterPedal$ = new Subject<void>()
  const dial: (number | '待投粉')[] = []
  const cards: { flour: number; water: number }[] = []
  combineLatest({ flour: flour$, water: water$ }).subscribe(({ flour, water }) => {
    dial.push(flour === 0 ? '待投粉' : 100 * water / flour)
  })
  function seal(pedal$: Subject<void>, scale$: Subject<number>) {
    return pedal$.pipe(withLatestFrom(scale$), map(([, grams]) => grams), take(1))
  }
  forkJoin({ flour: seal(flourPedal$, flour$), water: seal(waterPedal$, water$) })
    .subscribe(card => cards.push(card))
  waterPedal$.next() // 预演一次无读数的误踩，随后仍可正式封盆
  flour$.next(0); water$.next(120); flour$.next(200); water$.next(130)
  waterPedal$.next()
  assert.equal(cards.length, 0)
  flour$.next(250); flourPedal$.next()
  assert.deepEqual(cards, [{ flour: 250, water: 130 }])
  water$.next(0)
  assert.deepEqual(dial, ['待投粉',60,65,52,0])
  assert.equal(100 * cards[0].water / cards[0].flour, 52)

  const portions: number[] = []
  let previous = 0
  function addToBowl(reading: number) {
    portions.push(reading - previous)
    previous = reading
  }
  addToBowl(80); addToBowl(125)
  previous = 0 // 已确认换盆、完成去皮
  addToBowl(30)
  assert.deepEqual(portions, [80,45,30])
  console.log('称量通过', dial, cards, portions)
}

const part = process.argv[2]
if (!part || part === 'A') rehearseSubtitles()
if (!part || part === 'B') weighDough()
if (part && part !== 'A' && part !== 'B') throw Error('请选择 A 或 B')
