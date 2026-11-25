import assert from 'node:assert/strict'
import { Observable, defer, distinct, filter, from, map, pairwise, toArray } from 'rxjs'

function collect<T>(source: Observable<T>) {
  const values: T[] = []
  const handle = source.subscribe({ next: v => values.push(v), error: assert.fail })
  assert.equal(handle.closed, true)
  return values
}

// 题 A
type Foot = 'L' | 'R'
type Sample = { kind: 'foot'; foot: Foot; beat: number; down: boolean } | { kind: 'bar'; beat: number }
const foot = (beat: number, side: Foot, down = true): Sample => ({ kind: 'foot', beat, foot: side, down })
const bar = (beat: number): Sample => ({ kind: 'bar', beat })
function landings(samples: Sample[]) {
  return defer(() => {
    const held = new Set<Foot>()
    return from(samples).pipe(filter(sample => {
      if (sample.kind === 'bar') return true
      if (!sample.down) {
        held.delete(sample.foot)
        return false
      }
      if (held.has(sample.foot)) return false
      held.add(sample.foot)
      return true
    }))
  })
}
function score(steps: Observable<Sample>) {
  const arrows = steps.pipe(pairwise(), filter(([a, b]) =>
    a.kind === 'foot' && b.kind === 'foot' &&
    a.foot !== b.foot && b.beat - a.beat <= 2
  ), map(([a, b]) => [a.beat, b.beat]))
  return collect(arrows)
}
if (!process.argv[2] || process.argv[2] === 'A') {
  const steps = landings([foot(0, 'L'), foot(1, 'L'), foot(2, 'R'), foot(3, 'L', false),
    foot(4, 'L'), bar(5), foot(6, 'R'), foot(7, 'R', false), foot(8, 'R'),
    foot(9, 'L', false), foot(11, 'L'), foot(12, 'R', false), foot(13, 'R')])
  assert.deepEqual(collect(steps.pipe(filter(s => s.kind === 'foot'), map(s => s.foot + s.beat))),
    ['L0', 'R2', 'L4', 'R8', 'L11', 'R13'])
  const arrows = score(steps)
  assert.deepEqual(arrows, [[0, 2], [2, 4], [11, 13]])
  console.log('换脚箭头', arrows)
  const divided = landings([foot(0, 'L'), bar(1), foot(2, 'R')])
  assert.deepEqual(score(divided), [])
  assert.deepEqual(score(divided.pipe(filter(s => s.kind === 'foot'))), [[0, 2]])
  assert.deepEqual(score(landings([foot(0, 'L'), foot(1, 'L', false), foot(2, 'L'), foot(3, 'R')])),
    [[2, 3]])
}

// 题 B
type Lang = 'fr' | 'en' | 'tr'
type Card = { id: number; lang: Lang; word: string; sense: number; meaning: string }
function spelling(lang: Lang, word: string) {
  return word.normalize('NFC').toLocaleLowerCase(lang).normalize('NFC')
}
function bindCards(input: Card[]) {
  return collect(from(input).pipe(
    map(card => ({ card, key: spelling(card.lang, card.word) })),
    distinct(({ card, key }) => JSON.stringify([card.lang, key, card.sense])),
  ))
}
type Bound = ReturnType<typeof bindCards>
function lookup(cards: Bound, lang: Lang, query: string) {
  const key = spelling(lang, query)
  return cards.filter(row => row.card.lang === lang && row.key === key).map(row => row.card.id)
}
function frenchHints(cards: Bound, query: string) {
  const bare = (word: string) => word.normalize('NFD').replace(/\p{M}/gu, '')
  const key = spelling('fr', query)
  if (bare(key) !== key) return []
  return collect(from(cards).pipe(
    filter(row => row.card.lang === 'fr' && row.key !== key && bare(row.key) === key),
    distinct(row => row.key), map(row => row.card.word),
  ))
}
if (!process.argv[2] || process.argv[2] === 'B') {
  const input: Card[] = [
    { id: 1, lang: 'fr', word: 'Café', sense: 1, meaning: '饮品' },
    { id: 2, lang: 'fr', word: 'Cafe\u0301', sense: 1, meaning: '重复投稿' },
    { id: 3, lang: 'fr', word: 'CAFÉ', sense: 2, meaning: '店铺' },
    { id: 4, lang: 'fr', word: 'cafe', sense: 3, meaning: '代号' },
    { id: 5, lang: 'en', word: 'cafe', sense: 1, meaning: '小馆' },
    { id: 6, lang: 'tr', word: 'İL', sense: 1, meaning: '省' },
    { id: 7, lang: 'tr', word: 'IL', sense: 2, meaning: '字形样例' },
  ]
  const cards = bindCards(input)
  assert.deepEqual(cards.map(row => row.card.id), [1, 3, 4, 5, 6, 7])
  assert.deepEqual(lookup(cards, 'fr', 'café'), [1, 3])
  assert.deepEqual(lookup(cards, 'fr', 'cafe'), [4])
  assert.deepEqual(lookup(cards, 'en', 'cafe'), [5])
  assert.deepEqual(lookup(cards, 'tr', 'il'), [6])
  assert.deepEqual(lookup(cards, 'tr', 'ıl'), [7])
  assert.deepEqual(frenchHints(cards, 'cafe'), ['Café'])
  assert.deepEqual(frenchHints(cards, 'café'), [])
  console.log('法语 cafe', { exact: lookup(cards, 'fr', 'cafe'), hints: frenchHints(cards, 'cafe') })
  const bilingual = bindCards([input[3], { ...input[3], id: 8, lang: 'en' }])
  assert.deepEqual(bilingual.map(row => row.card.id), [4, 8])
  const swapped = bindCards([input[1], input[2], input[0]])
  assert.equal(swapped[0].card.word, 'Cafe\u0301')
  assert.deepEqual(lookup(swapped, 'fr', 'café'), [2, 3])
}

// 题 C
type Finish = { team: string; ms: number }
function results(input: Finish[], disqualified: Set<string>) {
  const [eligible] = collect(from(input).pipe(
    map((card, arrival) => ({ ...card, arrival })),
    filter(card => !disqualified.has(card.team)),
    toArray(),
  ))
  eligible.sort((a, b) => a.ms - b.ms || a.arrival - b.arrival)
  let previous: number | undefined, rank = 0
  return collect(from(eligible).pipe(map((card, i) => {
    const gap = previous === undefined ? null : card.ms - previous
    if (gap !== 0) rank = i + 1
    previous = card.ms
    const display = (Math.round(card.ms / 100) / 10).toFixed(1)
    return [card.team, rank, display, gap]
  })))
}
if (!process.argv[2] || process.argv[2] === 'C') {
  const cards = [{ team: '青', ms: 12049 }, { team: '黄', ms: 11980 }, { team: '白', ms: 12049 },
    { team: '紫', ms: 11970 }, { team: '橙', ms: 12051 }]
  const board = results(cards, new Set(['黄']))
  assert.deepEqual(board, [
    ['紫', 1, '12.0', null], ['青', 2, '12.0', 79], ['白', 2, '12.0', 0], ['橙', 4, '12.1', 2],
  ])
  assert.deepEqual(results(cards, new Set(['黄', '紫'])), [
    ['青', 1, '12.0', null], ['白', 1, '12.0', 0], ['橙', 3, '12.1', 2],
  ])
  assert.deepEqual(results(cards, new Set(cards.map(c => c.team))), [])
  assert.deepEqual(results(cards.slice(0, 1), new Set()), [['青', 1, '12.0', null]])
  console.log('终点成绩条', board)
}
