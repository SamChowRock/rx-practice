import assert from 'node:assert/strict'
import { Observable, Subject, Subscriber, concat, defer, from, take } from 'rxjs'

// 题 A
function shuffle(tape: number[], original = ['A', 'B', 'C', 'D']) {
  const deck = original.slice()
  if (tape.length !== deck.length - 1) throw Error('坏纸带')
  for (let i = deck.length - 1; i > 0; i--) {
    const j = tape[deck.length - 1 - i]
    if (!Number.isInteger(j) || j < 0 || j > i) throw Error('坏纸带')
    const fixed = deck[j]
    deck[j] = deck[i]
    deck[i] = fixed
  }
  return deck
}
function read<T>(source: Observable<T>) {
  const values: T[] = []
  source.subscribe({ next: value => values.push(value), error: assert.fail })
  return values
}
function cards() {
  const tapes = [[1, 0, 1], [0, 0, 0]]
  let used = 0
  const openRound = () => from(shuffle(tapes[used++]))
  const morning = openRound()
  const east = read(morning.pipe(take(1)))
  const west = read(morning)
  assert.deepEqual(east, ['C'])
  assert.equal(west.join(''), 'CDAB')
  assert.equal(used, 1)
  assert.equal(read(openRound()).join(''), 'BCDA')
  assert.equal(used, 2)
  const hands: string[][] = [[], []]
  let turn = 0
  morning.subscribe(card => hands[turn++ % 2].push(card))
  assert.deepEqual(hands, [['C', 'A'], ['D', 'B']])
  used = 0
  const wrong = defer(openRound)
  read(wrong.pipe(take(1)))
  assert.equal(read(wrong).join(''), 'BCDA')
  const issued: string[] = []
  defer(() => from(shuffle([1, 3, 1]))).subscribe({
    next: card => issued.push(card), error: err => issued.push(err.message),
  })
  assert.deepEqual(issued, ['坏纸带'])
  const orders = new Set<string>()
  for (let a = 0; a < 4; a++)
    for (let b = 0; b < 3; b++)
      for (let c = 0; c < 2; c++) orders.add(shuffle([a, b, c]).join(''))
  assert.equal(orders.size, 24)
  console.log('分桌与同桌', { east, west, hands, possibleOrders: orders.size })
}
if (!process.argv[2] || process.argv[2] === 'A') cards()

// 题 B
type Voice = { word(s: string): void; end(): void; fail(error: Error): void }
class VoiceBox {
  beat = 0
  openings: [number, string][] = []
  active = new Map<number, Voice>()
  start(line: string, events: Voice) {
    if (this.active.size) throw Error('发音器忙')
    const ticket = this.openings.push([this.beat, line])
    this.active.set(ticket, events)
    if (!line) this.finish(ticket)
    return ticket
  }
  finish(ticket: number) {
    const voice = this.active.get(ticket)
    this.active.delete(ticket)
    voice?.end()
  }
  stop(ticket: number) { this.active.delete(ticket) }
}
function speak(box: VoiceBox, line: string) {
  return new Observable<string>(sink => {
    const ticket = box.start(line, {
      word: word => sink.next(word),
      end: () => sink.complete(),
      fail: reason => sink.error(reason),
    })
    return () => box.stop(ticket)
  })
}
function rehearse(lines: string[]) {
  const box = new VoiceBox()
  const heard: string[] = [], ends: string[] = []
  const show = concat(...lines.map(line => speak(box, line)))
  assert.equal(box.openings.length, 0)
  const session = show.subscribe({
    next: word => heard.push(word),
    error: reason => ends.push(String(reason.message)),
    complete: () => ends.push('谢幕'),
  })
  return { box, session, heard, ends }
}
function voices() {
  const { box, heard, ends, session } = rehearse(['小 船', '靠 岸'])
  box.beat = 1
  box.active.get(1)!.word('小')
  box.beat = 2
  box.active.get(1)!.word('船')
  assert.deepEqual(box.openings, [[0, '小 船']])
  box.beat = 4
  box.finish(1)
  const second = box.active.get(2)!
  box.beat = 5
  second.word('靠')
  box.beat = 6
  session.unsubscribe()
  box.beat = 7
  second.word('岸')
  box.beat = 8
  second.end()
  assert.equal(box.active.size, 0)
  assert.deepEqual(box.openings, [[0, '小 船'], [4, '靠 岸']])
  assert.deepEqual(heard, ['小', '船', '靠'])
  assert.deepEqual(ends, [])
  const blank = rehearse(['', '到'])
  assert.deepEqual([...blank.box.active.keys()], [2])
  assert.deepEqual(blank.heard, [])
  blank.box.active.get(2)!.word('到')
  blank.box.finish(2)
  assert.deepEqual(blank.heard, ['到'])
  assert.deepEqual(blank.ends, ['谢幕'])
  const bad = rehearse(['小 船', '靠 岸'])
  const first = bad.box.active.get(1)!
  first.word('小')
  first.word('船')
  first.fail(Error('故障'))
  assert.deepEqual(bad.heard, ['小', '船'])
  assert.deepEqual(bad.ends, ['故障'])
  assert.deepEqual(bad.box.openings, [[0, '小 船']])
  assert.equal(bad.box.active.size, 0)
  console.log('朗读排练', { openings: box.openings, heard, active: box.active.size })
}
if (!process.argv[2] || process.argv[2] === 'B') voices()

// 题 C
function typeset(input: Observable<Uint8Array>) {
  return new Observable<string>(sink => {
    const decoder = new TextDecoder('utf-8', { fatal: true })
    let tail = ''
    function accept(text: string) {
      tail += text
      let cut = tail.indexOf('\n')
      while (cut >= 0 && !sink.closed) {
        const line = tail.slice(0, cut)
        tail = tail.slice(cut + 1)
        sink.next(line.endsWith('\r') ? line.slice(0, -1) : line)
        cut = tail.indexOf('\n')
      }
    }
    const reader = new Subscriber<Uint8Array>({
      next(chunk) {
        try { accept(decoder.decode(chunk, { stream: true })) }
        catch (error) { sink.error(error) }
      },
      complete() {
        try {
          accept(decoder.decode())
          if (tail) sink.next(tail)
          sink.complete()
        } catch (error) { sink.error(error) }
      },
      error: (error: unknown) => sink.error(error),
    })
    sink.add(reader)
    input.subscribe(reader)
  })
}
function poetry() {
  const chunks = [[0xe6, 0x9c], [0x88, 13], [10, 10, 115], [101, 97]]
    .map(bytes => Uint8Array.from(bytes))
  const roll = new Subject<Uint8Array>()
  const plates: [number, string][] = []
  let beat = 0
  typeset(roll).subscribe(line => plates.push([beat, line]))
  for (const chunk of chunks) { beat++; roll.next(chunk) }
  beat++
  roll.complete()
  assert.deepEqual(plates, [[3, '月'], [3, ''], [5, 'sea']])
  console.log('交盘拍号与盘内文字', plates)
  const stopped = new Subject<Uint8Array>()
  const stop = typeset(stopped).subscribe({ next: assert.fail, complete: assert.fail, error: assert.fail })
  stopped.next(chunks[0])
  stopped.next(chunks[1])
  stop.unsubscribe()
  stopped.next(chunks[2])
  stopped.complete()
  const bad: string[] = []
  typeset(from([new TextEncoder().encode('月\n'), chunks[0]])).subscribe({
    next: line => bad.push(line), error: () => bad.push('坏卷'), complete: assert.fail,
  })
  assert.deepEqual(bad, ['月', '坏卷'])
  const bytes = new TextEncoder().encode('月\r\n\nsea')
  for (let mask = 0; mask < 2 ** (bytes.length - 1); mask++) {
    const pieces: Uint8Array[] = []
    let start = 0
    for (let end = 1; end < bytes.length; end++) {
      if (mask & (1 << (end - 1))) {
        pieces.push(bytes.slice(start, end), new Uint8Array())
        start = end
      }
    }
    pieces.push(bytes.slice(start))
    assert.deepEqual(read(typeset(from(pieces))), ['月', '', 'sea'])
  }
}
if (!process.argv[2] || process.argv[2] === 'C') poetry()
