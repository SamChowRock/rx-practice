import assert from 'node:assert/strict'
import { Observable, Subscription, UnsubscriptionError } from 'rxjs'

// 题 A
function audition(record: string[], immediate = false) {
  const drives = new Set<() => void>()
  let reads = 0, loans = 0, returns = 0
  const seat = new Observable<string>(sink => {
    loans++
    let at = 0
    const advance = () => {
      reads++
      const groove = record[at++]
      if (groove === '片尾') sink.complete()
      else if (groove === '损坏') sink.error(Error('坏片'))
      else sink.next(groove)
    }
    drives.add(advance)
    if (immediate) advance()
    return () => {
      drives.delete(advance)
      returns++
    }
  })
  function listen(receipt: string[]) {
    return seat.subscribe({
      next: sound => receipt.push(sound),
      complete: () => receipt.push('盖章'),
      error: () => receipt.push('红灯'),
    })
  }
  return { listen, tick() { for (const advance of drives) advance() },
    counts: () => [reads, loans, returns, drives.size] }
}
function podcast() {
  for (const second of ['旁白', '损坏']) {
    const player = audition(['雨声', second, '片尾'])
    const first: string[] = [], next: string[] = []
    const seat = player.listen(first)
    player.tick()
    seat.unsubscribe()
    player.tick()
    assert.deepEqual(player.counts(), [1, 1, 1, 0])
    player.listen(next)
    player.tick()
    player.tick()
    player.tick()
    assert.deepEqual(first, ['雨声'])
    assert.deepEqual(next, second === '损坏' ? ['雨声', '红灯'] : ['雨声', '旁白', '盖章'])
    assert.deepEqual(player.counts(), [second === '损坏' ? 3 : 4, 2, 2, 0])
    console.log('试听收据', { first, next, counts: player.counts() })
  }
  const short = audition(['片尾'], true)
  const receipt: string[] = []
  short.listen(receipt).unsubscribe()
  short.tick()
  assert.deepEqual(receipt, ['盖章'])
  assert.deepEqual(short.counts(), [1, 1, 1, 0])
}
if (!process.argv[2] || process.argv[2] === 'A') podcast()

// 题 B
function drawing(machineClosed: boolean, leaveOldOwner = false) {
  const board = new Subscription(), press = new Subscription()
  const sheets = new Set<string>()
  const prints: string[] = [], returned: string[] = []
  function down(color: string, start: number) {
    let holder = board
    sheets.add(color)
    const points = [start]
    const pen = new Subscription(() => {
      sheets.delete(color)
      returned.push(color)
    })
    board.add(pen)
    return {
      move(x: number) { if (!pen.closed) points.push(x) },
      transfer() {
        if (pen.closed || press.closed) return false
        if (!leaveOldOwner) holder.remove(pen)
        press.add(pen)
        holder = press
        return true
      },
      up() {
        if (pen.closed) return
        prints.push([holder === press ? '印章机' : '画板', color, ...points].join(' '))
        pen.unsubscribe()
      },
    }
  }
  const red = down('红', 2), blue = down('蓝', 8)
  red.move(3)
  if (machineClosed) press.unsubscribe()
  assert.equal(red.transfer(), !machineClosed)
  assert.deepEqual([...sheets], ['红', '蓝'])
  board.unsubscribe()
  const afterFold = [...sheets]
  blue.move(9)
  red.move(5)
  red.up()
  red.move(6)
  press.unsubscribe()
  assert.deepEqual([...sheets], [])
  assert.deepEqual(returned.sort(), ['红', '蓝'].sort())
  return { prints, afterFold }
}
if (!process.argv[2] || process.argv[2] === 'B') {
  const paper = drawing(false)
  assert.deepEqual(paper, { prints: ['印章机 红 2 3 5'], afterFold: ['红'] })
  console.log('合板后余片与纸本', paper.afterFold, paper.prints)
  assert.deepEqual(drawing(true), { prints: [], afterFold: [] })
  assert.deepEqual(drawing(false, true), { prints: [], afterFold: [] })
}

// 题 C
function darkroom(missing?: string, jammed?: string) {
  const held = new Set<string>(), attempts: string[] = [], receipt: string[] = []
  let expose = () => {}
  const setup = new Observable<string>(sink => {
    const borrowed: string[] = []
    sink.add(() => {
      const failures: Error[] = []
      for (const item of borrowed.reverse()) {
        attempts.push(item)
        if (item === jammed) failures.push(Error(item + '未退回'))
        else held.delete(item)
      }
      if (failures.length) throw new UnsubscriptionError(failures)
    })
    for (const item of ['罩', '灯', '夹']) {
      if (item === missing) throw Error('缺' + item)
      held.add(item)
      borrowed.push(item)
    }
    expose = () => { sink.next('相片'); sink.complete() }
  })
  const handle = setup.subscribe({
    next: photo => receipt.push(photo),
    error: (err: Error) => receipt.push(err.message),
  })
  return { held, attempts, receipt, handle, expose }
}
if (!process.argv[2] || process.argv[2] === 'C') {
  const shortage = darkroom('灯')
  assert.deepEqual(shortage.receipt, ['缺灯'])
  assert.deepEqual(shortage.attempts, ['罩'])
  assert.equal(shortage.held.size, 0)
  const ok = darkroom()
  ok.expose()
  assert.deepEqual(ok.receipt, ['相片'])
  assert.deepEqual(ok.attempts, ['夹', '灯', '罩'])
  assert.equal(ok.held.size, 0)
  const jam = darkroom(undefined, '灯')
  assert.throws(() => jam.handle.unsubscribe(), (err: unknown) =>
    err instanceof UnsubscriptionError && err.errors[0].message === '灯未退回')
  jam.handle.unsubscribe()
  assert.deepEqual(jam.attempts, ['夹', '灯', '罩'])
  assert.deepEqual([...jam.held], ['灯'])
  assert.deepEqual(jam.receipt, [])
  console.log('撤景盘点', { attempts: jam.attempts, outstanding: [...jam.held] })
}
