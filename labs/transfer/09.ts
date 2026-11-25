import assert from 'node:assert/strict'
import {
  Subject, bufferCount, concatMap, filter, groupBy, map, mergeMap, of, pairwise,
  reduce, switchMap, takeUntil, toArray
} from 'rxjs'

// 题 A
function printContactSheets() {
  type Roll = '甲' | '乙'
  const negatives$ = new Subject<{ roll: Roll; frame: number }>()
  const developed$ = new Subject<Roll>()
  const pages: { roll: Roll; cells: (number | null)[] }[] = []
  const wrong: number[][] = []
  negatives$.pipe(map(n => n.frame), bufferCount(4)).subscribe(p => wrong.push(p))
  negatives$.pipe(
    groupBy(n => n.roll, { duration: g => developed$.pipe(filter(roll => roll === g.key)) }),
    mergeMap(g => g.pipe(map(n => n.frame), bufferCount(4),
      map(frames => {
        const cells: (number | null)[] = [null, null, null, null]
        frames.forEach((frame, i) => { cells[i] = frame })
        return { roll: g.key, cells }
      })))
  ).subscribe(p => pages.push(p))
  function expose(roll: Roll, frame: number) { negatives$.next({ roll, frame }) }
  expose('甲',11); expose('乙',21); expose('甲',12); expose('甲',13)
  assert.equal(pages.length, 0)
  assert.deepEqual(wrong, [[11,21,12,13]])
  expose('乙',22); expose('甲',14); expose('甲',15)
  assert.deepEqual(pages, [{ roll:'甲', cells:[11,12,13,14] }])
  developed$.next('乙')
  assert.equal(pages.length, 2)
  assert.deepEqual(pages[1], { roll:'乙', cells:[21,22,null,null] })
  developed$.next('甲')
  assert.deepEqual(pages[2], { roll:'甲', cells:[15,null,null,null] })
  developed$.next('乙')
  negatives$.complete()
  assert.equal(pages.length, 3)
  function backOfSheet<T>([a,b,c,d]: readonly T[]) { return [b,a,d,c] }
  assert.deepEqual(backOfSheet(pages[2].cells), [null,15,null,null])
  assert.deepEqual(backOfSheet(backOfSheet(pages[0].cells)), pages[0].cells)
  const used = pages.flatMap(p => p.cells.filter(cell => cell !== null))
  assert.deepEqual([...used].sort((a,b) => a-b), [11,12,13,14,15,21,22])
  console.log('接触印样通过', pages)
}

// 题 B
type Reading = { minute: number; rate: number | null }
type Bed = { name: string; open: number; close: number }
type Segment = { start: number; end: number; rate: number | null }
type Water = { bed: string; mm: number; unknown: number }

function measureRain() {
  const beds: Bed[] = [
    { name:'苗床', open:1, close:7 },
    { name:'花圃', open:4, close:10 }
  ]
  const readings: Reading[] = [
    { minute:0, rate:0 }, { minute:2, rate:6 }, { minute:5, rate:12 },
    { minute:8, rate:0 }, { minute:10, rate:null }
  ]
  function totals(points: Reading[], plots: Bed[] = beds) {
    const segments: Segment[] = []
    const result: Water[] = []
    of(...points).pipe(pairwise(), map(([left, right]) => ({
      start:left.minute, end:right.minute, rate:left.rate
    }))).subscribe(s => segments.push(s))
    of(...plots).pipe(concatMap(bed => of(...segments).pipe(
      reduce((total, s) => {
        const minutes = Math.max(0, Math.min(s.end, bed.close) - Math.max(s.start, bed.open))
        if (s.rate === null) total.unknown += minutes
        else total.mm += s.rate * minutes / 60
        return total
      }, { bed:bed.name, mm:0, unknown:0 })
    ))).subscribe(v => result.push(v))
    return result
  }
  const base = totals(readings)
  // 不在逐段累加时舍入，仅在最终比较时消除二进制浮点尾差。
  assert.deepEqual(base.map(v => [+v.mm.toFixed(3),v.unknown]), [[0.7,0],[0.7,0]])
  const earlierCover = totals(readings, [beds[0], { ...beds[1], close:7 }])
  assert.deepEqual(earlierCover.map(v => +v.mm.toFixed(3)), [0.7,0.5])
  const missing = totals(readings.map(p => p.minute === 5 ? { ...p, rate:null } : p))
  assert.deepEqual(missing.map(v => [+v.mm.toFixed(3),v.unknown]), [[0.3,2],[0.1,3]])
  const split = [...readings.slice(0, 3), { minute:6, rate:12 }, ...readings.slice(3)]
  assert.deepEqual(totals(split).map(v => +v.mm.toFixed(3)), [0.7,0.7])
  const stations = new Subject<number>(), openBed = new Subject<void>(), cover = new Subject<void>()
  const insideOnly: number[][] = []
  openBed.pipe(switchMap(() => stations.pipe(takeUntil(cover), toArray())))
    .subscribe(rates => insideOnly.push(rates))
  stations.next(6); openBed.next(); stations.next(12); stations.next(0); cover.next()
  assert.deepEqual(insideOnly, [[12,0]]) // 第四分钟揭盖时生效的 6 不在收听区间内
  console.log('雨量通过', { base, missing, insideOnly })
}

const part = process.argv[2]
if (!part || part === 'A') printContactSheets()
if (!part || part === 'B') measureRain()
if (part && part !== 'A' && part !== 'B') throw Error('请选择 A 或 B')
