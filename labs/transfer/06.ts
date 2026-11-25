import assert from 'node:assert/strict';
import {
  Subject, VirtualTimeScheduler, concatMap, defer, exhaustMap, filter, finalize, from,
  map, mergeMap, switchMap, take, takeUntil, timer,
} from 'rxjs';

// 题 A
{
  type Cell = { id: string; cost: number };
  const west: Cell[] = [{ id: '脊', cost: 12 }, { id: '谷', cost: 4 }, { id: '桥', cost: 6 }];
  const east: Cell[] = [{ id: '湾', cost: 3 }, { id: '岸', cost: 5 }];
  function draw(latestOnly: boolean, turnAt = 7) {
    const clock = new VirtualTimeScheduler();
    const sheets = new Subject<Cell[]>();
    const starts: [number, string][] = [], inks: [number, string][] = [];
    const frees: [number, string][] = [];
    let active = 0, peak = 0;
    const raster = (cell: Cell) => defer(() => {
      starts.push([clock.now(), cell.id]);
      peak = Math.max(peak, ++active);
      return timer(cell.cost, clock).pipe(map(() => cell.id), finalize(() => {
        active--;
        frees.push([clock.now(), cell.id]);
      }));
    });
    const render = (cells: Cell[]) => from(cells).pipe(mergeMap(raster, 2));
    sheets.pipe(latestOnly ? switchMap(render) : mergeMap(render))
      .subscribe(id => inks.push([clock.now(), id]));
    clock.schedule(() => sheets.next(west), 0);
    clock.schedule(() => sheets.next(east), turnAt);
    clock.schedule(() => sheets.complete(), 8);
    clock.flush();
    assert.equal(active, 0);
    return { starts, inks, frees, peak };
  }
  const result = draw(true);
  assert.deepEqual(result.starts, [[0, '脊'], [0, '谷'], [4, '桥'], [7, '湾'], [7, '岸']]);
  assert.deepEqual(result.inks, [[4, '谷'], [10, '湾'], [12, '岸']]);
  assert.equal(result.peak, 2);
  assert.deepEqual(result.frees.filter(([at]) => at === 7).map(([, id]) => id).sort(), ['桥', '脊'].sort());
  assert.deepEqual(draw(false).inks, [[4, '谷'], [10, '桥'], [10, '湾'], [12, '脊'], [12, '岸']]);
  assert.equal(draw(false).peak, 4);
  const earlyTurn = draw(true, 2);
  assert.deepEqual(earlyTurn.starts, [[0, '脊'], [0, '谷'], [2, '湾'], [2, '岸']]);
  assert.deepEqual(earlyTurn.inks, [[5, '湾'], [7, '岸']]);
  console.log('地形绘制', result);
}

// 题 B
{
  type Press = { take: string; at: number };
  const touches: Press[] = [{ at: 0, take: '甲' }, { at: 3, take: '乙' }, { at: 12, take: '丙' }];
  function record(queuePresses: boolean) {
    const clock = new VirtualTimeScheduler();
    const down = new Subject<Press>(), up = new Subject<string>(), mic = new Subject<string>();
    const opened: [number, string][] = [], cuts: string[] = [], notes: string[] = [];
    const capture = (press: Press) => defer(() => {
      opened.push([clock.now(), press.take]);
      return mic.pipe(
        takeUntil(up.pipe(filter(key => key === press.take), take(1))),
        map(sound => press.take + '/' + sound),
        finalize(() => cuts.push(clock.now() + '/' + press.take)),
      );
    });
    const session = down.pipe(queuePresses ? concatMap(capture) : exhaustMap(capture))
      .subscribe(sound => notes.push(sound));
    for (const press of touches) clock.schedule(() => down.next(press), press.at);
    for (const [at, key] of [[5, '乙'], [9, '甲'], [18, '丙']] as const) {
      clock.schedule(() => up.next(key), at);
    }
    for (const [at, sound] of [[1, '咚'], [6, '哒'], [10, '嗒'], [14, '啪']] as const) {
      clock.schedule(() => mic.next(sound), at);
    }
    clock.schedule(() => down.complete(), 19);
    clock.schedule(() => session.unsubscribe(), 21);
    clock.flush();
    return { opened, cuts, notes };
  }
  const live = record(false);
  assert.deepEqual(live.opened, [[0, '甲'], [12, '丙']]);
  assert.deepEqual(live.notes, ['甲/咚', '甲/哒', '丙/啪']);
  assert.deepEqual(live.cuts, ['9/甲', '18/丙']);
  const queued = record(true);
  assert.deepEqual(queued.opened, [[0, '甲'], [9, '乙']]);
  assert.deepEqual(queued.notes, ['甲/咚', '甲/哒', '乙/嗒', '乙/啪']);
  assert.deepEqual(queued.cuts, ['9/甲', '21/乙']);
  console.log('踏板录音', live, '积压按下事件', queued);
}
