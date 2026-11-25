import assert from 'node:assert/strict';
import {
  Subject, Subscription, every, filter, map, of, reduce, scan, startWith, take, takeUntil,
} from 'rxjs';

// 题 A
{
  type Card = { word: string; points: number };
  type Board = ReadonlyMap<string, number>;
  const blank: Board = new Map();
  function replace(board: Board, card: Card): Board {
    return new Map(board).set(card.word, card.points);
  }
  function bestTwo(board: Board) {
    const ranked = [...board.values()].sort((a, b) => b - a);
    return (ranked[0] ?? 0) + (ranked[1] ?? 0);
  }
  function play(cards: [number, string, number][], bell: number | null) {
    const desk = new Subject<Card>();
    const ring = new Subject<void>();
    const round = desk.pipe(takeUntil(ring));
    const pencil = round.pipe(scan(replace, blank), map(bestTwo), startWith(0));
    let second = 0;
    const scores: [number, number][] = [], awards: number[] = [], lights: number[] = [];
    const signedAt: number[] = [];
    const owner = new Subscription();
    owner.add(pencil.subscribe(n => scores.push([second, n])));
    owner.add(pencil.pipe(filter(n => n >= 8), take(1)).subscribe(() => lights.push(second)));
    owner.add(round.pipe(reduce(replace, blank), map(bestTwo)).subscribe(n => {
      awards.push(n);
      signedAt.push(second);
    }));
    for (const [at, word, points] of cards) {
      second = at;
      desk.next({ word, points });
      assert.deepEqual(awards, []);
    }
    if (bell !== null) {
      second = bell;
      ring.next();
    }
    owner.unsubscribe();
    assert.deepEqual(signedAt, bell === null ? [] : [bell]);
    return { scores, lights, awards };
  }
  const result = play([[1, 'CAT', 4], [3, 'TEA', 3], [5, 'ACT', 5],
    [7, 'ACT', 1], [9, 'EAT', 4], [11, 'CAT', 0]], 12);
  assert.deepEqual(result.scores, [[0, 0], [1, 4], [3, 7], [5, 9], [7, 7], [9, 8], [11, 7]]);
  assert.deepEqual(result.lights, [5]);
  assert.deepEqual(result.awards, [7]);
  assert.equal(result.awards[0] >= 8, false);
  assert.deepEqual(play([], 12).awards, [0]);
  assert.deepEqual(play([[1, 'CAT', 4]], null).awards, []);
  assert.deepEqual(play([[1, 'CAT', 4], [2, 'CAT', 1]], 3).awards, [1]);
  of(4, 1).pipe(reduce((sum, points) => sum + points, 0))
    .subscribe(wrong => assert.equal(wrong, 5));
  console.log('拼字桌', result);
}

// 题 B
{
  type Zone = '左' | '中' | '右';
  type Tile = { zone: Zone; size: number };
  type Inspection = { seen: ReadonlySet<Zone>; firstFault: Tile | null };
  const empty: Inspection = { seen: new Set(), firstFault: null };
  const sound = (tile: Tile) => tile.size >= 48 && tile.size <= 52;
  function inspect(state: Inspection, tile: Tile): Inspection {
    return {
      seen: new Set(state.seen).add(tile.zone),
      firstFault: state.firstFault ?? (sound(tile) ? null : tile),
    };
  }
  function verdict(state: Inspection) {
    if (state.firstFault) return '不合格';
    return state.seen.size === 3 ? '合格' : '证据不足';
  }
  function fire(box: [number, Zone, number][], sealed: boolean) {
    const pieces = new Subject<Tile>();
    const faults: [number, Zone][] = [], labels: string[] = [];
    let minute = 0;
    const owner = new Subscription();
    owner.add(pieces.pipe(filter(tile => !sound(tile)), take(1))
      .subscribe(tile => faults.push([minute, tile.zone])));
    owner.add(pieces.pipe(reduce(inspect, empty), map(verdict)).subscribe(label => {
      assert.equal(minute, 6);
      labels.push(label);
    }));
    for (const [at, zone, size] of box) {
      minute = at;
      pieces.next({ zone, size });
      assert.deepEqual(labels, []);
    }
    if (sealed) {
      minute = 6;
      pieces.complete();
    }
    owner.unsubscribe();
    return { faults, labels };
  }
  assert.deepEqual(fire([[1, '左', 48], [2, '左', 50], [4, '中', 52]], true),
    { faults: [], labels: ['证据不足'] });
  assert.deepEqual(fire([[1, '左', 48], [2, '中', 50], [4, '右', 52]], true).labels, ['合格']);
  const cracked = fire([[1, '左', 48], [2, '右', 53], [4, '右', 50]], true);
  assert.deepEqual(cracked, { faults: [[2, '右']], labels: ['不合格'] });
  assert.deepEqual(fire([], true).labels, ['证据不足']);
  const leftFirst = fire([[1, '左', 47], [2, '中', 54], [4, '右', 50]], true);
  const middleFirst = fire([[1, '中', 54], [2, '左', 47], [4, '右', 50]], true);
  assert.deepEqual(leftFirst, { faults: [[1, '左']], labels: ['不合格'] });
  assert.deepEqual(middleFirst, { faults: [[1, '中']], labels: ['不合格'] });
  assert.deepEqual(fire([[1, '右', 53]], false), { faults: [[1, '右']], labels: [] });
  of(48, 50, 52).pipe(every(n => n >= 48 && n <= 52)).subscribe(ok => assert.equal(ok, true));
  console.log('窑次验收', cracked);
}
