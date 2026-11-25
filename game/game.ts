import {
  EMPTY, concat, map, merge, of, scan, shareReplay, startWith,
  switchMap, takeWhile, timer, type Observable,
} from 'rxjs';

export const LANES = 5;
export const FLOOR = 20;
export const PERIOD = 10;
export type Stage = 1 | 2 | 3 | 4 | 5;
export type State = {
  tick: number;
  lane: number;
  stars: {lane: number; row: number}[];
  score: number;
  misses: number;
  paused: boolean;
};

export function initial(stage: Stage): State {
  return {
    tick: 0, lane: 2, score: 0, misses: 0, paused: false,
    stars: stage === 2 ? [{lane: 1, row: 0}] : [],
  };
}

export function finished(state: State, stage: Stage) {
  return stage === 2 ? state.score + state.misses > 0 : state.misses >= 3;
}

export function move(state: State, direction: number): State {
  const lane = Math.max(0, Math.min(LANES - 1, state.lane + direction));
  return { ...state, lane };
}

// 落星拍读取当前轨道，移动已有的星、结算，最后放入新星。
export function step(state: State, stage: Stage, spawnLane?: number): State {
  const next = { ...state, tick: state.tick + 1, stars: [] as State['stars'] };
  for (const star of state.stars) {
    const row = star.row + 1;
    if (row < FLOOR) next.stars.push({ ...star, row });
    else if (star.lane === state.lane) next.score++;
    else next.misses++;
  }
  if (spawnLane !== undefined && !finished(next, stage)) next.stars.push({lane: spawnLane, row: 0});
  return next;
}

type Action = {kind: 'pause'; paused: boolean} | {kind: 'tick'} | {kind: 'move'; direction: number};

// restart$ 替换整局。toggle$ 只替换时钟，scan 在它外面保存本局进度。
export function gameStates(
  stage: Stage, restart$: Observable<unknown>, toggle$: Observable<unknown>,
  direction$: Observable<number>, clock$: Observable<unknown>, pickLane: (ordinal: number) => number,
): Observable<State> {
  return restart$.pipe(
    startWith(null),
    switchMap(() => {
      const seed = initial(stage);
      const paused$ = stage >= 4
        ? toggle$.pipe(scan(paused => !paused, false), startWith(false))
        : of(false);
      // 首步同步发出。方向改变时，switchMap 撤掉上一方向的连移。
      const moves$ = direction$.pipe(switchMap(direction => {
        if (direction === 0) return EMPTY;
        const action: Action = {kind: 'move', direction};
        return timer(160, 95).pipe(map(() => action), startWith(action));
      }));
      const actions$ = paused$.pipe(switchMap(paused => {
        const change = of<Action>({kind: 'pause', paused});
        if (paused) return change;
        return concat(change, merge(moves$, clock$.pipe(map((): Action => ({kind: 'tick'})))));
      }));
      return actions$.pipe(
        scan((state, action) => {
          if (action.kind === 'move') return move(state, action.direction);
          if (action.kind === 'pause') return { ...state, paused: action.paused };
          const tick = state.tick + 1;
          const spawnLane = stage >= 3 && tick % PERIOD === 0 ? pickLane(tick / PERIOD - 1) : undefined;
          return step(state, stage, spawnLane);
        }, seed),
        takeWhile(state => !finished(state, stage), true),
      );
    }),
    shareReplay({bufferSize: 1, refCount: true}),
  );
}
