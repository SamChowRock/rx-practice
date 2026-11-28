import { concat, defer, distinctUntilChanged, map, merge, of, scan, startWith, switchMap, takeWhile } from 'rxjs';
import type { Observable } from 'rxjs';

export const gates = [.15, .32, .5, .69, .87] as const;
export const voyages = {
  calm: { duration: 22500, window: 590, perfect: 195 },
  swift: { duration: 15800, window: 365, perfect: 120 },
};
export type Mode = keyof typeof voyages;
export type Mark = 'perfect' | 'lit' | 'miss';
export type Phase = 'idle' | 'sailing' | 'paused' | 'arrived' | 'lost';
export type State = {
  mode: Mode; phase: Phase; elapsed: number; last: number; marks: Mark[];
  score: number; message: string; flashAt: number; feedbackAt: number;
};
type Action = { type: 'resume' | 'pause' | 'frame' | 'flash'; now: number };

export function initial(mode: Mode): State {
  return { mode, phase: 'idle', elapsed: 0, last: 0, marks: [], score: 0,
    message: '船头进入绿色光圈时点灯。', flashAt: -Infinity, feedbackAt: -Infinity };
}
export function offset(s: State) {
  const gate = gates[s.marks.length];
  return gate === undefined ? Infinity : s.elapsed - gate * voyages[s.mode].duration;
}
function mark(s: State, result: Mark, message: string): State {
  const marks = [...s.marks, result];
  const lost = marks.filter(m => m === 'miss').length >= 3;
  return { ...s, marks, phase: lost ? 'lost' : s.phase,
    score: s.score + (result === 'perfect' ? 150 : result === 'lit' ? 100 : 0), message, feedbackAt: s.elapsed };
}

export function advance(state: State, action: Action): State {
  if (action.type === 'resume') return { ...state, phase: 'sailing', last: action.now };
  if (state.phase !== 'sailing') return state;
  const rules = voyages[state.mode];
  const elapsed = Math.min(rules.duration, state.elapsed + Math.max(0, action.now - state.last));
  let s = { ...state, last: action.now, elapsed };
  while (s.phase === 'sailing' && s.marks.length < gates.length && offset(s) > rules.window) {
    s = mark(s, 'miss', '错过航标');
  }
  if (s.phase === 'lost') return s;
  if (elapsed >= rules.duration) return { ...s, phase: 'arrived', message: '邮船已靠岸' };
  if (action.type === 'pause') return { ...s, phase: 'paused' };
  if (action.type !== 'flash' || s.marks.length === gates.length || elapsed - s.flashAt < 350) return s;
  const distance = Math.abs(offset(s));
  s = { ...s, flashAt: elapsed, feedbackAt: elapsed };
  if (distance > 1250) return { ...s, message: '航标还远，再等一等' };
  if (distance > rules.window) return mark(s, 'miss', '信号过早');
  return distance <= rules.perfect ? mark(s, 'perfect', '正中标心 +150') : mark(s, 'lit', '成功引航 +100');
}

// 每次开航拥有一套时钟和监听。暂停只替换输入分支，累计航程留在外层 scan 中。
export function voyageStates(start: Observable<Mode>, toggle: Observable<unknown>, hide: Observable<unknown>,
  flash: Observable<unknown>, frames: Observable<unknown>, now: () => number): Observable<State> {
  return start.pipe(switchMap(mode => {
    const sailing = merge(toggle.pipe(map(() => 'toggle')), hide.pipe(map(() => 'pause'))).pipe(
      scan((active, event) => event === 'toggle' ? !active : false, true),
      startWith(true), distinctUntilChanged(),
    );
    return sailing.pipe(
      switchMap(active => active ? concat(
        defer(() => of<Action>({ type: 'resume', now: now() })),
        merge(frames.pipe(map((): Action => ({ type: 'frame', now: now() }))),
          flash.pipe(map((): Action => ({ type: 'flash', now: now() })))),
      ) : of<Action>({ type: 'pause', now: now() })),
      scan(advance, initial(mode)),
      takeWhile(s => s.phase !== 'arrived' && s.phase !== 'lost', true),
    );
  }));
}
