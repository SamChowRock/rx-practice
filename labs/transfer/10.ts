import assert from 'node:assert/strict';
import { Subject, defer, filter, finalize, firstValueFrom, from, scan, shareReplay } from 'rxjs';

// 题 A
async function magazine() {
  type Part = { edition: string; total: number; bytes: number[] };
  const saved: Part = { edition: '"sea-1"', total: 8, bytes: [10, 11, 12] };
  function receive(base: Part, reply: Response) {
    return defer(async (): Promise<Part> => {
      const edition = reply.headers.get('etag');
      assert.ok(edition);
      const body = Array.from(new Uint8Array(await reply.arrayBuffer()));
      if (reply.status === 200) {
        const declared = reply.headers.get('content-length');
        assert.ok(declared !== null && /^\d+$/.test(declared));
        assert.equal(body.length, Number(declared));
        return { edition, total: body.length, bytes: body };
      }
      assert.equal(reply.status, 206);
      assert.equal(edition, base.edition);
      const start = base.bytes.length;
      const end = start + body.length - 1;
      assert.ok(body.length > 0 && end < base.total);
      assert.equal(reply.headers.get('content-range'), `bytes ${start}-${end}/${base.total}`);
      return { edition, total: base.total, bytes: [...base.bytes, ...body] };
    });
  }
  const suffix = (start: number) => new Response(new Uint8Array([13, 14, 15, 16, 17]), {
    status: 206, headers: { etag: '"sea-1"', 'content-range': `bytes ${start}-7/8` }
  });
  const complete = await firstValueFrom(receive(saved, suffix(3)));
  assert.deepEqual(complete.bytes, [10, 11, 12, 13, 14, 15, 16, 17]);
  const partial = new Response(new Uint8Array([13, 14, 15]), {
    status: 206, headers: { etag: '"sea-1"', 'content-range': 'bytes 3-5/8' }
  });
  const next = await firstValueFrom(receive(saved, partial));
  assert.equal(next.bytes.length, 6);
  assert.ok(next.bytes.length < next.total);
  await assert.rejects(firstValueFrom(receive(saved, suffix(4))));
  const fresh = new Uint8Array([20, 21, 22, 23, 24, 25]);
  const replacement = await firstValueFrom(receive(saved, new Response(fresh, {
    headers: { etag: '"sea-2"', 'content-length': '6' }
  })));
  assert.deepEqual(replacement, { edition: '"sea-2"', total: 6, bytes: [...fresh] });
  assert.deepEqual(saved.bytes, [10, 11, 12]);
}

// 题 B
function chess() {
  type Turn = { room: string; ply: number; move: string };
  const wire = new Subject<Turn>(), commands: string[] = [];
  const room = defer(() => {
    commands.push('join Q');
    return wire.pipe(filter(turn => turn.room === 'Q'), finalize(() => commands.push('leave Q')));
  }).pipe(
    scan((board, turn) => {
      if (board.frozen || turn.ply <= board.moves.length) return board;
      if (turn.ply !== board.moves.length + 1) return { ...board, frozen: true };
      return { moves: [...board.moves, turn.move], frozen: false };
    }, { moves: [] as string[], frozen: false }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
  const viewer = room.subscribe();
  wire.next({ room: 'Q', ply: 1, move: 'b1-c3' });
  const boards: string[][] = [], freezes: boolean[] = [];
  const reviewer = room.subscribe(board => { boards.push(board.moves); freezes.push(board.frozen); });
  viewer.unsubscribe();
  assert.deepEqual(commands, ['join Q']);
  from([
    { room: 'R', ply: 2, move: 'd7-d5' },
    { room: 'Q', ply: 2, move: 'e7-e5' },
    { room: 'Q', ply: 2, move: 'e7-e5' },
    { room: 'Q', ply: 4, move: 'g8-f6' }
  ]).subscribe(turn => wire.next(turn));
  assert.deepEqual(boards, [['b1-c3'], ...Array.from({ length: 3 }, () => ['b1-c3', 'e7-e5'])]);
  assert.deepEqual(freezes, [false, false, false, true]);
  reviewer.unsubscribe();
  assert.deepEqual(commands, ['join Q', 'leave Q']);

  const recent = new Subject<Turn>();
  const lastMove = recent.pipe(shareReplay({ bufferSize: 1, refCount: true }));
  const seat = lastMove.subscribe();
  recent.next({ room: 'Q', ply: 1, move: 'b1-c3' });
  recent.next({ room: 'Q', ply: 2, move: 'e7-e5' });
  const evidence: string[] = [];
  const newcomer = lastMove.subscribe(turn => evidence.push(turn.move));
  assert.deepEqual(evidence, ['e7-e5']);
  newcomer.unsubscribe();
  seat.unsubscribe();
}

const choice = process.argv[2] ?? 'both';
assert.ok(['A', 'B', 'both'].includes(choice));
if (choice !== 'B') { await magazine(); console.log('A 续页、拒收错段、换版重取通过'); }
if (choice !== 'A') { chess(); console.log('B 接手局面、房间租期、缺手冻结通过'); }
