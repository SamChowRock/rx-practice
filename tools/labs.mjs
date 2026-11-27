import { spawnSync } from 'node:child_process';
const transfer = process.argv[2] === '--transfer';
const chosen = process.argv[transfer ? 3 : 2];
const chapters = Array.from({length:transfer ? 17 : 24}, (_, index) => String(index+1).padStart(2,'0'));
if (chosen && !chapters.includes(chosen.padStart(2,'0'))) throw Error('请输入 01 至 ' + chapters.at(-1) + ' 的章节编号');
for (const id of chosen ? [chosen.padStart(2,'0')] : chapters) {
  console.log('\n━━ 第 ' + id + ' 章' + (transfer ? ' · 发散题' : '') + ' ━━');
  const result = spawnSync(process.execPath,[(transfer ? 'labs/transfer/' : 'labs/') + id + '.ts'], {stdio:'inherit',timeout:45_000});
  if (result.error) console.error(result.error.message);
  if (result.status !== 0) process.exit(result.status ?? 1);
}
