import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';

const filename = '发散练习导航.md';
const lines = [
  '# 发散题导航',
  '',
  '第一次只读题面，先画时间线，写下状态变化，再想一个能使你的方案失败的输入。答完再展开参考解法，运行本章实验。题 A 与题 B 可以分两天做。',
  '',
  '| 章 | 发散题（独立题面与参考推导） | 实验 |',
  '|---:|---|---|',
];
let questionCount = 0;
for (let number = 1; number <= 17; number++) {
  const id = String(number).padStart(2, '0');
  const chapterPath = 'book/' + id + '.md';
  const chapter = readFileSync(chapterPath, 'utf8');
  const heading = /<a id="transfer-([a-z])"><\/a>\s*#{2,4} 发散题 ([A-Z])[｜|：:]\s*(.+)/g;
  const sections = [...chapter.matchAll(heading)];
  const keys = sections.map(match => match[1]);
  assert.ok(keys.includes('a') && keys.includes('b'), '缺少题 A 或题 B：' + chapterPath);
  assert.equal(new Set(keys).size, keys.length, '题目锚点重复：' + chapterPath);

  const links = sections.map((match, index) => {
    const [, key, label, title] = match;
    assert.equal(key.toUpperCase(), label);
    const body = chapter.slice(match.index, sections[index + 1]?.index).split('## 隔章自测')[0];
    assert.match(body, /^`{3}(?:ts|typescript|js|javascript)\b/m, '题里应就地讲代码：' + chapterPath + ' ' + key);
    return label + ' · [' + title.trim() + '](' + chapterPath + '#transfer-' + key + ')';
  });

  const lab = 'labs/transfer/' + id + '.ts';
  assert.match(readFileSync(lab, 'utf8'), /\bassert[.(]/, '发散实验无断言：' + lab);
  lines.push('| ' + id + ' | ' + links.join('；') + ' | [实验源码](' + lab + ') |');
  questionCount += sections.length;
}
lines.push('', '单章运行 `npm run transfer -- 07`。省略编号会依次执行全部发散实验。原主线实验可用 `npm run lab -- 07` 运行。', '');

const result = lines.join('\n');
if (process.argv.includes('--check')) {
  assert.equal(readFileSync(filename, 'utf8'), result, '运行 npm run index 更新题目导航');
} else {
  writeFileSync(filename, result);
}
console.log('已核对 17 章、' + questionCount + ' 个发散题入口及章内代码、17 份独立实验。');
