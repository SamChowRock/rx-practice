import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';

const filename = '发散练习导航.md';
const lines = [
  '# 配套练习册',
  '',
  '跟着章节读控制台时遇到的订阅、取消和重试，在这些题里换了场景。每道题都附参考解法和实验。',
  '',
  '| 章节与主题 | 题目 | 实验 |',
  '|---|---|---|',
];
let questionCount = 0;
for (let number = 1; number <= 17; number++) {
  const id = String(number).padStart(2, '0');
  const chapterPath = 'book/' + id + '.md';
  const chapter = readFileSync(chapterPath, 'utf8');
  const workbookPath = '练习/' + id + '.md';
  const workbook = readFileSync(workbookPath, 'utf8');
  const chapterTitle = workbook.match(/^# (.+)$/m)[1];
  const heading = /<a id="transfer-([a-z])"><\/a>\s*#{2,4} 发散题 ([A-Z])[｜|：:]\s*(.+)/g;
  const sections = [...workbook.matchAll(heading)];
  const keys = sections.map(match => match[1]);
  assert.ok(keys.includes('a') && keys.includes('b'), '缺少题 A 或题 B：' + chapterPath);
  assert.equal(new Set(keys).size, keys.length, '题目锚点重复：' + chapterPath);

  const links = sections.map((match, index) => {
    const [, key, label, title] = match;
    assert.equal(key.toUpperCase(), label);
    const body = workbook.slice(match.index, sections[index + 1]?.index);
    const anchor = 'transfer-' + key;
    assert.ok(chapter.includes('<a id="' + anchor + '"></a>'), '原题目书签失效：' + chapterPath + '#' + anchor);
    assert.ok(chapter.includes('(../' + workbookPath + '#' + anchor + ')'), '正文缺少练习入口：' + workbookPath);
    assert.match(body, /^`{3}(?:ts|typescript|js|javascript)\b/m, '题里应就地讲代码：' + workbookPath + ' ' + key);
    return label + ' · [' + title.trim() + '](' + workbookPath + '#transfer-' + key + ')';
  });

  const lab = 'labs/transfer/' + id + '.ts';
  assert.match(readFileSync(lab, 'utf8'), /\bassert[.(]/, '发散实验无断言：' + lab);
  lines.push('| [' + chapterTitle + '](' + chapterPath + ') | ' + links.join('；') + ' | [实验源码](' + lab + ') |');
  questionCount += sections.length;
}
lines.push('', '单章运行 `npm run transfer -- 07`。省略编号会依次执行全部发散实验。原主线实验可用 `npm run lab -- 07` 运行。', '');

const result = lines.join('\n');
if (process.argv.includes('--check')) {
  assert.equal(readFileSync(filename, 'utf8'), result, '运行 npm run index 更新题目导航');
} else {
  writeFileSync(filename, result);
}
console.log('已核对 17 章、' + questionCount + ' 个练习入口及解题代码、17 份独立实验。');
