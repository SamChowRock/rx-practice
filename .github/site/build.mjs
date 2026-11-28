import { execFileSync } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import GithubSlugger from 'github-slugger';
import { load } from 'cheerio/slim';
import { createHighlighter } from 'shiki';

const siteTitle = 'RxJS 实战教程';
const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../..');
const out = path.join(here, 'dist');
const files = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8' })
  .split('\0').filter(file => file && !file.startsWith('.') && path.basename(file) !== 'AGENTS.md');
const theme = 'github-dark';
const highlighter = await createHighlighter({
  themes: [theme],
  langs: ['bash', 'typescript', 'javascript'],
});
const languages = new Set(highlighter.getLoadedLanguages());
const aliases = new Map([['sh', 'bash'], ['ts', 'typescript']]);
const md = new MarkdownIt({
  html: true,
  highlight(source, label) {
    const lang = aliases.get(label) ?? label;
    if (!languages.has(lang)) return '';
    return highlighter.codeToHtml(source, { lang, theme });
  },
});
const escape = md.utils.escapeHtml;
const pages = new Map();
const targets = new Set();
const links = [];


const githubMark = (await readFile(path.join(here, 'github-mark.svg'), 'utf8')).trim();
const iconPaths = {
  contents: '<path d="M8 5h12M8 12h12M8 19h12"/><circle cx="4" cy="5" r=".7"/><circle cx="4" cy="12" r=".7"/><circle cx="4" cy="19" r=".7"/>',
  settings: '<path d="M3 7h3m6 0h9M3 17h9m6 0h3"/><circle cx="9" cy="7" r="3"/><circle cx="15" cy="17" r="3"/>',
  theme: '<circle cx="12" cy="12" r="8.5"/><path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor" stroke="none"/>',
};

function icon(name) {
  return `<svg class="nav-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor"
    stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">${iconPaths[name]}</svg>`;
}

function htmlPath(file) {
  if (file === 'README.md') return 'guide.html';
  return file.replace(/(^|\/)README\.md$/, '$1index.md').replace(/\.md$/, '.html');
}

async function save(file, contents) {
  await mkdir(path.join(out, path.dirname(file)), { recursive: true });
  await writeFile(path.join(out, file), contents);
  targets.add(file);
}

function readerContent($) {
  const headings = $('h2').length ? $('h2') : $('h3');
  const sections = headings.toArray().map(el =>
    `<a href="#${escape(encodeURIComponent($(el).attr('id')))}">${escape($(el).text())}</a>`
  ).join('');
  const rail = sections ? `<aside><details class="mobile-index"><summary>本页目录</summary>${sections}</details>
    <div class="desktop-index"><strong>本页目录</strong>${sections}</div></aside>` : '';
  return `<main class="reader">${rail}<article>${$.html()}</article></main>`;
}

async function writePage(dest, title, content) {
  const base = path.relative(path.dirname(dest), '.') || '.';
  const nav = `<nav aria-label="阅读导航"><div class="bar">
    <a class="brand" href="${base}/index.html">${escape(siteTitle)}</a>
    <a href="${base}/index.html#chapters">${icon('contents')}全书目录</a>
    <a href="${base}/game/index.html">${icon('settings')}游戏实战</a>
    <a href="https://github.com/SamChowRock/rx-practice">${githubMark}GitHub 仓库</a>
    <label class="theme-control" hidden>${icon('theme')}
      <select class="theme-select" aria-label="页面主题" title="页面主题">
      <option value="system">跟随系统</option><option value="light">浅色模式</option><option value="dark">深色模式</option>
      </select>
    </label>
  </div></nav>`;
  const body = '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>' +
    escape(title) + '</title><script src="' + base + '/assets/theme.js"></script><link rel="stylesheet" href="' + base + '/assets/book.css"></head><body>' +
    nav + content + '</body></html>';
  const page = load(body);
  pages.set(dest, new Set(page('[id]').map((_, el) => page(el).attr('id')).get()));
  page('[href],[src]').each((_, el) => links.push([dest, page(el).attr('href') ?? page(el).attr('src')]));
  await save(dest, body);
}

const chapters = files.filter(file => /^book\/\d{2}\.md$/.test(file)).sort();
const lessons = files.filter(file => /^game\/\d{2}-.+\.md$/.test(file)).sort();
const readingOrder = [...chapters, 'game/README.md', ...lessons];
const titles = new Map();
const sources = new Set();
for (const file of files.filter(name => name.endsWith('.md'))) {
  const markdown = await readFile(path.join(root, file), 'utf8');
  const $ = load(md.render(markdown), {}, false);
  const slugger = new GithubSlugger();
  $('h1,h2,h3,h4,h5,h6').each((_, el) => $(el).attr('id', slugger.slug($(el).text())));
  const title = $('h1').first().text();
  titles.set(file, title);
  const dest = htmlPath(file);
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (/^(?:[a-z][\w+.-]*:|\/\/)/i.test(href)) return;
    const [, pathname, suffix] = href.match(/^([^?#]*)(.*)$/s);
    if (!pathname) return;
    const source = path.normalize(path.join(path.dirname(file), decodeURIComponent(pathname)));
    let target;
    if (source.endsWith('.md')) target = htmlPath(source);
    if (/\.(ts|mjs)$/.test(source)) {
      if (!files.includes(source)) throw new Error('找不到实验源码 ' + source);
      sources.add(source);
      target = source + '.html';
    }
    if (target) $(el).attr('href', path.relative(path.dirname(dest), target) + suffix);
  });
  for (const image of $('img[src^="../figures/"]').toArray()) {
    const src = $(image).attr('src');
    const asset = path.join(path.dirname(file), decodeURIComponent(src));
    if (!targets.has(asset)) await save(asset, await readFile(path.join(root, asset)));
    $(image).parent('a').wrap('<span class="timeline-scroll"></span>');
  }
  $('table').wrap('<div class="table-scroll"></div>');
  const index = readingOrder.indexOf(file);
  if (index !== -1) {
    const pager = $('<div class="pager" aria-label="顺序阅读"></div>');
    for (const [next, label] of [[index - 1, '← 上一节'], [index + 1, '下一节 →']]) {
      if (readingOrder[next]) pager.append($('<a></a>').attr('href',
        path.relative(path.dirname(dest), htmlPath(readingOrder[next]))).text(label));
    }
    $.root().append(pager);
  }
  await writePage(dest, title, readerContent($));
}

for (const file of sources) {
  const code = await readFile(path.join(root, file), 'utf8');
  const html = highlighter.codeToHtml(code, {lang: file.endsWith('.ts') ? 'typescript' : 'javascript', theme});
  await writePage(file + '.html', file, '<main class="reader"><article><h1>' + escape(file) + '</h1>' + html + '</article></main>');
}

function section(title, entries) {
  const rows = entries.map(file => {
    const text = titles.get(file);
    const [prefix, ...rest] = text.split('｜');
    const label = rest.length ? '<span class="chapter-num">' + escape(prefix) + '</span>' + escape(rest.join('｜')) : escape(text);
    return '<h4><a href="' + escape(htmlPath(file)) + '">' + label + '</a></h4>';
  }).join('');
  return '<section class="part"><h3>' + title + '</h3><div class="chapter-list">' + rows + '</div></section>';
}
const intro = load(md.render(await readFile(path.join(root,'README.md'),'utf8')), {}, false);
const groups = [
  ['走进实时控制台',1], ['观察一次执行',3], ['时间与在途工作',3], ['多个参与者',3], ['真实线路与测试',4],
  ['事故回放与独立排错',3], ['结业与独立练习',2], ['强化篇 · 构建归档阅读器',3],
  ['强化篇 · 从报告生成到下载',3],
];
let cursor = 0;
const outline = groups.map(([title, count]) => {
  const entries = chapters.slice(cursor, cursor + count);
  cursor += count;
  return section(title, entries);
}).join('');
if (cursor !== chapters.length) throw new Error('请为新增章节指定目录分组');
const references = [...titles.keys()].filter(file => !readingOrder.includes(file));
await writePage('index.html', siteTitle, `<main class="contents">
  <header class="overview"><div class="overview-copy"><h1>${escape(siteTitle)}</h1>
  <p>${escape(intro('p').first().text())}</p><a class="start" href="book/00.html">从第 0 章开始 →</a></div>
  <div class="overview-index"><strong>本页索引</strong><a href="#chapters">课程正文<span>↘</span></a>
  <a href="#game">游戏实战<span>↘</span></a><a href="#references">索引与配套资料<span>↘</span></a></div></header>
  <details class="intro"><summary>第一次跟做？查看运行方式</summary><p>使用 Node.js 24，在项目根目录运行
  <code>npm ci</code> 和 <code>npm run lab -- 01</code>。<a href="guide.html">完整阅读指南</a></p></details>
  <h2 id="chapters">课程正文</h2>${outline}
  <h2 id="game">游戏实战</h2><p><a class="start" href="game/play.html">玩接星星 →</a> <a class="start" href="game/harbor/play.html">玩雾港引航 →</a></p>
  ${section('实战讲义', ['game/README.md', ...lessons])}
  <h2 id="references">索引与配套资料</h2>${section('学习参考', references)}</main>`);
for (const game of ['game/play.html', 'game/harbor/play.html']) await save(game, await readFile(path.join(root, game)));
for (const name of ['book.css','theme.js']) await save('assets/' + name, await readFile(path.join(here, name)));

const errors = [];
for (const [from, link] of links) {
  const url = new URL(link, 'https://book.invalid/' + from);
  if (url.origin !== 'https://book.invalid') continue;
  const target = decodeURIComponent(url.pathname.slice(1));
  if (!targets.has(target) || (url.hash && !pages.get(target)?.has(decodeURIComponent(url.hash.slice(1))))) {
    errors.push(from + ' → ' + link);
  }
}
if (errors.length) throw new Error('无效站内链接\n' + errors.join('\n'));
console.log(pages.size + ' 个阅读页面，' + links.length + ' 个链接和资源引用已检查。');
