import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import ts from 'typescript';
import { resolve } from 'node:path';

const read = path => readFileSync(path,'utf8');
const outline = read('RxJS-教程大纲.md');
const manifest = JSON.parse(read('node_modules/rxjs/package.json'));
assert.equal(manifest.version,'7.8.2');
const inputs = Object.entries(manifest.exports).filter(([key,value]) => value.types && !key.includes('*')).map(([key,value]) => [key.replace('.','rxjs'),resolve('node_modules/rxjs',value.types)]);
const program = ts.createProgram(inputs.map(([,file]) => file),{skipLibCheck:true,target:ts.ScriptTarget.ES2022});
const checker = program.getTypeChecker();
const published = inputs.flatMap(([entry,file]) => checker.getExportsOfModule(checker.getSymbolAtLocation(program.getSourceFile(file))).map(symbol => entry + '|' + symbol.name)).sort();
const declares = new Map();
let entry = '';
for (const line of read('RxJS-7.8.2-API清单.md').split('\n')) {
  const group = line.match(/^## (rxjs[^（]*)（/);
  if (group) entry = group[1];
  const item = line.match(/^- \[[ xX]\] \[(\w+)\]\(#(api-\d+)\)/);
  if (item) declares.set(entry + '|' + item[1],item[2]);
}
assert.deepEqual([...declares.keys()].sort(),published);
const planned = new Map();
const chapters = new Map();
for (const chunk of outline.split(/^## /m).slice(1,-1)) {
  const id = chunk.startsWith('结业') ? '17' : chunk.slice(0,2);
  const chapter = read('book/' + id + '.md');
  const lab = read('labs/' + id + '.ts');
  const practice = new Map();
  const tasks = chunk.split('**本章回访（不重复计数）**')[1].split('\n').filter(line => line.startsWith('- '));
  tasks.forEach((task,index) => {
    const [names, ...question] = task.split('：');
    const anchor = 'practice-' + (index+1);
    assert.ok(chapter.includes('<a id="'+anchor+'"></a>') && chapter.includes(question.join('：')),'缺少章节自测题：'+id+' '+anchor);
    for (const [,name] of names.matchAll(/`(\w+)`/g)) practice.set(name,{anchor,index});
  });
  chapters.set(id,{chapter,lab,practice});
  const section = chunk.split('**本章回访')[0];
  for (const match of section.matchAll(/^- `(rxjs[^`]*)`：(.*)$/gm)) {
    for (const [,name] of match[2].matchAll(/`(\w+)`/g)) {
      const key = match[1] + '|' + name;
      assert.ok(!planned.has(key),'重复主讲：' + key);
      planned.set(key,id);
    }
  }
}
assert.deepEqual([...planned.keys()].sort(),published);
const lines = ['# 教师索引：从 API 找正文和实验','','先做章节开头的无提示题，再用这页查漏。声明链接给出精确重载；主讲章节解释为什么选它，实验列是该章比较行为的工作台，不代表每个类型都有独立的运行时对象。',''];
const focus = [...outline.matchAll(/^\| `(\w+)` \| (\d+) \| (.*?) \|$/gm)];
assert.equal(focus.length,26);
function found(id, name) {
  const {chapter,lab} = chapters.get(id);
  const word = new RegExp('(?<![A-Za-z0-9_$])'+name+'(?![A-Za-z0-9_$])');
  return [chapter,lab].some(content => word.test(content));
}
lines.push('## 26 个重点的间隔练习','','每格是不同章节的任务，同章出现多次仍只算一次。先在纸上预测，跑实验，再改变一个条件。不会的格子写下使自己改判的反例，不要只勾选读过。','','| API | 主讲 | 后续练习 |','|---|---|---|');
const chapterLink = id => '[第 '+id+' 章](book/'+id+'.md)';
for (const [,name,first,refs] of focus) {
  const ids = refs.split('、').map(value => value === '结业' ? '17' : value);
  assert.ok(new Set(ids).size>=5 && ids.every(id=>id>first));
  for (const id of ids) assert.ok(found(id,name),'缺回访正文或实验：' + id + ' ' + name);
  lines.push('| `'+name+'` | '+chapterLink(first)+' | '+ids.map(id=>{
    const task=chapters.get(id).practice.get(name);
    assert.ok(task,'缺少回访题：'+id+' '+name);
    return '[第 '+id+' 章 · 题 '+(task.index+1)+'](book/'+id+'.md#'+task.anchor+')';
  }).join('、')+' |');
}
for (const [entry] of inputs) {
  const keys = published.filter(key=>key.split('|')[0]===entry);
  lines.push('','## '+entry+'（'+keys.length+'）','','| 导出名与声明 | 主讲 | 对照实验 |','|---|---|---|');
  for (const key of keys) {
    const id = planned.get(key), name = key.split('|')[1];
    assert.ok(found(id,name),'缺主讲正文或实验：' + key + ' → ' + id);
    lines.push('| [`'+name+'`](RxJS-7.8.2-API清单.md#'+declares.get(key)+') | '+chapterLink(id)+' | [实验 '+id+'](labs/'+id+'.ts) |');
  }
}
const contents = lines.join('\n')+'\n';
if (process.argv.includes('--check')) assert.equal(read('API-正文索引.md'),contents,'索引过期，请 npm run index');
else writeFileSync('API-正文索引.md',contents);
console.log('已核对：'+published.length+' 个入口导出、'+chapters.size+' 章实验、26 × 至少 5 个跨章回访定位。名称与链接检查不替代读者的独立作答。');
