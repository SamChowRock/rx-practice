import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

for (const directory of ['./', './harbor/']) {
  const local = name => new URL(directory + name, import.meta.url);
  const bundle = await build({
    entryPoints: [fileURLToPath(local('main.ts'))],
    bundle: true, minify: true, format: 'iife', write: false,
  });
  const html = await readFile(local('screen.html'), 'utf8');
  await writeFile(local('play.html'), html.replace('/* GAME_SCRIPT */', () => bundle.outputFiles[0].text.replace(/<\/script/gi, '<\\/script')));
}
