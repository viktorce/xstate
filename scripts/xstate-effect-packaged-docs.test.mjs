import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const pkgDir = join(root, 'packages', 'xstate-effect');
const pkg = JSON.parse(readFileSync(join(pkgDir, 'package.json'), 'utf8'));
const readme = readFileSync(join(pkgDir, 'README.md'), 'utf8');

// Every README link that points into the package's own docs/ directory
// must resolve to a file that actually ships with the published package.
const docLinks = [
  ...readme.matchAll(/\]\((\.\/)?(docs\/[^)#]+\.md)(?:#[^)]*)?\)/g),
].map((m) => m[2]);

test('README docs links exist in the repo', () => {
  assert.ok(docLinks.length > 0, 'README should link into docs/');
  for (const doc of docLinks) {
    assert.ok(
    existsSync(join(pkgDir, doc)),
    `README links to ${doc} but the file is missing`
    );
  }
});

test('every README docs link is included in the published package "files"', () => {
  assert.ok(Array.isArray(pkg.files), 'package.json must declare a "files" array');
  const files = new Set(pkg.files.map((f) => f.replace(/\/$/, '')));
  const missing = [...new Set(docLinks.map((link) => link.split('/')[0]))].filter(
    (dir) => !files.has(dir) && !files.has(`${dir}/`)
  );
  assert.deepEqual(
    missing,
    [],
    `README links into ${missing.join(', ')}/ but package.json "files" omits that directory`
  );
});
