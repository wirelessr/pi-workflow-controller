"""Exercise classification and accounting without replacing Go internals."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import suite


class SuiteTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, name, text):
        path = self.root / name
        path.write_text(text)
        return path

    def test_partition(self):
        tops = {suite.ENGINE: [suite.QUEUE, 'TestNewEngine'], 'new/pkg': ['TestNew']}
        groups = suite.partition(tops)
        cases = [('new/pkg', 'TestNew/new-child', 'base-new-pkg'),
                 (suite.ENGINE, 'TestNewEngine/child', 'base-internal-engine'),
                 (suite.ENGINE, suite.QUEUE, None)]
        for name, expected in [('default1024', 'queue-stress'), ('small2', 'queue-small'),
                               ('default102', 'queue-small'), ('default1024x', 'queue-small'), ('unknown', 'queue-small')]:
            cases.append((suite.ENGINE, suite.QUEUE + '/' + name, expected))
        for package, name, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(suite.owner(groups, package, name), expected)
        for bad in [{}, {suite.ENGINE: ['TestNew']}, {'p': ['TestX', 'TestX']}]:
            with self.subTest(inventory=bad), self.assertRaises(ValueError):
                suite.partition(bad)
        for package, name in [('new/pkg', 'TestUnknown'), ('unknown/pkg', 'TestNew')]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                suite.owner(groups, package, name)
        base = next(g for g in groups if g['package'] == 'new/pkg')
        for suffix in ('', '/child'):
            with self.subTest(overlap_suffix=suffix), self.assertRaises(ValueError):
                suite.owner(groups + [copy.deepcopy(base)], base['package'], base['tops'][0] + suffix)

    def log(self, names, package='p', count=1):
        events = [{'Action': 'start', 'Package': package}]
        for _ in range(count):
            for name in names:
                events.append({'Action': 'run', 'Package': package, 'Test': name})
            for name in reversed(names):
                events.append({'Action': 'pass', 'Package': package, 'Test': name})
        events.append({'Action': 'pass', 'Package': package})
        return events

    def parse(self, rows, group, count=1):
        path = self.write('events.jsonl', ''.join(json.dumps(e) + '\n' for e in rows))
        return suite.events(path, group, count)

    def test_events_table(self):
        group = dict(id='g', package='p', tops=['TestX'], parent='TestX')
        good = self.log(['TestX', 'TestX/a'])
        cases = [('valid', good, 1, True), ('repeat-three', self.log(['TestX', 'TestX/a'], count=3), 3, True),
                 ('parent-only', self.log(['TestX']), 1, False),
                 ('missing-parent', self.log(['TestOther']), 1, False),
                 ('unfinished', good[:-2] + good[-1:], 1, False),
                 ('wrong-count', good, 3, False), ('double-count', self.log(['TestX', 'TestX/a'], count=2), 1, False),
                 ('missing-run', [e for e in good if not(e['Action'] == 'run' and e.get('Test') == 'TestX/a')], 1, False),
                 ('duplicate-run', good[:3] + [good[2]] + good[3:], 1, False),
                 ('package-only', self.log([]), 1, False), ('missing-package-terminal', good[:-1], 1, False)]
        for action in ('fail', 'skip'):
            rows = copy.deepcopy(good)
            rows[3]['Action'] = action
            cases.append((action, rows, 1, False))
        for name, rows, count, valid in cases:
            with self.subTest(name=name):
                if valid:
                    self.assertEqual(set(self.parse(rows, group, count)), {'TestX', 'TestX/a'})
                else:
                    with self.assertRaises(ValueError):
                        self.parse(rows, group, count)
        empty = dict(id='empty', package='p', tops=[], parent=None)
        for terminal in ('skip', 'pass'):
            with self.subTest(empty_package_terminal=terminal):
                self.assertEqual(self.parse([{'Action': 'start', 'Package': 'p'}, {'Action': terminal, 'Package': 'p'}], empty), {})

    def test_compiler_no_test_files_coverage_terminal(self):
        module = self.root / 'no-tests'
        module.mkdir()
        (module / 'go.mod').write_text('module example.test/empty\n\ngo 1.25.0\n')
        (module / 'x.go').write_text('package empty\nfunc Value() int { return 1 }\n')
        group = dict(id='empty', package='example.test/empty', tops=[], parent=None)
        for flags in ([], ['-race', '-covermode=atomic', '-coverprofile=' + str(self.root / 'empty.out')]):
            with self.subTest(flags=flags):
                result = subprocess.run(['go', 'test', '-p', '1', '-json', '-count=1', *flags], cwd=module,
                                        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                self.assertEqual(self.parse([json.loads(line) for line in result.stdout.splitlines()], group), {})

    def test_skip_and_normalization(self):
        package, name = sorted(suite.ALLOWED_SKIPS)[0]
        group = dict(id='skip', package=package, tops=[name.split('/')[0]], parent=None)
        names = [name.split('/')[0], name] if '/' in name else [name]
        rows = self.log(names, package)
        for event in rows:
            if event.get('Test') == name and event['Action'] == 'pass':
                event['Action'] = 'skip'
        for reason, valid in [('PWC_BUNDLED_PI=1', True), ('unrelated skip', False)]:
            with self.subTest(reason=reason):
                actual = rows[:-1] + [{'Action': 'output', 'Package': package, 'Test': name, 'Output': reason}] + rows[-1:]
                if valid:
                    self.parse(actual, group)
                else:
                    with self.assertRaises(ValueError):
                        self.parse(actual, group)
        package = 'pi-workflow-controller/internal/contract'
        parent = 'TestRegistryNeverLoadsExternalReferences'
        names = [parent, parent + '/http://127.0.0.1:123/schema.json', parent + '/http://127.0.0.1:456/schema.json']
        with self.assertRaises(ValueError):
            self.parse(self.log(names, package), dict(id='n', package=package, tops=[parent], parent=None))
        self.assertEqual(suite.normalize('other', names[1]), names[1])

    def test_collection_group_accounting(self):
        group = dict(id='g', package='p', tops=['TestX'], run='^TestX$', parent=None)
        plan = dict(identity={'source': 'same'}, groups=[group])
        path = self.root / 'normal--g.result.json'
        self.write('normal--g.jsonl', ''.join(json.dumps(e) + '\n' for e in self.log(['TestX'])))
        result = dict(exit=0, identity=plan['identity'], group=group, mode='normal', count=1, seconds=1)
        for mutation, valid in [('good', True), ('missing', False), ('extra', False), ('failed', False), ('stale', False)]:
            with self.subTest(mutation=mutation):
                path.write_text(json.dumps(result))
                extra = self.root / 'normal--extra.result.json'
                if extra.exists():
                    extra.unlink()
                if mutation == 'missing':
                    path.unlink()
                elif mutation == 'extra':
                    extra.write_text(json.dumps(result))
                elif mutation in ('failed', 'stale'):
                    changed = dict(result)
                    changed['exit' if mutation == 'failed' else 'identity'] = 1
                    path.write_text(json.dumps(changed))
                if valid:
                    report, _ = suite.collect(plan, self.root, 'normal')
                    self.assertEqual(report['union'], {'p:TestX': ['pass']})
                else:
                    with self.assertRaises(ValueError):
                        suite.collect(plan, self.root, 'normal')

    def test_profile_rejections(self):
        good = 'mode: atomic\np/x.go:1.1,2.2 1 0\n'
        for text in ['', 'mode: count\n', good.rstrip(), good + good.splitlines()[1] + '\n',
                     good.replace('1 0', '1 -1'), good.replace('1.1', '0.1'), 'mode: atomic\ninvalid\n']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                suite.profile(self.write('bad.out', text))

    def test_merge_compiler_inventory(self):
        # The reference block inventory comes from the compiler, including zero-hit blocks.
        module = self.root / 'module'
        module.mkdir()
        (module / 'go.mod').write_text('module example.test/fixture\n\ngo 1.25.0\n')
        (module / 'x.go').write_text('package fixture\nfunc Value(b bool) int { if b { return 1 }; return 2 }\n')
        (module / 'x_test.go').write_text('package fixture\nimport "testing"\nfunc TestValue(t *testing.T) { if Value(true) != 1 { t.Fatal("value") } }\n')
        for name, selector in [('ref', '^$'), ('a', '.'), ('b', '.')]:
            subprocess.run(['go', 'test', '-p', '1', '-race', '-count=1', '-run', selector, '-covermode=atomic',
                            '-coverprofile=' + str(self.root / (name + '.out'))], cwd=module,
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        groups = [dict(id=n, package='example.test/fixture') for n in ('a', 'b')]
        profiles = {n: self.root / (n + '.out') for n in ('a', 'b')}
        references = {'example.test/fixture': self.root / 'ref.out'}
        original = profiles['b'].read_text()
        ref = suite.profile(references['example.test/fixture'])
        self.assertTrue(ref)
        result = suite.merge(groups, profiles, references, self.root / 'merged.out')
        merged = suite.profile(self.root / 'merged.out')
        a = suite.profile(profiles['a'])
        self.assertEqual(result['blocks'], len(ref))
        self.assertEqual(merged, {k: (v[0], v[1] * 2) for k, v in a.items()})
        self.assertTrue(any(v[1] == 0 for v in a.values()))
        with self.assertRaises(ValueError):
            suite.merge(groups + [groups[0]], profiles, references, self.root / 'duplicate-id.out')
        for name in ['missing', 'extra', 'duplicate-path', 'duplicate-inode', 'symlink', 'missing-zero', 'statements', 'mode', 'truncated', 'empty', 'wrong-package']:
            with self.subTest(name=name):
                profiles['b'].write_text(original)
                selected, refs = dict(profiles), dict(references)
                if name == 'missing':
                    del selected['b']
                elif name == 'extra':
                    selected['other'] = profiles['a']
                elif name == 'duplicate-path':
                    selected['b'] = selected['a']
                elif name in ('duplicate-inode', 'symlink'):
                    link = self.root / name
                    if name == 'symlink':
                        link.symlink_to(profiles['a'])
                    else:
                        os.link(profiles['a'], link)
                    selected['b'] = link
                elif name == 'missing-zero':
                    lines = original.splitlines()
                    index = next(i for i, line in enumerate(lines[1:], 1) if line.endswith(' 0'))
                    del lines[index]
                    profiles['b'].write_text('\n'.join(lines) + '\n')
                elif name == 'statements':
                    lines = original.splitlines()
                    block, stmts, hits = lines[1].split()
                    lines[1] = f'{block} {int(stmts)+1} {hits}'
                    profiles['b'].write_text('\n'.join(lines) + '\n')
                elif name == 'mode':
                    profiles['b'].write_text(original.replace('atomic', 'count'))
                elif name == 'truncated':
                    profiles['b'].write_text(original.rstrip())
                elif name == 'empty':
                    profiles['b'].write_text('mode: atomic\n')
                elif name == 'wrong-package':
                    refs = {'other': references['example.test/fixture']}
                with self.assertRaises(ValueError):
                    suite.merge(groups, selected, refs, self.root / ('rejected-' + name))


if __name__ == '__main__':
    unittest.main()
