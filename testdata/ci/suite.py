"""Native Go test partitions, event accounting, and atomic coverage (stdlib only)."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time

ENGINE = "pi-workflow-controller/internal/engine"
QUEUE = "TestEnginePersistenceObservationQueueSaturation"
# Only opt-in integration identities are configured here, never a case registry.
OPTINS = {
    ENGINE: {
        "TestEngineBundledPublicationAndCancellation": "snapshot-reuse-isolation cancel-candidate timeout-candidate cancel-staged cancel-confirmed".split(),
        "TestEngineLiveCancellationTimeoutRecovery": [""],
    },
    "pi-workflow-controller/internal/runtime": {
        "TestBundledStartupMemoryReuseCleanup": [""],
        "TestBundledInputActions": ["handled", "transform"],
        "TestBundledDialogs": ["select", "confirm", "input", "editor"],
        "TestBundledBusyRPCRejection": [""],
        "TestBundledProviderRetry": ["success", "exhaust", "cancel-sleep"],
        "TestBundledAutoCompaction": ["success", "aborted", "failed"],
        "TestBundledProcessAndPipeFailure": ["process-exit", "pipe-failure"],
        "TestBundledPartialStartup": ["missing-executable", "invalid-model", "unsupported-thinking"],
        "TestBundledBashCancellation": [""],
        "TestBundledIndependentCleanupInsurance": [""],
        "TestBundledTerminalOutcomes": ["stop", "error", "length"],
    },
    "pi-workflow-controller/internal/workflows": {
        "TestSmokeEchoBundledDefinition": ["match-unicode-quotes-shell-data", "mismatch-valid-contract"],
    },
}
ALLOWED_SKIPS = {(p, t + ("/" + child if child else ""))
                 for p, tests in OPTINS.items() for t, children in tests.items() for child in children}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value):
    with Path(path).open("x") as out:
        json.dump(value, out, indent=2, sort_keys=True)
        out.write("\n")


def load(path):
    return json.loads(Path(path).read_text())


def anchored(names):
    return "^(" + "|".join(re.escape(n) for n in sorted(names)) + ")$" if names else "^$"


def complement(word):
    parts = []
    for i, ch in enumerate(word):
        tail = "[^" + re.escape(ch) + "].*"
        parts.append(re.escape(word[:i]) + ("(" + tail + ")?" if i else tail))
    return "^(" + "|".join(parts + [re.escape(word) + ".+"]) + ")$"


def partition(tops):
    require(tops and all(len(v) == len(set(v)) for v in tops.values()), "empty/duplicate inventory")
    groups = []
    def add(identity, package, names, run=None, parent=None):
        groups.append(dict(id=identity, package=package, tops=sorted(names),
                           run=run or anchored(names), parent=parent))
    for package, names in sorted(tops.items()):
        selected = set(names)
        if package == ENGINE:
            require(QUEUE in selected, "missing queue parent")
            selected.remove(QUEUE)
            add("queue-small", package, [QUEUE], "^" + QUEUE + "$/" + complement("default1024"), QUEUE)
            add("queue-stress", package, [QUEUE], "^" + QUEUE + "$/^default1024$", QUEUE)
        add("base-" + package.removeprefix("pi-workflow-controller/").replace("/", "-"), package, selected)
    require(len({g['id'] for g in groups}) == len(groups), "group ID collision")
    return groups


def owner(groups, package, name):
    top, *tail = name.split("/")
    candidates = [g for g in groups if g['package'] == package and top in g['tops']]
    if not tail and len(candidates) > 1:
        require(all(g['parent'] == top for g in candidates), "overlapping top-level partition: " + name)
        return None  # Shared ancestors are structural events, not repeated leaves.
    matches = [g['id'] for g in candidates if not g['parent'] or
               (tail and re.fullmatch(g['run'].split("/", 1)[1], tail[0]))]
    require(len(matches) == 1, "unclassified/overlapping test: " + package + ":" + name)
    return matches[0]


def normalize(package, name):
    if package == "pi-workflow-controller/internal/contract" and name.startswith("TestRegistryNeverLoadsExternalReferences/"):
        name = re.sub(r"http://127\.0\.0\.1:\d+/schema\.json", "http://127.0.0.1:<port>/schema.json", name)
        name = re.sub(r"file://[^\s]*?/TestRegistryNeverLoadsExternalReferences\d+/001/external\.json", "file://<owned-temp>/external.json", name)
    return name


def events(path, group, count=1):
    active, terminals, outputs, raw_names = set(), collections.defaultdict(list), collections.defaultdict(str), {}
    package_terms = []
    starts = 0
    with Path(path).open() as source:
        for number, line in enumerate(source, 1):
            require(line.endswith("\n"), f"truncated JSON line {number}")
            event = json.loads(line)
            require(isinstance(event, dict) and event.get('Package') == group['package'], "wrong package event")
            action, raw = event.get('Action'), event.get('Test')
            require(action in {'start', 'run', 'pause', 'cont', 'output', 'pass', 'fail', 'skip', 'bench'}, "unknown action")
            if not raw:
                if action == 'start':
                    starts += 1
                if action in ('pass', 'fail', 'skip'):
                    package_terms.append(action)
                continue
            name = normalize(group['package'], raw)
            if action == 'run':
                slot = (name, len(terminals[name]))
                require(slot not in raw_names or raw_names[slot] == raw, "normalization collision")
                raw_names[slot] = raw
                require(name not in active, "duplicate run: " + name)
                active.add(name)
            elif action == 'output':
                outputs[name] += event.get('Output', '')
            elif action in ('pass', 'fail', 'skip'):
                require(name in active, "terminal without run: " + name)
                active.remove(name)
                terminals[name].append(action)
    require(not active, "unfinished tests: " + str(sorted(active)))
    expected_package = (['pass'],) if group['tops'] else (['pass'], ['skip'])
    require(starts == 1 and package_terms in expected_package, "failed/unfinished package")
    require(bool(group['tops']) or not terminals, "unexpected tests in empty partition")
    require(all(len(actions) == count for actions in terminals.values()), "wrong occurrence count")
    require(set(group['tops']) <= terminals.keys(), "missing top-level tests")
    if group['parent']:
        require(any(n.startswith(group['parent'] + '/') for n in terminals), "parent-only partition")
    for name, actions in terminals.items():
        require('fail' not in actions, "failed test: " + name)
        if 'skip' in actions:
            require((group['package'], name) in ALLOWED_SKIPS and
                    ('PWC_BUNDLED_PI=1' in outputs[name] or 'PWC_LIVE_PI=1' in outputs[name]), "unexpected skip: " + name)
    return dict(terminals)


def collect(plan, directory, mode):
    directory = Path(directory)
    expected = {g['id'] for g in plan['groups']}
    paths = list(directory.glob(mode + "--*.result.json"))
    require({p.name[len(mode)+2:-12] for p in paths} == expected and len(paths) == len(expected), "missing/extra/duplicate group")
    if mode == 'race':
        require({p.name[len(mode)+2:-4] for p in directory.glob(mode + '--*.out')} == expected,
                'missing/extra coverage profile')
    union, duplicates, profiles, timings = {}, [], {}, {}
    for group in plan['groups']:
        identity = group['id']
        stem = directory / (mode + "--" + identity)
        result = load(str(stem) + '.result.json')
        require(result['exit'] == 0 and result['identity'] == plan['identity'] and
                result['mode'] == mode and result['group'] == group and result['count'] == 1, "failed/stale group")
        terms = events(str(stem) + '.jsonl', group)
        for name, actions in terms.items():
            assigned = owner(plan['groups'], group['package'], name)
            require(assigned is None or assigned == identity, "wrong group ownership: " + name)
            key = group['package'] + ':' + name
            if key in union:
                require(assigned is None and union[key] == actions, "duplicate leaf: " + name)
                duplicates.append(key)
            union[key] = actions
        profiles[identity] = Path(str(stem) + '.out')
        timings[identity] = result['seconds']
    return {'union': union, 'structural_duplicates': duplicates, 'seconds': timings}, profiles


def profile(path):
    path = Path(path)
    require(stat.S_ISREG(path.lstat().st_mode), "profile not regular")
    text = path.read_text()
    require(text.startswith('mode: atomic\n') and text.endswith('\n'), "mode conflict/truncated profile")
    blocks = {}
    for line in text.splitlines()[1:]:
        match = re.fullmatch(r"(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)", line)
        require(match is not None, "invalid coverage row")
        file, *values = match.groups()
        sl, sc, el, ec, stmts, hits = map(int, values)
        require(min(sl, sc, el, ec) > 0 and (el, ec) >= (sl, sc), "invalid coordinates")
        key = (file, sl, sc, el, ec)
        require(key not in blocks, "duplicate block")
        blocks[key] = (stmts, hits)
    return blocks


def merge(groups, profiles, references, destination):
    require(len(groups) == len({g['id'] for g in groups}), "duplicate profile ID")
    require(set(profiles) == {g['id'] for g in groups}, "missing/extra profile")
    require(set(references) == {g['package'] for g in groups}, "missing/extra reference")
    inodes = []
    for p in list(profiles.values()) + list(references.values()):
        require(stat.S_ISREG(Path(p).lstat().st_mode), "profile symlink/nonregular")
        st = Path(p).stat()
        inodes.append((st.st_dev, st.st_ino))
    require(len(inodes) == len(set(inodes)), "duplicate profile path/inode")
    inventories = {p: profile(path) for p, path in references.items()}
    for package, blocks in inventories.items():
        require(all(k[0].rsplit('/', 1)[0] == package for k in blocks), "reference package mismatch")
    combined = {}
    for group in groups:
        blocks = profile(profiles[group['id']])
        reference = inventories[group['package']]
        require({k: v[0] for k, v in blocks.items()} == {k: v[0] for k, v in reference.items()}, "block/statement inventory mismatch")
        for key, (stmts, hits) in blocks.items():
            old_stmts, old_hits = combined.get(key, (stmts, 0))
            require(stmts == old_stmts, "statement conflict")
            combined[key] = (stmts, old_hits + hits)
    with Path(destination).open('x') as out:
        out.write('mode: atomic\n')
        for (file, sl, sc, el, ec), (stmts, hits) in sorted(combined.items()):
            out.write(f'{file}:{sl}.{sc},{el}.{ec} {stmts} {hits}\n')
    return {'blocks': len(combined), 'statements': sum(v[0] for v in combined.values()),
            'covered_statements': sum(v[0] for v in combined.values() if v[1])}


def identity():
    # Include embedded inputs; CI tooling/docs do not change Go instrumentation.
    paths = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard']).decode().split('\0')
    files = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in sorted(set(paths))
             if p and Path(p).is_file() and (p.startswith(('internal/', 'cmd/')) or p in ('go.mod', 'go.sum'))}
    return {'files': files, 'go': subprocess.check_output(['go', 'version'], text=True).strip(),
            'target': subprocess.check_output(['go', 'env', 'GOOS', 'GOARCH', 'CGO_ENABLED'], text=True).splitlines()}


def inventory(destination):
    packages = subprocess.check_output(['go', 'list', '-mod=readonly', '-p', '1', './...'], text=True).splitlines()
    tops = {}
    for package in packages:
        raw = subprocess.check_output(['go', 'test', '-p', '1', '-list', '.', package], text=True)
        tops[package] = [line for line in raw.splitlines() if re.fullmatch(r'(Test|Fuzz|Example)\w*', line)]
    save(destination, {'identity': identity(), 'tops': tops, 'groups': partition(tops)})


def run_group(plan, group, mode, directory, count=1):
    require(identity() == plan['identity'], 'stale inventory')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stem = directory / (mode + '--' + group['id'])
    command = ['go', 'test', '-p', '1', '-json', '-count=' + str(count), '-timeout=15m', '-run', group['run']]
    if mode in ('race', 'target-race', 'reference'):
        command.append('-race')
    if mode in ('race', 'reference'):
        require(not Path(str(stem) + '.out').exists(), 'profile already exists')
        command += ['-covermode=atomic', '-coverprofile=' + str(stem) + '.out']
    command.append(group['package'])
    started = time.monotonic()
    with Path(str(stem) + '.jsonl').open('x') as out, Path(str(stem) + '.stderr').open('x') as err:
        result = subprocess.run(command, stdout=out, stderr=err)
    save(str(stem) + '.result.json', {'exit': result.returncode, 'group': group, 'mode': mode,
         'count': count, 'identity': plan['identity'], 'command': command, 'seconds': time.monotonic()-started})
    require(result.returncode == 0, 'go test failed: ' + str(stem))
    if mode != 'reference':
        events(str(stem) + '.jsonl', group, count)
    require(identity() == plan['identity'], 'source changed during test')


def targeted(plan):
    result = []
    for ident, package, names, selector in [
        ('target-host', 'pi-workflow-controller/internal/testutil/protocol',
         ['TestHostRegisterCleanup', 'TestHostRegisterCleanupFailures'], None),
        ('target-engine', ENGINE, [QUEUE, 'TestEnginePersistenceFinalizationIOFailures', 'TestEnginePersistenceStageConfirmBoundary'], None),
    ]:
        require(set(names) <= set(plan['tops'][package]), 'missing targeted parent')
        result.append(dict(id=ident, package=package, tops=names, run=selector or anchored(names), parent=None))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['inventory', 'matrix', 'run', 'reference', 'check'])
    parser.add_argument('--plan', required=True)
    parser.add_argument('--directory')
    parser.add_argument('--group')
    parser.add_argument('--mode', choices=['normal', 'race', 'target-normal', 'target-race'])
    args = parser.parse_args()
    if args.operation == 'inventory':
        inventory(args.plan)
        return
    plan = load(args.plan)
    if args.operation == 'matrix':
        print(json.dumps({'include': [{'group': g['id'], 'mode': m} for m in ('normal', 'race') for g in plan['groups']] +
                         [{'group': g['id'], 'mode': m} for m in ('target-normal', 'target-race') for g in targeted(plan)]}))
    elif args.operation == 'run':
        groups = targeted(plan) if args.mode.startswith('target-') else plan['groups']
        group = next(g for g in groups if g['id'] == args.group)
        run_group(plan, group, args.mode, args.directory, 3 if args.mode.startswith('target-') else 1)
    elif args.operation == 'reference':
        for package in plan['tops']:
            group = dict(id=package.replace('/', '-'), package=package, tops=[], run='^$', parent=None)
            run_group(plan, group, 'reference', args.directory)
    else:
        normal, _ = collect(plan, args.directory, 'normal')
        race, profiles = collect(plan, args.directory, 'race')
        require(normal['union'] == race['union'], 'normal/race case mismatch')
        for mode in ('target-normal', 'target-race'):
            for group in targeted(plan):
                stem = Path(args.directory) / (mode + '--' + group['id'])
                result = load(str(stem) + '.result.json')
                require(result['exit'] == 0 and result['identity'] == plan['identity'] and
                        result['group'] == group and result['mode'] == mode and result['count'] == 3, 'failed/stale targeted group')
                terms = events(str(stem) + '.jsonl', group, 3)
                expected = {}
                for key, actions in normal['union'].items():
                    package, name = key.split(':', 1)
                    top, *tail = name.split('/')
                    if package == group['package'] and top in group['tops'] and (
                            not group['parent'] or not tail or re.fullmatch(group['run'].split('/', 1)[1], tail[0])):
                        expected[name] = actions * 3
                require(terms == expected, 'targeted/full collection mismatch')
        references = {}
        for package in plan['tops']:
            stem = Path(args.directory) / ('reference--' + package.replace('/', '-'))
            result = load(str(stem) + '.result.json')
            require(result['exit'] == 0 and result['identity'] == plan['identity'] and result['mode'] == 'reference', 'failed/stale reference')
            references[package] = Path(str(stem) + '.out')
        coverage = merge(plan['groups'], profiles, references, Path(args.directory) / 'coverage.out')
        save(Path(args.directory) / 'summary.json', dict(normal=normal, race=race, coverage=coverage))


if __name__ == '__main__':
    main()
