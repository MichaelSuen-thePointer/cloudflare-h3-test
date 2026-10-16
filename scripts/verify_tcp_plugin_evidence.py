"""Verify the frozen 2026-10-16 TCP-plugin evidence without network access."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('directory', nargs='?', type=Path,
                    default=Path(__file__).resolve().parents[1] /
                    'test-results/tcp-plugin-throughput-qlog-20261016')
root = parser.parse_args().directory.resolve()


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def sha(stream):
    h = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        h.update(chunk)
    return h.hexdigest()


def close(actual, expected):
    if actual is None or expected is None:
        assert actual is expected, (actual, expected)
    else:
        assert math.isclose(actual, expected, rel_tol=1e-11, abs_tol=1e-9), (actual, expected)


manifest = read(root / 'manifest.json')
qlogs = 0
for entry in manifest['items'] + manifest['generated_files']:
    path = (root / entry['archived']).resolve()
    assert path.is_relative_to(root), 'manifest path escapes evidence directory'
    with path.open('rb') as f:
        assert sha(f) == entry['archived_sha256'], f'archive digest mismatch: {path.name}'
    if entry.get('kind') == 'lossless raw qlog':
        qlogs += 1
        with gzip.open(path, 'rb') as f:
            assert sha(f) == entry['source_sha256'], f'gzip content mismatch: {path.name}'
assert qlogs == 10, qlogs

comparison = {(r['dataset'], r['name']): r for r in read(root / 'comparison.json')}
measured = warmups = 0
for dataset in read(root / 'index.json'):
    folder = root / dataset['dataset']
    rows = [json.loads(line) for line in (folder / 'results.jsonl').read_text().splitlines()]
    assert len(rows) == dataset['rows']
    assert len({(r['name'], r['round']) for r in rows}) == len(rows), 'duplicate attempt'
    cloudflare_speed = dataset['dataset'].startswith(('01-', '02-'))
    for row in rows:
        is_warmup = row['round'] == 0
        warmups += is_warmup
        measured += not is_warmup
        expected_bytes = 1048576 if is_warmup else (10485760 if cloudflare_speed else 52428800)
        expected_code = 206 if is_warmup and not cloudflare_speed else 200
        assert row['valid'] == (row['curl_exit'] == 0 and row['http_code'] == expected_code
                                and row['bytes'] == expected_bytes), (dataset['dataset'], row)
        close(row['mbps'], row['bytes'] * 8 / row['seconds'] / 1e6)
    if (folder / 'timeline.jsonl').exists():
        timeline = [json.loads(line) for line in (folder / 'timeline.jsonl').read_text().splitlines()]
        assert {(r['name'], r['round']) for r in timeline} == {(r['name'], r['round']) for r in rows}
        assert all(t['start_unix_ms'] <= t['end_unix_ms'] for t in timeline)
    for name in dict.fromkeys(r['name'] for r in rows):
        attempts = [r for r in rows if r['name'] == name and r['round'] > 0]
        valid = [r for r in attempts if r['valid']]
        eligible = [r for r in attempts if r['http_code'] != 429]
        saved = comparison[(dataset['dataset'], name)]
        assert saved['attempts'] == len(attempts)
        assert saved['complete'] == len(valid)
        for code, field in [(429, 'http429')]:
            assert saved[field] == sum(r['http_code'] == code for r in attempts)
        for code in (28, 56):
            assert saved[f'curl{code}'] == sum(r['curl_exit'] == code for r in attempts)
        close(saved['median_success_mbps'], statistics.median(r['mbps'] for r in valid) if valid else None)
        close(saved['successful_aggregate_mbps'],
              sum(r['bytes'] for r in valid) * 8 / sum(r['seconds'] for r in valid) / 1e6 if valid else None)
        close(saved['complete_goodput_excluding_http429_mbps'],
              sum(r['bytes'] for r in valid) * 8 / sum(r['seconds'] for r in eligible) / 1e6 if eligible else None)

assert (measured, warmups) == (102, 18), (measured, warmups)
for name, expected in [('05-qlog-investigation', 4), ('06-stream-window-ab', 6)]:
    analysis = read(root / name / 'qlog-analysis.json')
    assert len(analysis['connections']) == expected
    assert all(c['parse_errors'] == 0 and c['connection_end'] for c in analysis['connections'])

print(f'OK: {measured} measured attempts, {warmups} warmups, {qlogs} lossless qlogs; hashes and statistics verified.')
