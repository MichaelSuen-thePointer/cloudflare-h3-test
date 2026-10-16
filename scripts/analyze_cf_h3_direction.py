"""Summarize controlled direction probes without confusing stream and connection closure."""
import argparse
import collections
import datetime as dt
import json
from pathlib import Path


def read_events(path):
    if not path.exists():
        return []
    return [json.loads(s.lstrip('\x1e\ufeff')) for s in path.read_text(encoding='utf-8').splitlines() if s.strip('\x1e\ufeff \r\n')]


def analyze_trace(path, root):
    events = read_events(path)
    states = collections.defaultdict(lambda: {'tx': [], 'rx': [], 'new_tx': [], 'frontier': 0, 'fin': [], 'remote_reset': [], 'remote_stop': []})
    controls, closures, params = [], [], []
    ref = None
    for e in events:
        if 'trace' in e:
            ref = dt.datetime.fromisoformat(e['trace']['common_fields']['reference_time']['wall_clock_time']).timestamp() * 1000
        t = e.get('time', 0)
        name, data = e.get('name'), e.get('data', {})
        if name == 'transport:connection_closed':
            closures.append({'time_ms': t, **data})
        if name == 'transport:parameters_set':
            params.append(data)
        if name not in ('transport:packet_sent', 'transport:packet_received'):
            continue
        direction = 'tx' if name.endswith('sent') else 'rx'
        for f in data.get('frames', []):
            kind, sid = f.get('frame_type'), f.get('stream_id')
            if kind in ('ping', 'ack', 'connection_close'):
                controls.append({'time_ms': t, 'direction': direction, **f})
            if sid is None:
                continue
            s = states[sid]
            if kind == 'stream':
                size = f.get('length', 0)
                if size:
                    s[direction].append({'time_ms': t, 'offset': f.get('offset', 0), 'length': size})
                    end = f.get('offset', 0) + size
                    if direction == 'tx' and end > s['frontier']:
                        s['frontier'] = end
                        s['new_tx'].append({'time_ms': t, 'end': end})
                if f.get('fin'):
                    s['fin'].append({'time_ms': t, 'direction': direction})
            if kind in ('reset_stream', 'stop_sending') and direction == 'rx':
                r = {'time_ms': t, 'wall_unix_ms': ref + t if ref else None, **f}
                for label, key in [('last_tx', 'tx'), ('last_new_tx', 'new_tx'), ('last_rx', 'rx')]:
                    previous = s[key][-1] if s[key] else None
                    r[label] = previous
                    r['after_' + label + '_seconds'] = (t - previous['time_ms']) / 1000 if previous else None
                since = s['new_tx'][-1]['time_ms'] if s['new_tx'] else 0
                r['tx_pings_since_last_new_up'] = sum(x['direction'] == 'tx' and x['frame_type'] == 'ping' and since <= x['time_ms'] <= t for x in controls)
                r['tx_acks_since_last_new_up'] = sum(x['direction'] == 'tx' and x['frame_type'] == 'ack' and since <= x['time_ms'] <= t for x in controls)
                s['remote_reset' if kind == 'reset_stream' else 'remote_stop'].append(r)
    streams = []
    for sid, s in sorted(states.items()):
        if sid % 4:
            continue
        streams.append({'stream_id': sid, 'tx_stream_frames': len(s['tx']), 'rx_stream_frames': len(s['rx']),
                        'new_tx_times_ms': [x['time_ms'] for x in s['new_tx']],
                        'new_tx_gaps_seconds': [(b['time_ms'] - a['time_ms']) / 1000 for a, b in zip(s['new_tx'], s['new_tx'][1:])],
                        'fin': s['fin'], 'remote_reset': s['remote_reset'], 'remote_stop': s['remote_stop']})
    return {'file': str(path.relative_to(root)), 'parse_errors': 0, 'connection_closed': closures,
            'peer_connection_close_frames': [x for x in controls if x['direction'] == 'rx' and x['frame_type'] == 'connection_close'],
            'parameters': params, 'streams': streams}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('directory', type=Path)
    args = p.parse_args()
    root = args.directory
    origin = read_events(root / 'origin.jsonl')
    rows = []
    for directory in sorted(x for x in root.iterdir() if x.is_dir()):
        events = read_events(directory / 'client.jsonl')
        if not events:
            continue
        start = next((x for x in events if x['event'] == 'case_start'), {})
        down = [x for x in events if x['event'] == 'down_frame' and x.get('id') == directory.name]
        end = next((x for x in events if x['event'] == 'down_end'), {})
        origin_case = [x for x in origin if x.get('id') == directory.name]
        source_start = next((x for x in origin_case if x['event'] == 'origin_app_start'), None)
        origin_reads = [x for x in origin_case if x['event'] == 'origin_up_read']
        traces = [analyze_trace(x, root) for x in sorted(directory.rglob('*.sqlog'))]
        remote = [r for t in traces for s in t['streams'] for r in s['remote_reset']]
        row = {'case': directory.name, 'parameters': start, 'valid_origin_request': source_start is not None,
               'valid_controlled_downlink': bool(down) or start.get('down_interval_ms') == 0,
               'frames': end.get('frames'), 'first_down_ms': down[0]['t_ms'] if down else None,
               'last_down_ms': down[-1]['t_ms'] if down else None, 'down_end': end,
               'case_end': next((x for x in events if x['event'] == 'case_end'), {}),
               'schedule_anchor': next((x for x in events if x['event'] == 'upload_schedule_anchor' and x.get('id') == directory.name), None),
               'application_uploads': [x for x in events if x['event'] == 'up_write' and x.get('id') == directory.name],
               'origin_up_times_ms': [x['t_ms'] - source_start['t_ms'] for x in origin_reads] if source_start else [],
               'origin_up_gaps_seconds': [(b['t_ms'] - a['t_ms']) / 1000 for a, b in zip(origin_reads, origin_reads[1:])],
               'origin_terminal': [x for x in origin_case if x['event'] in ('origin_up_end', 'origin_app_end')],
               'guard': next((x for x in events if x['event'] == 'guard_after_main_end'), None),
               'reuse': [x for x in events if x['event'] in ('carrier_reuse', 'reuse_result')],
               'remote_resets': remote, 'traces': traces}
        rows.append(row)
        primary = next((x for x in remote if x['stream_id'] == 0), None)
        gap = f"{primary['after_last_new_tx_seconds']:.3f}" if primary else '-'
        print(f"{directory.name:38} frames={str(row['frames']):>4} reset_after_new_up={gap:>7} src={row['valid_origin_request']}")
    (root / 'analysis.json').write_text(json.dumps(rows, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    valid = [r for r in rows if r['valid_origin_request'] and r['valid_controlled_downlink']]
    traces = [t for r in rows for t in r['traces']]
    resets = [x for r in valid for x in r['remote_resets'] if x['stream_id'] == 0]
    active_resets = [x for r in valid if r['parameters'].get('down_interval_ms', 0) > 0
                     for x in r['remote_resets'] if x['stream_id'] == 0]
    summary = {'cases': len(rows), 'valid_cases': len(valid),
               'invalid_cases': [r['case'] for r in rows if r not in valid],
               'captured_traces': len(traces),
               'all_traces_closed': all(t['connection_closed'] for t in traces),
               'peer_connection_close_frames': sum(len(t['peer_connection_close_frames']) for t in traces),
               'valid_primary_peer_resets': len(resets),
               'reset_codes': sorted({x['error_code'] for x in resets}),
               'reset_after_last_new_up_seconds': {'min': min(x['after_last_new_tx_seconds'] for x in resets),
                                                   'max': max(x['after_last_new_tx_seconds'] for x in resets)} if resets else None,
               'continuous_downlink_resets': len(active_resets),
               'reset_after_last_down_seconds': {'min': min(x['after_last_rx_seconds'] for x in active_resets),
                                                 'max': max(x['after_last_rx_seconds'] for x in active_resets)} if active_resets else None}
    (root / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    lines = ['case\tvalid\tframes\treset_after_new_up_seconds\treset_after_last_down_seconds']
    for row in rows:
        reset = next((x for x in row['remote_resets'] if x['stream_id'] == 0), None)
        gap = f"{reset['after_last_new_tx_seconds']:.6f}" if reset else '-'
        last_down = f"{reset['after_last_rx_seconds']:.6f}" if reset and reset['after_last_rx_seconds'] is not None else '-'
        lines.append(f"{row['case']}\t{row in valid}\t{row['frames']}\t{gap}\t{last_down}")
    (root / 'case-summary.txt').write_text('\n'.join(lines) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
