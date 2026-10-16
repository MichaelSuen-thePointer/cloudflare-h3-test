"""Analyze archived TCP-plugin QUIC qlogs without network access.

Usage: python scripts/analyze_tcp_plugin_qlog.py DATASET_DIRECTORY
The directory contains timeline.jsonl and qlog/**/*.sqlog[.gz].
"""
import argparse, gzip, json, pathlib, collections, datetime, statistics, bisect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('directory', type=pathlib.Path)
ROOT = parser.parse_args().directory.resolve()
timeline = [json.loads(x) for x in (ROOT/'timeline.jsonl').read_text().splitlines()]
trials = [x for x in timeline if x['name'] in ('tcp-h3-qlog','baseline','window16')]

def quantiles(values):
    if not values: return None
    a=sorted(values)
    return {'min':a[0], 'median':statistics.median(a), 'p95':a[int((len(a)-1)*.95)], 'max':a[-1]}

def stream_state():
    return dict(rx=[],tx=[],credit=[],blocked=[],terminal=[],ranges=[],frontier=0,overlap=0,
                disorder=0,gaps=[],last_progress=None,headroom=[],max_seen=0)

def receive(s,t,offset,length,pn,initial_window):
    end=offset+length
    if not length:return
    s['rx'].append((t,offset,length,pn))
    ranges=s['ranges']; old=s['frontier']
    if offset>old:s['disorder']+=1
    i=bisect.bisect_left(ranges,(offset,-1))
    if i and ranges[i-1][1]>=offset:i-=1
    start=offset;finish=end;j=i
    while j<len(ranges) and ranges[j][0]<=finish:
        a,b=ranges[j]; s['overlap']+=max(0,min(end,b)-max(offset,a))
        start=min(start,a);finish=max(finish,b);j+=1
    ranges[i:j]=[(start,finish)]
    new=ranges[0][1] if ranges and ranges[0][0]==0 else 0
    s['max_seen']=max(s['max_seen'],end)
    if new>old:
        if s['last_progress'] is not None:
            s['gaps'].append((t-s['last_progress'],s['last_progress'],t,old,new))
        s['last_progress']=t;s['frontier']=new
    credit=s['credit'][-1][1] if s['credit'] else initial_window
    s['headroom'].append((t,credit-s['max_seen']))

summaries=[]; details=[]
paths = sorted(p for p in (ROOT/'qlog').rglob('*') if p.name.endswith(('.sqlog','.sqlog.gz')))
for path in paths:
    streams=collections.defaultdict(stream_state);counts=collections.Counter();frames=collections.Counter()
    metrics=[];losses=[];ptos=[];ends=[];params=[];pns=[];rxpackets=[];parse_errors=0;ref=None;initial_window=6291456
    opener = gzip.open if path.suffix == '.gz' else open
    raw_bytes = 0
    with opener(path, 'rt', encoding='utf-8', newline='') as f:
        for line in f:
            raw_bytes += len(line.encode('utf-8'))
            try:e=json.loads(line.lstrip('\x1e'))
            except (ValueError,UnicodeError):parse_errors+=1;continue
            if 'trace' in e:
                h=e;wall=e['trace']['common_fields']['reference_time']['wall_clock_time']
                ref=datetime.datetime.fromisoformat(wall).timestamp()*1000;continue
            if ref is None:continue
            t=ref+e.get('time',0);name=e.get('name','');d=e.get('data',{});counts[name]+=1
            if name=='recovery:metrics_updated':metrics.append((t,d))
            elif name=='recovery:packet_lost':losses.append((t,d))
            elif name=='recovery:pto_count_updated':ptos.append((t,d))
            elif name=='transport:connection_closed':ends.append((t,d))
            elif name=='transport:parameters_set':
                params.append(d)
                if d.get('initiator')=='local':initial_window=d.get('initial_max_stream_data_bidi_local',initial_window)
            if name not in ('transport:packet_sent','transport:packet_received'):continue
            direction='rx' if name.endswith('received') else 'tx'
            pn=d.get('header',{}).get('packet_number');ptype=d.get('header',{}).get('packet_type')
            if direction=='rx':
                rxpackets.append((t,pn,ptype,d.get('raw',{}).get('length',0)))
                if ptype in ('1RTT','1rtt','1_rtt') and pn is not None:pns.append(pn)
            for fr in d.get('frames',[]):
                ft=fr.get('frame_type','');frames[direction+':'+ft]+=1
                sid=fr.get('stream_id');s=streams[sid] if sid is not None else None
                if ft=='stream' and s is not None:
                    if direction=='rx':receive(s,t,fr.get('offset',0),fr.get('length',0),pn,initial_window)
                    elif fr.get('length',0):s['tx'].append((t,fr.get('offset',0),fr.get('length',0)))
                    if fr.get('fin'):s['terminal'].append((t,direction,fr))
                elif ft=='max_stream_data' and direction=='tx' and s is not None:
                    s['credit'].append((t,fr.get('maximum',fr.get('max_stream_data',0))))
                elif ft in ('data_blocked','stream_data_blocked'):
                    if s is not None:s['blocked'].append((t,direction,fr))
                elif ft in ('reset_stream','stop_sending') and s is not None:
                    s['terminal'].append((t,direction,fr))
                elif ft=='connection_close':ends.append((t,direction,fr))
    relative=str(path.relative_to(ROOT/'qlog')).removesuffix('.gz')
    summary={'file':relative,'bytes':raw_bytes,'parse_errors':parse_errors,'events':dict(counts),
             'frames':dict(frames),'parameters':params,'connection_end':ends,'local_packets_lost':len(losses),
             'rtt_ms':quantiles([d['smoothed_rtt'] for t,d in metrics if 'smoothed_rtt' in d])}
    if pns:
        unique=set(pns);summary['received_1rtt_pn']={'count':len(pns),'unique':len(unique),'span':max(unique)-min(unique)+1,'missing_numbers':max(unique)-min(unique)+1-len(unique)}
    for sid,s in streams.items():
        total=sum(x[2] for x in s['rx'])
        if sid%4 or total<500000:continue
        candidates=[]
        for trial in trials:
            within=sum(x[2] for x in s['rx'] if trial['start_unix_ms']<=x[0]<=trial['end_unix_ms'])
            candidates.append((within,trial))
        matched=max(candidates,key=lambda x:x[0]) if candidates else (0,None)
        trial=matched[1];start=trial['start_unix_ms'] if trial else s['rx'][0][0];stop=trial['end_unix_ms'] if trial else s['rx'][-1][0]
        gaps=[x for x in s['gaps'] if start<=x[1] and x[2]<=stop]
        buckets=collections.defaultdict(int)
        for t,off,n,pn in s['rx']:
            if start<=t<=stop:buckets[int((t-start)//1000)]+=n
        local_metrics=[d['smoothed_rtt'] for t,d in metrics if start<=t<=stop and 'smoothed_rtt' in d]
        head=[x[1] for x in s['headroom'] if start<=x[0]<=stop]
        terms=[]
        for t,direction,fr in s['terminal']:
            tx_before=[x for x in s['tx'] if x[0]<=t]
            rx_before=[x for x in s['rx'] if x[0]<=t]
            terms.append(dict(unix_ms=t,relative_seconds=(t-start)/1000,direction=direction,frame=fr,
                              after_last_tx_seconds=(t-tx_before[-1][0])/1000 if tx_before else None,
                              after_last_rx_seconds=(t-rx_before[-1][0])/1000 if rx_before else None))
        details.append({'file':relative,'stream_id':sid,'trial':trial,'matched_rx_bytes':matched[0],
                        'received_stream_frame_bytes':total,'unique_end':s['frontier'],'max_offset':s['max_seen'],
                        'duplicate_overlap_bytes':s['overlap'],'out_of_order_frames':s['disorder'],
                        'credit_sent':s['credit'],'rx_credit_headroom_bytes':quantiles(head),
                        'largest_inorder_progress_gaps':[{'seconds':x[0]/1000,'start_relative_seconds':(x[1]-start)/1000,'end_relative_seconds':(x[2]-start)/1000,'offset_before':x[3],'offset_after':x[4]} for x in sorted(gaps,reverse=True)[:12]],
                        'rtt_ms':quantiles(local_metrics),'blocked':s['blocked'],'terminal':terms,
                        'rx_stream_bytes_per_second':dict(sorted(buckets.items())),
                        'local_packet_loss_during_trial':sum(start<=t<=stop for t,d in losses),
                        'pto_events_during_trial':[(t,d) for t,d in ptos if start<=t<=stop]})
    summaries.append(summary)
out={'connections':summaries,'streams':details,'limitations':['Receiver vantage: local congestion window and loss events concern client uploads, not server download congestion.','Missing packet numbers are an observation, not a direct remote loss counter.','Flow-control headroom based on stream offset and sent credit; check emitted field names before interpretation.','Forced stop can truncate active qlogs; final run uses Ctrl+C and reports parse errors.']}
(ROOT/'qlog-analysis.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'connections':len(summaries),'parse_errors':sum(x['parse_errors'] for x in summaries),'business_streams':len(details)},indent=2))
