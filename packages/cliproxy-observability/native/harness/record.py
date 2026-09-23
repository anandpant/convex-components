"""Three approved real protocol calls via existing dev gateway; never copy OAuth state.

Run only explicitly, with /run/dev-key.json mounted read-only and a private output
mount at /recordings. This recorder is never part of CI.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
import proof

root=proof.ROOT
root.mkdir(mode=0o700);(root/'auth').mkdir();(root/'outbox').mkdir(mode=0o700)
private=json.loads(Path('/run/dev-key.json').read_text())
key=private['apiKey'];base=private['baseURL'].rstrip('/')
# Keep the normal stock OpenAI-compatible executor; only its upstream URL/key are configured.
# Actual OAuth execution remains owned by the existing gateway, with unknown selected provider.
stock=proof.config(True).replace('http://127.0.0.1:8316/v1',base).replace('harmless-upstream-test-key',key).replace('proof-model','gpt-5.6-luna')
# Disable request logging; these stock stdout logs contain only startup and request status.
(root/'stock.yaml').write_text(stock);(root/'stock.yaml').chmod(0o600)
scope={'enabled':True,'instanceId':'isolated-real-recording','revision':'proof-v1','socket':str(root/'outbox/capture.sock'),'bindings':[{'key':proof.DEV,'destinationId':'harness-dev','deployment':proof.DEPLOYMENT,'environment':'dev'}],'redactions':[key]}
(root/'capture.json').write_text(json.dumps(scope));(root/'capture.json').chmod(0o600)
(root/'nginx.conf').write_text(proof.nginx_config())
processes=[]
try:
    exporter=subprocess.Popen(['/capture/cliproxy-capture-exporter','--socket',str(root/'outbox/capture.sock'),'--db',str(root/'outbox/events.db'),'--reserve-bytes','1048576'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);processes.append(exporter)
    nginx=subprocess.Popen(['nginx','-c',str(root/'nginx.conf'),'-g','daemon off;'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);processes.append(nginx)
    server=subprocess.Popen(['/stock/cli-proxy-api','-config',str(root/'stock.yaml')],env={**os.environ,'CLIPROXY_CAPTURE_CONFIG':str(root/'capture.json')},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);processes.append(server)
    proof.wait_port(8317);proof.wait_port(8318)
    records=[]
    import http.client
    for i,(protocol,route,stream) in enumerate([('messages-sse','/v1/messages',True),('responses-sse','/v1/responses',True),('chat-json','/v1/chat/completions',False)]):
        prompt='Reply with exactly CAPTURE_OK.'
        data={'model':'gpt-5.6-luna','stream':stream,'messages':[{'role':'user','content':prompt}],'max_tokens':512}
        if route=='/v1/responses':data={'model':'gpt-5.6-luna','stream':True,'input':prompt,'max_output_tokens':512}
        conn=http.client.HTTPConnection('127.0.0.1',8318,timeout=120)
        started=time.perf_counter();conn.request('POST',route,json.dumps(data),{'Authorization':'Bearer '+proof.DEV,'X-Meshix-Deployment':proof.DEPLOYMENT,'Content-Type':'application/json'})
        res=conn.getresponse();status=res.status
        # Downstream bytes are never written to disk; only the plugin's sanitized outbox is exported.
        body=res.read();elapsed=(time.perf_counter()-started)*1000;conn.close()
        if status!=200:
            category='unclassified'
            for label,needles in [('quota_or_rate_limit',[b'rate limit',b'quota',b'usage limit']),('authentication',[b'auth',b'token',b'credential']),('connection',[b'connection',b'dial tcp',b'timeout',b'lookup']),('model_routing',[b'unknown model',b'no available'])]:
                if any(needle in body.lower() for needle in needles):category=label;break
            raise RuntimeError(f'{protocol}: HTTP {status}; category={category}; elapsedMs={elapsed:.0f}; body withheld')
        captured=proof.wait_events(i+1)
        groups={}
        for event in captured:groups.setdefault(event['requestId'],[]).append(event)
        call=next(events for events in groups.values() if events[0]['route']=='POST '+route)
        if not any(e['kind']=='completion' and e.get('completionOutcome')=='succeeded' for e in call):raise RuntimeError('no successful stock completion')
        joined=b''.join(base64.b64decode(e.get('body','')) for e in call if e['kind'] in ['response','stream_chunk'])
        if not joined or any(e.get('gap') for e in call):raise RuntimeError(f'{protocol}: captured output missing or incomplete')
        for event in call:
            payload=base64.b64decode(event.get('body',''))
            if key.encode() in payload or proof.DEV.encode() in payload or re.search(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',payload):raise RuntimeError('fixture secret/account scan failed')
        fixture={'provenance':{'kind':'real_provider_recording','recordedAt':call[0]['observedAt'],'sourceProtocol':protocol,'requestedModel':'gpt-5.6-luna','selectedUpstreamProvider':'unknown','topology':'isolated official stock 7.3.5 -> existing dev gateway -> existing OAuth owner; no copied OAuth state','stockSHA256':hashlib.sha256(Path('/stock/cli-proxy-api').read_bytes()).hexdigest(),'pluginSHA256':hashlib.sha256(Path('/capture/cliproxy-capture.so').read_bytes()).hexdigest(),'pluginVersion':'0.1.0','redactionVersion':'framed-json-v1','durationMs':elapsed,'downstreamStatus':status,'eventsSHA256':hashlib.sha256(json.dumps(call,separators=(',',':')).encode()).hexdigest()},'events':call}
        output=Path('/recordings')/(protocol+'.json');output.write_text(json.dumps(fixture,indent=2)+'\n')
        records.append({'protocol':protocol,'observations':len(call),'durationMs':elapsed,'fixtureSHA256':hashlib.sha256(output.read_bytes()).hexdigest()})
    print(json.dumps({'realRecordings':records},indent=2))
finally:
    for process in reversed(processes):
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill()
    # Remove ephemeral credential-bearing config; OAuth state was never present.
    (root/'stock.yaml').unlink(missing_ok=True);(root/'capture.json').unlink(missing_ok=True)
