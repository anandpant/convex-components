"""Isolated stock binary proof. All provider payloads here are SIMULATED, never real recordings."""
import concurrent.futures
import base64
from contextlib import closing
from collections import Counter
import hashlib
import http.client
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
import platform
import pwd
import grp
import socket
import sqlite3
import statistics
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path('/tmp/capture-proof')
DEV = 'harness-Dev-key-0123456789'
PROD = 'harness-Prod-key-0123456789'
PERSONAL = 'harness-Personal-key-0123456789'
PREVIEW = 'harness-Preview-key-0123456789'
DEPLOYMENT = 'https://harness-dev.convex.site'
provider_headers = []
content_requests = {}
TOOL_ARGUMENTS = '{"text":"é token secret https://example.invalid/?x=1&y=2","nested":{"value":"keep"}}'
class Provider(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_): pass
    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        data = json.loads(raw)
        tool_proof = 'CAPTURE_TOOL' in json.dumps(data)
        provider_headers.append(dict(self.headers))
        if 'FAIL_CAPTURE' in json.dumps(data):
            body=b'{"error":{"message":"simulated post-hook failure","type":"server_error"}}'
            self.send_response(503);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
        if data.get('stream'):
            body = b''
            items = [
                {'choices':[{'index':0,'delta':{'role':'assistant','content':'CAPTURE_'},'finish_reason':None}]},
                {'choices':[{'index':0,'delta':{'content':'OK'},'finish_reason':None}]},
            ]
            if tool_proof: items += [
                {'choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'id':'call-proof','type':'function','function':{'name':'proof_tool','arguments':TOOL_ARGUMENTS[:30]}}]},'finish_reason':None}]},
                {'choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'function':{'arguments':TOOL_ARGUMENTS[30:]}}]},'finish_reason':None}]},
            ]
            items += [{'choices':[{'index':0,'delta':{},'finish_reason':'stop'}], 'usage':{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10}}]
            for item in items:
                item.update(id='chatcmpl-harness', object='chat.completion.chunk', created=1700000000, model='proof-model')
                body += b'data: ' + json.dumps(item).encode() + b'\n\n'
            body += b'data: [DONE]\n\n'
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(body)));self.end_headers()
            for part in body.splitlines(keepends=True): self.wfile.write(part);self.wfile.flush()
        else:
            message={'role':'assistant','content':'CAPTURE_OK'}
            if tool_proof: message['tool_calls']=[{'id':'call-proof','type':'function','function':{'name':'proof_tool','arguments':TOOL_ARGUMENTS}}]
            body = json.dumps({'id':'chatcmpl-harness','object':'chat.completion','created':1700000000,'model':'proof-model','choices':[{'index':0,'message':message,'finish_reason':'stop'}], 'usage':{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10}}).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)


def wait_port(port):
    for _ in range(150):
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1): return
        except OSError: time.sleep(.1)
    raise RuntimeError(f'port {port} did not open')


def request(path='/v1/chat/completions', *, port=8318, key=DEV, headers=None, stream=False, payload_size=32, failure=False, proof_id=None):
    h={'Authorization':'Bearer '+key,'X-Meshix-Deployment':DEPLOYMENT,'Content-Type':'application/json'}
    if headers: h.update(headers)
    h={k:v for k,v in h.items() if v is not None}
    data={'model':'proof-model','stream':stream,'messages':[{'role':'user','content':'Say CAPTURE_OK. '+('x'*payload_size)}],'max_tokens':32}
    if path=='/v1/responses': data={'model':'proof-model','stream':stream,'input':'Say CAPTURE_OK. '+('x'*payload_size)}
    if failure: data['messages']=[{'role':'user','content':'FAIL_CAPTURE'}]
    if proof_id:
        h['X-Meshix-Request-Id'] = proof_id
        if not failure:
            if path=='/v1/responses': data['input']+=' CAPTURE_TOOL'
            else: data['messages'][0]['content']+=' CAPTURE_TOOL'
    raw = json.dumps(data).encode()
    if proof_id:
        content_requests[proof_id] = raw
    c=http.client.HTTPConnection('127.0.0.1',port,timeout=30)
    start=time.perf_counter_ns();c.request('GET' if path=='/v1/models' else 'POST',path,raw,h);r=c.getresponse();first=r.read(1);ttft=(time.perf_counter_ns()-start)/1e6;body=first+r.read();latency=(time.perf_counter_ns()-start)/1e6;status=r.status;c.close()
    return status,body,ttft,latency


def duplicate_request():
    c=http.client.HTTPConnection('127.0.0.1',8318,timeout=10)
    body=json.dumps({'model':'proof-model','messages':[{'role':'user','content':'harmless duplicate-header test'}]})
    c.putrequest('POST','/v1/chat/completions')
    for k,v in [('Authorization','Bearer '+DEV),('X-Api-Key',DEV),('x-api-key',DEV),('X-Meshix-Deployment',DEPLOYMENT),('Content-Type','application/json'),('Content-Length',str(len(body)))]:c.putheader(k,v)
    c.endheaders(body.encode());r=c.getresponse();r.read();c.close()


def config(enabled):
    return f'''host: "127.0.0.1"
port: 8317
auth-dir: "{ROOT}/auth"
api-keys: [{DEV}, {PROD}, {PERSONAL}, {PREVIEW}]
request-log: false
error-logs-max-files: -1
usage-statistics-enabled: false
remote-management:
  disable-control-panel: true
plugins:
  enabled: {str(enabled).lower()}
  dir: /capture
  configs:
    cliproxy-capture:
      enabled: true
      priority: 1
openai-compatibility:
  - name: playback
    base-url: http://127.0.0.1:8316/v1
    api-key-entries:
      - api-key: harmless-upstream-test-key
    headers:
      X-Meshix-Capture-Destination: "$X-Meshix-Capture-Destination"
      X-Meshix-Capture-Route: "$X-Meshix-Capture-Route"
      X-Meshix-Capture-Revision: "$X-Meshix-Capture-Revision"
    models:
      - name: proof-model
'''


def nginx_config():
    # Regex matching is case-sensitive. Combined identities prevent bearer/API-key disagreement.
    maps=['map "$http_authorization|$http_x_api_key|$http_x_goog_api_key|$args" $capture_destination {','default "";']
    for key,dest in [(DEV,'harness-dev'),(PROD,'harness-prod')]:
        for value in [f'Bearer {key}\\|\\|\\|',f'\\|{key}\\|\\|',f'Bearer {key}\\|{key}\\|\\|']:
            maps.append(f'"~^{value}$" {dest};')
    maps.append('}')
    proxy='''proxy_http_version 1.1; proxy_buffering off; proxy_request_buffering off;
      proxy_set_header Connection "";
      proxy_set_header X-Meshix-Capture-Destination $capture_destination;
      proxy_set_header X-Meshix-Capture-Route "$request_method $uri";
      proxy_set_header X-Meshix-Capture-Revision proof-v1;
      proxy_pass http://127.0.0.1:8317;'''
    locations=''
    for route in ['/v1/messages','/v1/messages/count_tokens','/v1/responses','/v1/chat/completions','/v1/models']:
        method='GET' if route.endswith('models') else 'POST'
        locations+=f'location = {route} {{ limit_except {method} {{ deny all; }} {proxy} }}\n'
    # Compiled absolute defaults ignore -p and can chown a live nginx's directories.
    runtime = ROOT / 'nginx'
    user = pwd.getpwuid(os.geteuid()).pw_name
    group = grp.getgrgid(os.getegid()).gr_name
    worker = f'user {user} {group};\n' if os.geteuid() == 0 else ''
    return worker + f'''worker_processes 1; pid {runtime}/nginx.pid;
lock_file {runtime}/nginx.lock;
error_log {runtime}/error.log warn;
events {{ worker_connections 1024; }}
http {{ access_log off; client_max_body_size 2m;
client_body_temp_path {runtime}/client_body;
proxy_temp_path {runtime}/proxy;
fastcgi_temp_path {runtime}/fastcgi;
uwsgi_temp_path {runtime}/uwsgi;
scgi_temp_path {runtime}/scgi;
'''+'\n'.join(maps)+f'''
server {{ listen 127.0.0.1:8318; {locations} location / {{ return 404; }} }}
server {{ listen 127.0.0.1:8319; location / {{
 proxy_http_version 1.1; proxy_buffering off; proxy_set_header Upgrade $http_upgrade;
 proxy_set_header Connection "upgrade";
 proxy_set_header X-Meshix-Capture-Destination "";
 proxy_set_header X-Meshix-Capture-Route "";
 proxy_set_header X-Meshix-Capture-Revision "";
 proxy_pass http://127.0.0.1:8317;
}} }} }}'''


def prepare_nginx_runtime():
    runtime = ROOT / 'nginx'
    runtime.mkdir(mode=0o700)
    for name in ['client_body', 'proxy', 'fastcgi', 'uwsgi', 'scgi']:
        (runtime / name).mkdir(mode=0o700)


def nginx_command(*args):
    runtime = ROOT / 'nginx'
    # -e also isolates early errors before nginx has parsed error_log.
    return ['nginx', '-p', str(runtime) + '/', '-e', str(runtime / 'error.log'),
            '-c', str(ROOT / 'nginx.conf'), *args]


def events():
    with closing(sqlite3.connect(ROOT/'outbox/events.db')) as db: return [json.loads(r[0]) for r in db.execute('select payload from events order by destination,instance,boot,request_id,sequence')]


def stop_proc(process):
    assert process.poll() is None, 'process exited before controlled shutdown'
    started = time.time_ns()
    process.terminate()
    code = process.wait(timeout=10)
    assert code == 0, f'graceful shutdown failed: exit {code}'
    return {'pid': process.pid, 'startedAtNs': started,
            'completedAtNs': time.time_ns(), 'returnCode': code}


def timestamp_ns(value):
    # Go publishes UTC RFC3339Nano; retain all nine fractional digits.
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z', value)
    assert match, 'invalid health publication timestamp'
    seconds = int(datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc).timestamp())
    return seconds*1_000_000_000 + int((match[2] or '').ljust(9, '0'))


def verify_ledger(found, boot, completions, instance):
    assert found, 'durable ledger missing'
    calls = {}
    content_bytes = 0
    for event in found:
        assert event['pluginBootId'] == boot and event['instanceId'] == instance, 'unexpected capture boot/instance'
        assert event['destinationId'] == 'harness-dev' and event['requestId'], 'unexpected capture scope'
        assert not (event.get('gap') or event.get('captureIncomplete') or event.get('lostContentBytes', 0)), 'captured protocol content has gaps'
        body = base64.b64decode(event.get('body', ''), validate=True)
        error = event.get('error', '').encode()
        assert len(body) == event['contentBytes'] == event.get('observedBodyBytes', 0), 'body byte accounting differs'
        assert hashlib.sha256(body).hexdigest() == event['contentSha256'], 'body digest differs'
        assert len(error) == event.get('observedErrorBytes', 0), 'error byte accounting differs'
        assert DEV.encode() not in body, 'routing credential reached content'
        content_bytes += len(body) + len(error)
        calls.setdefault(event['requestId'], []).append(event)
    assert sum(e['kind'] == 'completion' for e in found) == completions, 'completion count differs'
    for call in calls.values():
        call.sort(key=lambda e: e['sequence'])
        assert [e['sequence'] for e in call] == list(range(1, len(call)+1)), 'sequence gap'
        assert call[-1]['kind'] == 'completion' and sum(e['kind'] == 'completion' for e in call) == 1, 'call terminal missing or misplaced'
    return content_bytes


def final_health(boot, completions, tail, shutdown, config):
    assert shutdown['returnCode'] == 0, 'graceful shutdown failed'
    found = events()  # Re-read the complete ledger, including outage/error tail.
    content_bytes = verify_ledger(found, boot, completions, config['instanceId'])
    assert tail in found, 'tail completion missing or changed'
    assert max(timestamp_ns(e['observedAt']) for e in found) <= shutdown['startedAtNs'], 'shutdown boundary predates captured tail'
    with closing(sqlite3.connect(ROOT/'outbox/events.db')) as db:
        rows = [json.loads(row[0]) for row in db.execute('SELECT payload FROM health')]
    health = [h for h in rows if h['pluginBootId'] == boot and h['instanceId'] == config['instanceId']]
    destinations = {b['destinationId'] for b in config['bindings']}
    assert len(health) == len(destinations) and {h['destinationId'] for h in health} == destinations, 'current-boot health missing'
    for h in health:
        assert shutdown['startedAtNs'] <= timestamp_ns(h['observedAt']) <= shutdown['completedAtNs'], 'final health publication missing or stale'
        assert h['observationsTotal'] == len(found), 'final health does not cover complete ledger'
        assert h['activeCalls'] == 0 and h['droppedObservationsTotal'] == 0 and h['lostControlObservationsTotal'] == 0, 'capture record loss'
        content = h['content']
        assert content_bytes > 0 and content['observedBytes'] == content['committedBytes'] == content_bytes, 'final health byte accounting differs'
        assert all(content[k] == 0 for k in ['lostBytes', 'unconfirmedBytes', 'unknownLosses', 'missedCalls', 'incompleteCalls', 'capacityRejections', 'retainedChargedBytes']), 'capture content loss or uncertainty'
        assert h['precommitCoverage'] == 'unknown_before_local_commit', 'process-loss boundary differs'
    return health, {'pluginBootId': boot, 'observations': len(found),
                    'contentBytes': content_bytes, 'tailRequestId': tail['requestId'],
                    'tailSequence': tail['sequence'], 'tailObservedAt': tail['observedAt'],
                    'shutdown': shutdown}

def wait_events(expected):
    for _ in range(100):
        found=events()
        if sum(e['kind']=='completion' for e in found)>=expected:return found
        time.sleep(.05)
    # Preserve bounded evidence on a failing CI run; --rm otherwise removes it.
    # This private fixture contains only simulated provider content.
    with closing(sqlite3.connect(ROOT/'outbox/events.db')) as db:
        health = [json.loads(row[0]) for row in db.execute('SELECT payload FROM health')]
    diagnostics = {
        'expectedCompletions': expected,
        'observationsByKind': dict(Counter(e['kind'] for e in found)),
        'gaps': dict(Counter(e['gap'] for e in found if e.get('gap'))),
        'health': health,
    }
    print('capture wait failure: '+json.dumps(diagnostics), flush=True)
    raise AssertionError(f'expected {expected} completed captures; got {len(found)} observations')

def peak_rss(pids, stop, samples):
    while not stop.is_set():
        for name,pid in pids.items():
            try:
                fields=Path(f'/proc/{pid}/status').read_text().splitlines()
                rss=int(next(x.split()[1] for x in fields if x.startswith('VmRSS:')))
                hwm=int(next(x.split()[1] for x in fields if x.startswith('VmHWM:')))
                samples[name]=max(samples.get(name,0),rss,hwm)
            except (OSError,StopIteration):pass
        stop.wait(.01)

def percentiles(values):
    v=sorted(values)
    return {p:v[min(len(v)-1,round((len(v)-1)*q))] for p,q in [('p50',.5),('p95',.95),('p99',.99)]}

def main():
    protocol_only=os.environ.get('CLIPROXY_HARNESS_PROTOCOL_ONLY')=='1'
    ROOT.mkdir(mode=0o700);(ROOT/'auth').mkdir();(ROOT/'outbox').mkdir(mode=0o700)
    prepare_nginx_runtime()
    capture_config={'enabled':True,'instanceId':'isolated-stock-proof','revision':'proof-v1','socket':str(ROOT/'outbox/capture.sock'),'bindings':[{'key':DEV,'destinationId':'harness-dev','deployment':DEPLOYMENT,'environment':'dev'},{'key':PROD,'destinationId':'harness-prod','deployment':'https://harness-prod.convex.site','environment':'prod'}]}
    (ROOT/'capture.json').write_text(json.dumps(capture_config));(ROOT/'capture.json').chmod(0o600)
    (ROOT/'nginx.conf').write_text(nginx_config())
    provider=ThreadingHTTPServer(('127.0.0.1',8316),Provider);threading.Thread(target=provider.serve_forever,daemon=True).start()
    procs=[];report={'fixtureProvenance':'simulated_provider_playback','stockVersion':os.environ.get('CLIPROXY_STOCK_VERSION','7.3.5'),'stockSHA256':hashlib.sha256(Path('/stock/cli-proxy-api').read_bytes()).hexdigest(),'architecture':platform.machine(),'load':{'concurrency':8,'calls':160,'requestContentBytes':65536}}
    report['qualificationMode']='protocol_only' if protocol_only else 'protocol_and_load'
    if protocol_only: report['load']=None
    def start_exporter():
        p=subprocess.Popen(['/capture/cliproxy-capture-exporter','--socket',str(ROOT/'outbox/capture.sock'),'--db',str(ROOT/'outbox/events.db'),'--reserve-bytes','1048576'],stdout=subprocess.DEVNULL,stderr=open(ROOT/'exporter.log','ab'));procs.append(p)
        for _ in range(100):
            if (ROOT/'outbox/capture.sock').exists():break
            time.sleep(.05)
        return p
    def start_stock(enabled):
        (ROOT/'stock.yaml').write_text(config(enabled))
        p=subprocess.Popen(['/stock/cli-proxy-api','-config',str(ROOT/'stock.yaml')],env={**os.environ,'CLIPROXY_CAPTURE_CONFIG':str(ROOT/'capture.json')},stdout=open(ROOT/f'stock-{enabled}.log','wb'),stderr=subprocess.STDOUT);procs.append(p);wait_port(8317);return p
    try:
        exporter=start_exporter()
        subprocess.run(nginx_command('-t'), check=True)
        nginx=subprocess.Popen(nginx_command('-g','daemon off;'));procs.append(nginx);wait_port(8318)
        stock=start_stock(False)
        for enabled in [False,True]:
            if enabled:
                report["baselineAuthorityPositiveControl"]=any(any(k.lower().startswith("x-meshix-capture-") for k in h) for h in provider_headers)
                provider_headers.clear()
                stop_proc(stock);stock=start_stock(True)
            for _ in range(5): assert request()[0]==200
            if protocol_only: continue
            stop=threading.Event();samples={};monitor=threading.Thread(target=peak_rss,args=({'stock':stock.pid,'exporter':exporter.pid},stop,samples));monitor.start()
            start=time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool: results=list(pool.map(lambda _:request(stream=True,payload_size=65536),range(160)))
            elapsed=time.monotonic()-start;stop.set();monitor.join()
            assert all(r[0]==200 and b'CAPTURE_' in r[1] for r in results)
            report['enabled' if enabled else 'baseline']={'ttfbMs':percentiles([r[2] for r in results]),'latencyMs':percentiles([r[3] for r in results]),'callsPerSecond':len(results)/elapsed,'peakRssKiB':samples}
        base=5 if protocol_only else 165
        initial=wait_events(base)
        boot=initial[0]['pluginBootId']
        assert {e['pluginBootId'] for e in initial}=={boot}, 'capture restarted during fixture'
        assert sum(e['kind']=='completion' for e in initial)==base, 'initial completion count differs'
        captures={}
        content_proof={}
        for route,stream in [(route,stream) for route in ['/v1/messages','/v1/responses','/v1/chat/completions'] for stream in [False,True]]:
            proof_id='protocol-'+str(len(captures))
            status,body,_,_=request(route,stream=stream,proof_id=proof_id);assert status==200,(route,status,body[:200]);captures[route+('#sse' if stream else '#json')]={'status':status,'downstreamSHA256':hashlib.sha256(body).hexdigest()}
            (ROOT/f'{proof_id}-request.bin').write_bytes(content_requests[proof_id])
            (ROOT/f'{proof_id}-response.bin').write_bytes(body)
            content_proof[proof_id]=(stream,body)
        found=wait_events(base+6);before=len(found)
        report['contentProof']={}
        for proof_id,(stream,downstream) in content_proof.items():
            call=[e for e in found if e.get('correlation',{}).get('requestId')==proof_id]
            assert call and not any(e.get('gap') or e.get('captureIncomplete') or e.get('lostContentBytes',0) for e in call), f'{proof_id}: incomplete capture'
            def content(kind): return b''.join(base64.b64decode(e.get('body','')) for e in call if e['kind']==kind)
            assert content('request')==content_requests[proof_id], f'{proof_id}: request bytes differ'
            # These stock hooks retain the source payload before executor translation.
            assert content('request_after_auth')==content_requests[proof_id], f'{proof_id}: after-auth bytes differ'
            output=content('stream_chunk' if stream else 'response')
            route=call[0]['route']
            if stream:
                chunks=[]
                for e in call:
                    if e['kind']!='stream_chunk': continue
                    raw=base64.b64decode(e['body']);lengths=e['stockHookChunkLengths']
                    assert e['bodyFraming']=='stock_hook_block' and sum(lengths)==len(raw), 'invalid callback boundaries'
                    offset=0
                    for n in lengths: chunks.append(raw[offset:offset+n]);offset+=n
                # Chat hooks contain raw JSON. Stock's HTTP writer adds data:
                # framing and [DONE]. Messages/Responses hooks already carry SSE.
                wire=output
                if route.endswith('/chat/completions'):
                    wire=b''.join(b'data: '+chunk+b'\n\n' for chunk in chunks if chunk)+b'data: [DONE]\n\n'
                elif route.endswith('/responses'):
                    # Older stock hooks omit separators; its HTTP writer adds
                    # them per callback. Current stock may already include them.
                    wire=b''
                    for chunk in chunks:
                        wire+=chunk
                        if not chunk.endswith((b'\n\n',b'\r\n\r\n')):
                            wire+=b'\r\n' if chunk.endswith(b'\r\n') else b'\n' if chunk.endswith(b'\n') else b'\n\n'
                    wire+=b'\n'  # Stock's final separator is outside the hooks.
                assert wire==downstream, f'{proof_id}: ordered stream bytes differ'
            else:
                assert output==downstream, f'{proof_id}: response bytes differ'
            assert b'proof_tool' in output, f'{proof_id}: tool name missing'
            if stream:
                frames=[json.loads(chunk) for chunk in chunks if chunk] if route.endswith('/chat/completions') else [json.loads(line[5:]) for chunk in chunks for line in chunk.splitlines() if line.startswith(b'data:') and line[5:].strip()!=b'[DONE]']
                if route.endswith('/messages'):
                    arguments=''.join(f.get('delta',{}).get('partial_json','') for f in frames if f.get('type')=='content_block_delta')
                elif route.endswith('/responses'):
                    arguments=''.join(f.get('delta','') for f in frames if f.get('type')=='response.function_call_arguments.delta')
                else:
                    arguments=''.join(tool.get('function',{}).get('arguments','') for f in frames for choice in f.get('choices',[]) for tool in choice.get('delta',{}).get('tool_calls',[]))
                assert arguments==TOOL_ARGUMENTS, f'{proof_id}: tool argument bytes differ'
            else:
                response=json.loads(output)
                if route.endswith('/messages'):
                    assert next(b['input'] for b in response['content'] if b['type']=='tool_use')==json.loads(TOOL_ARGUMENTS), f'{proof_id}: tool input differs'
                elif route.endswith('/responses'):
                    assert next(b['arguments'] for b in response['output'] if b['type']=='function_call')==TOOL_ARGUMENTS, f'{proof_id}: tool argument bytes differ'
                else:
                    assert response['choices'][0]['message']['tool_calls'][0]['function']['arguments']==TOOL_ARGUMENTS, f'{proof_id}: tool argument bytes differ'
            report['contentProof'][proof_id]={'route':route,'stream':stream,'requestBytes':len(content('request')),'afterAuthBytes':len(content('request_after_auth')),'responseBytes':len(output),'capturedResponseSHA256':hashlib.sha256(output).hexdigest(),'orderedContent':'exact request/after-auth and response bytes; stream callback boundaries restore exact HTTP framing; complete tool arguments verified'}
        # Exclusions must produce no outbox observations; duplicate authority input is overwritten by nginx.
        for kwargs in [dict(key=PERSONAL,headers={'X-Meshix-Capture-Destination':'harness-dev','X-Meshix-Capture-Route':'POST /v1/chat/completions','X-Meshix-Capture-Revision':'proof-v1'}),dict(key=PREVIEW),dict(key=DEV.lower()),dict(port=8319,headers={'X-Meshix-Capture-Destination':'harness-dev','X-Meshix-Capture-Route':'POST /v1/chat/completions','X-Meshix-Capture-Revision':'proof-v1'}),dict(headers={'X-Meshix-Capture-Destination':'harness-prod','X-Meshix-Deployment':'wrong'}),dict(headers={'X-Api-Key':PERSONAL}),dict(headers={'X-Goog-Api-Key':PERSONAL}),dict(headers={'X-Meshix-Deployment':''})]:request(**kwargs)
        duplicate_request()
        time.sleep(.2);assert len(events())==before,'excluded request reached outbox'
        assert request(headers={'X-Meshix-Capture-Destination':'harness-prod'})[0]==200
        assert request(headers={'Authorization':None,'X-Api-Key':DEV})[0]==200
        assert request(headers={'X-Api-Key':DEV})[0]==200
        found=wait_events(base+9)
        for route in ['/v1/models','/v1/messages/count_tokens']:
            status,body,_,_=request(route);assert status==200,(route,status);captures[route]={'status':status,'downstreamSHA256':hashlib.sha256(body).hexdigest()}
        found=wait_events(base+11)
        before=len(found)
        conn=http.client.HTTPConnection('127.0.0.1',8318,timeout=10);conn.request('POST','/v1/messages','{',{'Authorization':'Bearer '+DEV,'X-Meshix-Deployment':DEPLOYMENT,'Content-Type':'application/json'});res=conn.getresponse();assert res.status==400;res.read();conn.close();time.sleep(.1);assert len(events())==before,'pre-hook coverage boundary changed'
        report['preHookFailure']='unavailable: malformed request rejected before plugin lifecycle'
        report['postHookFailure']='failed completion captured'
        assert all(not any(k.lower().startswith('x-meshix-capture-') for k in h) for h in provider_headers),'authority reached provider'
        verify_ledger(found, boot, base+11, capture_config['instanceId'])
        report['protocolProof']=captures;report['durableEventsBeforeRestart']=len(found)
        stop_proc(exporter);exporter=start_exporter();assert len(events())==len(found)
        # Outbox unavailable: inference succeeds; ACKed events survive and unACKed events replay.
        stop_proc(exporter);start=time.monotonic();assert request()[0]==200;report['collectorUnavailableInferenceMs']=(time.monotonic()-start)*1000
        exporter=start_exporter();wait_events(base+12)
        # Fault injection is last: stock correctly cools down the failing upstream auth.
        status,error_body,_,_=request(failure=True,proof_id='error-proof');assert status==503,status
        (ROOT/'error-response.bin').write_bytes(error_body)
        failed=wait_events(base+13);assert any(e.get('completionOutcome')=='failed' for e in failed),'post-hook failure missing'
        error_call=[e for e in failed if e.get('correlation',{}).get('requestId')=='error-proof']
        terminal=next(e for e in error_call if e['kind']=='completion')
        assert terminal.get('error','').encode()==error_body, 'post-hook error bytes differ'
        assert b''.join(base64.b64decode(e.get('body','')) for e in error_call if e['kind']=='request')==content_requests['error-proof'], 'failed request content differs'
        report['errorContentProof']={'downstreamSHA256':hashlib.sha256(error_body).hexdigest(),'capturedDiagnosticSHA256':hashlib.sha256(terminal['error'].encode()).hexdigest(),'failedRequestBytes':len(content_requests['error-proof'])}
        shutdown=stop_proc(stock)
        health,boundary=final_health(boot, base+13, terminal, shutdown, capture_config)
        report['finalHealth']=health
        report['finalHealthBoundary']=boundary
        report['processLossBoundary']='unknown_before_local_commit; graceful drain is not crash-loss proof'
        report['durability']='restart + collector unavailable replay passed';report['scope']='personal/preview/case/conflict/missing deployment/device spoof excluded; gateway overwrite and both carriers passed'
        (ROOT/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:p.kill()
        provider.shutdown()

if __name__=='__main__':main()
