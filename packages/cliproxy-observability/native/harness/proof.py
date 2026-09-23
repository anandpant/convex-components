"""Isolated stock binary proof. All provider payloads here are SIMULATED, never real recordings."""
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
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
class Provider(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_): pass
    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        provider_headers.append(dict(self.headers))
        if data.get('stream'):
            body = b''
            for item in [
                {'choices':[{'index':0,'delta':{'role':'assistant','content':'CAPTURE_'},'finish_reason':None}]},
                {'choices':[{'index':0,'delta':{'content':'OK'},'finish_reason':None}]},
                {'choices':[{'index':0,'delta':{},'finish_reason':'stop'}], 'usage':{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10}}
            ]:
                item.update(id='chatcmpl-harness', object='chat.completion.chunk', created=1700000000, model='proof-model')
                body += b'data: ' + json.dumps(item).encode() + b'\n\n'
            body += b'data: [DONE]\n\n'
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(body)));self.end_headers()
            for part in body.splitlines(keepends=True): self.wfile.write(part);self.wfile.flush()
        else:
            body = json.dumps({'id':'chatcmpl-harness','object':'chat.completion','created':1700000000,'model':'proof-model','choices':[{'index':0,'message':{'role':'assistant','content':'CAPTURE_OK'},'finish_reason':'stop'}], 'usage':{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10}}).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)


def wait_port(port):
    for _ in range(150):
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1): return
        except OSError: time.sleep(.1)
    raise RuntimeError(f'port {port} did not open')


def request(path='/v1/chat/completions', *, port=8318, key=DEV, headers=None, stream=False, payload_size=32):
    h={'Authorization':'Bearer '+key,'X-Meshix-Deployment':DEPLOYMENT,'Content-Type':'application/json'}
    if headers: h.update(headers)
    h={k:v for k,v in h.items() if v is not None}
    data={'model':'proof-model','stream':stream,'messages':[{'role':'user','content':'Say CAPTURE_OK. '+('x'*payload_size)}],'max_tokens':32}
    if path=='/v1/responses': data={'model':'proof-model','stream':stream,'input':'Say CAPTURE_OK. '+('x'*payload_size)}
    c=http.client.HTTPConnection('127.0.0.1',port,timeout=30)
    start=time.perf_counter_ns();c.request('GET' if path=='/v1/models' else 'POST',path,json.dumps(data),h);r=c.getresponse();first=r.read(1);ttft=(time.perf_counter_ns()-start)/1e6;body=first+r.read();latency=(time.perf_counter_ns()-start)/1e6;status=r.status;c.close()
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
    return '''worker_processes 1; pid /tmp/capture-proof/nginx.pid;
error_log /tmp/capture-proof/nginx-error.log warn;
events { worker_connections 1024; }
http { access_log off; client_max_body_size 2m; '''+'\n'.join(maps)+f'''
server {{ listen 127.0.0.1:8318; {locations} location / {{ return 404; }} }}
server {{ listen 127.0.0.1:8319; location / {{
 proxy_http_version 1.1; proxy_buffering off; proxy_set_header Upgrade $http_upgrade;
 proxy_set_header Connection "upgrade";
 proxy_set_header X-Meshix-Capture-Destination "";
 proxy_set_header X-Meshix-Capture-Route "";
 proxy_set_header X-Meshix-Capture-Revision "";
 proxy_pass http://127.0.0.1:8317;
}} }} }}'''


def events():
    with sqlite3.connect(ROOT/'outbox/events.db') as db: return [json.loads(r[0]) for r in db.execute('select payload from events order by rowid')]

def wait_events(expected):
    for _ in range(100):
        found=events()
        if sum(e['kind']=='completion' for e in found)>=expected:return found
        time.sleep(.05)
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
    ROOT.mkdir(mode=0o700);(ROOT/'auth').mkdir();(ROOT/'outbox').mkdir(mode=0o700)
    capture_config={'enabled':True,'instanceId':'isolated-stock-proof','revision':'proof-v1','socket':str(ROOT/'outbox/capture.sock'),'bindings':[{'key':DEV,'destinationId':'harness-dev','deployment':DEPLOYMENT,'environment':'dev'},{'key':PROD,'destinationId':'harness-prod','deployment':'https://harness-prod.convex.site','environment':'prod'}]}
    (ROOT/'capture.json').write_text(json.dumps(capture_config));(ROOT/'capture.json').chmod(0o600)
    (ROOT/'nginx.conf').write_text(nginx_config())
    provider=ThreadingHTTPServer(('127.0.0.1',8316),Provider);threading.Thread(target=provider.serve_forever,daemon=True).start()
    procs=[];report={'fixtureProvenance':'simulated_provider_playback','stockVersion':'7.3.5','stockSHA256':hashlib.sha256(Path('/stock/cli-proxy-api').read_bytes()).hexdigest(),'architecture':platform.machine(),'load':{'concurrency':8,'calls':160,'requestContentBytes':65536}}
    def start_exporter():
        p=subprocess.Popen(['/capture/cliproxy-capture-exporter','--socket',str(ROOT/'outbox/capture.sock'),'--db',str(ROOT/'outbox/events.db'),'--reserve-bytes','1048576'],stdout=subprocess.DEVNULL,stderr=open(ROOT/'exporter.log','ab'));procs.append(p)
        for _ in range(100):
            if (ROOT/'outbox/capture.sock').exists():break
            time.sleep(.05)
        return p
    def start_stock(enabled):
        (ROOT/'stock.yaml').write_text(config(enabled))
        p=subprocess.Popen(['/stock/cli-proxy-api','-config',str(ROOT/'stock.yaml')],env={**os.environ,'CLIPROXY_CAPTURE_CONFIG':str(ROOT/'capture.json')},stdout=open(ROOT/f'stock-{enabled}.log','wb'),stderr=subprocess.STDOUT);procs.append(p);wait_port(8317);return p
    def stop_proc(p):p.terminate();p.wait(timeout=10)
    try:
        exporter=start_exporter()
        nginx=subprocess.Popen(['nginx','-c',str(ROOT/'nginx.conf'),'-g','daemon off;']);procs.append(nginx);wait_port(8318)
        stock=start_stock(False)
        for enabled in [False,True]:
            if enabled:
                report["baselineAuthorityPositiveControl"]=any(any(k.lower().startswith("x-meshix-capture-") for k in h) for h in provider_headers)
                provider_headers.clear()
                stop_proc(stock);stock=start_stock(True)
            for _ in range(5): assert request()[0]==200
            stop=threading.Event();samples={};monitor=threading.Thread(target=peak_rss,args=({'stock':stock.pid,'exporter':exporter.pid},stop,samples));monitor.start()
            start=time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool: results=list(pool.map(lambda _:request(stream=True,payload_size=65536),range(160)))
            elapsed=time.monotonic()-start;stop.set();monitor.join()
            assert all(r[0]==200 and b'CAPTURE_' in r[1] for r in results)
            report['enabled' if enabled else 'baseline']={'ttfbMs':percentiles([r[2] for r in results]),'latencyMs':percentiles([r[3] for r in results]),'callsPerSecond':len(results)/elapsed,'peakRssKiB':samples}
        base=sum(e['kind']=='completion' for e in wait_events(165))
        captures={}
        for route,stream in [('/v1/messages',True),('/v1/responses',True),('/v1/chat/completions',False)]:
            status,body,_,_=request(route,stream=stream);assert status==200,(route,status,body[:200]);captures[route]={'status':status,'downstreamSHA256':hashlib.sha256(body).hexdigest()}
        found=wait_events(base+3);before=len(found)
        for route in captures:
            output=[e for e in found if e['route']=='POST '+route and e['kind'] in ['response','stream_chunk']]
            assert any(e.get('body') for e in output), f'{route}: body missing'
            assert not any(e.get('gap') for e in output), f'{route}: incomplete body'
        # Exclusions must produce no outbox observations; duplicate authority input is overwritten by nginx.
        for kwargs in [dict(key=PERSONAL,headers={'X-Meshix-Capture-Destination':'harness-dev','X-Meshix-Capture-Route':'POST /v1/chat/completions','X-Meshix-Capture-Revision':'proof-v1'}),dict(key=PREVIEW),dict(key=DEV.lower()),dict(port=8319,headers={'X-Meshix-Capture-Destination':'harness-dev','X-Meshix-Capture-Route':'POST /v1/chat/completions','X-Meshix-Capture-Revision':'proof-v1'}),dict(headers={'X-Meshix-Capture-Destination':'harness-prod','X-Meshix-Deployment':'wrong'}),dict(headers={'X-Api-Key':PERSONAL}),dict(headers={'X-Goog-Api-Key':PERSONAL}),dict(headers={'X-Meshix-Deployment':''})]:request(**kwargs)
        duplicate_request()
        time.sleep(.2);assert len(events())==before,'excluded request reached outbox'
        assert request(headers={'X-Meshix-Capture-Destination':'harness-prod'})[0]==200
        assert request(headers={'Authorization':None,'X-Api-Key':DEV})[0]==200
        assert request(headers={'X-Api-Key':DEV})[0]==200
        found=wait_events(base+6)
        assert all(not any(k.lower().startswith('x-meshix-capture-') for k in h) for h in provider_headers),'authority reached provider'
        for e in found:
            body=__import__('base64').b64decode(e.get('body',''));assert hashlib.sha256(body).hexdigest()==e['contentSha256'];assert DEV.encode() not in body
            assert e['destinationId']=='harness-dev' and e['requestId'] and e['pluginBootId']
        for e in found:
            if e['kind']=='completion':
                sequence=[x['sequence'] for x in found if x['requestId']==e['requestId']]
                assert sequence==list(range(1,e['sequence']+1)), 'sequence gap'
        report['protocolProof']=captures;report['durableEventsBeforeRestart']=len(found)
        stop_proc(exporter);exporter=start_exporter();assert len(events())==len(found)
        # Outbox unavailable: inference succeeds; ACKed events survive and unACKed events replay.
        stop_proc(exporter);start=time.monotonic();assert request()[0]==200;report['collectorUnavailableInferenceMs']=(time.monotonic()-start)*1000
        exporter=start_exporter();wait_events(base+7)
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
