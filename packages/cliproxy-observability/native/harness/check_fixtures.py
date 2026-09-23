"""Verify checked-in recording integrity/provenance without making network calls."""
import base64
import hashlib
import json
from pathlib import Path
import re
import sys

root=Path(sys.argv[1])
for protocol,terminal in [('messages-sse',b'"type":"message_stop"'),('responses-sse',b'"type":"response.completed"'),('chat-json',b'"finish_reason":"stop"')]:
    fixture=json.loads((root/(protocol+'.json')).read_text())
    provenance=fixture['provenance'];events=fixture['events']
    assert provenance['kind']=='real_provider_recording'
    assert provenance['sourceProtocol']==protocol
    assert provenance['selectedUpstreamProvider']=='unknown'
    assert re.fullmatch('[a-f0-9]{64}',provenance['stockSHA256'])
    assert re.fullmatch('[a-f0-9]{64}',provenance['pluginSHA256'])
    canonical=json.dumps(events,separators=(',',':')).encode()
    assert hashlib.sha256(canonical).hexdigest()==provenance['eventsSHA256']
    assert [e['sequence'] for e in events]==list(range(1,len(events)+1))
    assert len({(e['instanceId'],e['pluginBootId'],e['requestId'],e['destinationId']) for e in events})==1
    assert events[0]['kind']=='request' and events[-1]['completionOutcome']=='succeeded'
    content=b''
    for event in events:
        body=base64.b64decode(event.get('body',''),validate=True)
        assert len(body)==event['contentBytes'] and hashlib.sha256(body).hexdigest()==event['contentSha256']
        assert not event.get('gap')
        assert not re.search(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',body)
        assert b'harness-Dev-key-' not in body
        if event['kind'] in ['response','stream_chunk']:content+=body
    assert terminal in content, (protocol,'protocol terminal missing')
    print(protocol+': provenance, identity, hashes, terminal and gap checks passed')
