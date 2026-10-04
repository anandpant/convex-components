"""Run only inside a disposable container or private mount/network namespaces."""
import hashlib
import http.client
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import proof


@unittest.skipUnless(
    os.environ.get('CLIPROXY_HARNESS_ISOLATED') == '1'
    and os.geteuid() == 0 and shutil.which('nginx'),
    'requires explicitly isolated Linux root with nginx',
)
class NginxIsolationTest(unittest.TestCase):
    def test_validation_startup_spooling_and_bad_config_preserve_external_paths(self):
        build = subprocess.run(['nginx', '-V'], capture_output=True, text=True, check=True)
        flags = dict(x[2:].split('=', 1) for x in shlex.split(build.stderr)
                     if x.startswith('--') and '=' in x)
        names = ['client-body', 'proxy', 'fastcgi', 'uwsgi', 'scgi']
        external = [Path(flags[f'http-{name}-temp-path']) for name in names]
        self.assertTrue(all(p.is_absolute() for p in external))
        saved = []
        saved_files = []
        try:
            for path in external:
                path.mkdir(parents=True, exist_ok=True)
                meta = path.stat()
                saved.append((path, meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)))
                sentinel = path / 'capture-proof-isolation-sentinel'
                sentinel.write_bytes(b'external nginx state must remain unchanged')
                os.chown(path, 12345, 12345)
                path.chmod(0o700)

            default_files = [Path(flags[name]) for name in
                             ['pid-path', 'lock-path', 'http-log-path', 'error-log-path']
                             if flags[name] != 'stderr']
            for path in default_files:
                path.parent.mkdir(parents=True, exist_ok=True)
                previous = (path.read_bytes(), path.stat()) if path.exists() else None
                saved_files.append((path, previous))
                path.write_bytes(b'external nginx file must remain unchanged')
                os.chown(path, 12345, 12345)
                path.chmod(0o600)

            def snapshot():
                return [(p.stat().st_uid, p.stat().st_gid, stat.S_IMODE(p.stat().st_mode),
                         hashlib.sha256((p / 'capture-proof-isolation-sentinel').read_bytes()).hexdigest())
                        for p in external] + [
                            (p.stat().st_uid, p.stat().st_gid, stat.S_IMODE(p.stat().st_mode),
                             hashlib.sha256(p.read_bytes()).hexdigest()) for p in default_files]

            before = snapshot()
            original_root = proof.ROOT
            with tempfile.TemporaryDirectory(prefix='nginx-isolation-') as tmp:
                proof.ROOT = Path(tmp)
                process = None
                provider = None
                try:
                    proof.prepare_nginx_runtime()
                    config = proof.ROOT / 'nginx.conf'
                    config.write_text(proof.nginx_config())
                    subprocess.run(proof.nginx_command('-t'), check=True, capture_output=True)
                    self.assertEqual(snapshot(), before)

                    class Provider(BaseHTTPRequestHandler):
                        def log_message(self, *_): pass
                        def do_POST(self):
                            body = self.rfile.read(int(self.headers['Content-Length']))
                            self.send_response(200)
                            self.send_header('Content-Length', str(len(body)))
                            self.end_headers()
                            self.wfile.write(body)

                    provider = ThreadingHTTPServer(('127.0.0.1', 8317), Provider)
                    threading.Thread(target=provider.serve_forever, daemon=True).start()
                    process = subprocess.Popen(proof.nginx_command('-g', 'daemon off;'),
                                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    proof.wait_port(8319)
                    payload = b'x' * 65536
                    client = http.client.HTTPConnection('127.0.0.1', 8319, timeout=10)
                    client.request('POST', '/v1/responses', payload)
                    response = client.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.read(), payload)
                    client.close()
                    log = (proof.ROOT / 'nginx/error.log').read_text()
                    self.assertIn(str(proof.ROOT / 'nginx/client_body'), log)
                    self.assertEqual(snapshot(), before)
                    process.terminate()
                    process.wait(timeout=10)

                    config.write_text(config.read_text() + '\ninvalid_fixture_directive;\n')
                    bad = subprocess.run(proof.nginx_command('-t'), capture_output=True)
                    self.assertNotEqual(bad.returncode, 0)
                    self.assertEqual(snapshot(), before)
                finally:
                    if process is not None and process.poll() is None:
                        process.terminate()
                        process.wait(timeout=10)
                    if provider is not None:
                        provider.shutdown()
                        provider.server_close()
                    proof.ROOT = original_root
            self.assertEqual(snapshot(), before)
        finally:
            for path, previous in saved_files:
                if previous is None:
                    path.unlink(missing_ok=True)
                else:
                    body, meta = previous
                    path.write_bytes(body)
                    os.chown(path, meta.st_uid, meta.st_gid)
                    path.chmod(stat.S_IMODE(meta.st_mode))
            for path, uid, gid, mode in saved:
                (path / 'capture-proof-isolation-sentinel').unlink(missing_ok=True)
                os.chown(path, uid, gid)
                path.chmod(mode)


if __name__ == '__main__':
    unittest.main()
