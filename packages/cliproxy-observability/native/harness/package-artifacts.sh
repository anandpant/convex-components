#!/bin/sh
# Caller has already built/tested the official-stock Linux harness.
set -eu
image=${1:?image required}
output=${2:?output directory required}
[ -z "$(git status --porcelain -- packages/cliproxy-observability)" ] || { echo "Commit the package before recording artifact source identity" >&2; exit 1; }
version=$(node -p 'require("./packages/cliproxy-observability/package.json").version')
architecture=$(docker image inspect "$image" --format '{{.Architecture}}')
case "$architecture" in amd64|arm64) ;; *) exit 1;; esac
mkdir -p "$output/payload"
container=$(docker create "$image")
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
docker cp "$container:/capture/cliproxy-capture.so" "$output/payload/cliproxy-capture.so"
docker cp "$container:/capture/cliproxy-capture-exporter" "$output/payload/cliproxy-capture-exporter"
cp packages/cliproxy-observability/LICENSE "$output/payload/LICENSE"
cp packages/cliproxy-observability/native/plugin/UPSTREAM-LICENSE "$output/payload/UPSTREAM-LICENSE"
node --input-type=module - "$output/payload/manifest.json" "$version" "$architecture" "$(git rev-parse HEAD)" <<'JS'
import { writeFileSync } from "node:fs";
const [path,version,architecture,sourceCommit]=process.argv.slice(2);
writeFileSync(path,JSON.stringify({schemaVersion:1,version,architecture,sourceCommit,stockVersion:"7.3.5",stockBinarySHA256:architecture==="amd64"?"de7a84f665c19e5f991d15faa577436472b44a4dd013eb1c31fe2dae0befc26c":"79766341173c68f9df506fdb9d0b3e1277a7adc58c925812ca0bcc0ecd7af30f",stockCommit:"b681a1e0f7b89d26814f788b60bf84feaf72e912",pluginABI:1,rpcSchema:6,capturePolicy:"hook-body-v1",goBuildImage:"golang:1.26-bookworm@sha256:a688600ca24f8a4d3ca77f95b0dd40704a9fc787c826660eb7ba0b641b8b175d",platform:"linux",libc:"glibc-2.36-or-newer"},null,2)+"\n");
JS
epoch=$(git show -s --format=%ct HEAD)
python3 - "$output" "$version" "$architecture" "$epoch" <<'PYTHON'
import gzip, hashlib, pathlib, sys, tarfile
output=pathlib.Path(sys.argv[1]);version,architecture=sys.argv[2:4];epoch=int(sys.argv[4]);payload=output/'payload'
files=sorted(p for p in payload.iterdir() if p.name != "SHA256SUMS")
(payload/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in files))
archive=output/f'cliproxy-capture_{version}_linux_{architecture}.tar.gz'
with archive.open('wb') as raw, gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as compressed, tarfile.open(fileobj=compressed,mode='w') as tar:
    for path in sorted(payload.iterdir()):
        info=tar.gettarinfo(str(path),arcname=path.name);info.uid=info.gid=0;info.uname=info.gname='root';info.mtime=epoch;info.mode=0o755 if path.name=='cliproxy-capture-exporter' else 0o644
        with path.open('rb') as data:tar.addfile(info,data)
(output/'SHA256SUMS').write_text(hashlib.sha256(archive.read_bytes()).hexdigest()+'  '+archive.name+'\n')
PYTHON
