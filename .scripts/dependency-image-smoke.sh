#!/usr/bin/env bash
set -euo pipefail
: "${1:?usage: dependency-image-smoke.sh IMAGE}"
python3 - "$1" <<'PY'
import hashlib, http.server, json, os, pathlib, shutil, subprocess, sys, tempfile, threading, time, uuid
image = sys.argv[1]
root = pathlib.Path(os.environ.get('EVIDENCE', '.forge-evidence')).resolve()
root.mkdir(parents=True, exist_ok=True)
run = pathlib.Path(tempfile.mkdtemp(prefix='forge091-fbs-', dir=root))
name = 'forge091-fbs-' + uuid.uuid4().hex[:12]
manifest = {'planned_names': [name], 'name': name, 'image': image, 'root': str(run), 'container_ids': [], 'cleanup': False}
manifest_path = run / 'resources.json'
def save():
    manifest_path.write_text(json.dumps(manifest, indent=2))
def command(*args, check=True, data=None):
    # Parsed output (container IDs, ports) comes from stdout alone; stderr stays apart as diagnostics.
    p = subprocess.run(args, input=data, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
    with (run / 'commands.log').open('a') as log:
        log.write(json.dumps(args) + '\n' + p.stdout + '\nstderr:\n' + p.stderr + '\nexit=' + str(p.returncode) + '\n')
    if check and p.returncode:
        raise RuntimeError(p.stdout + p.stderr)
    return p
save()
# A fresh local server implements the existing content-hash read contract.
payload = b'synthetic file backup integrity payload\x00\xff' * 4096
content_hash = hashlib.sha3_256(payload).hexdigest()
class Source(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        key = self.path.split('/')[3] if len(self.path.split('/')) > 3 else ''
        code = 200 if key == content_hash else (400 if len(key) < 32 else 404)
        self.send_response(code)
        self.end_headers()
        if code == 200:
            self.wfile.write(payload)
    def log_message(self, *args):
        pass
server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Source)
threading.Thread(target=server.serve_forever, daemon=True).start()
pgid = None
try:
    if command('docker', 'container', 'inspect', name, check=False).returncode == 0:
        raise RuntimeError('resource already exists')
    pgid = command('docker', 'run', '-d', '--name', name, '--label', 'forge.run=091',
        '-e', 'POSTGRES_PASSWORD=synthetic', '-p', '127.0.0.1::5432',
        '--tmpfs', '/var/lib/postgresql/data', 'postgres:16-alpine').stdout.strip()
    manifest['container_ids'].append(pgid)
    save()
    for _ in range(60):
        # TCP, not the socket: the entrypoint's init-time server listens on the socket only.
        if command('docker', 'exec', pgid, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('fresh postgres did not become ready')
    port = command('docker', 'inspect', '--format', '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}', pgid).stdout.strip()
    def sql(query, db='postgres'):
        return command('docker', 'exec', '-i', pgid, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, data=query)
    sql('CREATE DATABASE filebackup; CREATE DATABASE alkemio;')
    sql('''CREATE TABLE file (id UUID PRIMARY KEY, "externalID" VARCHAR(128), "createdBy" UUID,
        "createdDate" TIMESTAMPTZ, "updatedDate" TIMESTAMPTZ, size BIGINT, "temporaryLocation" BOOLEAN);''', 'alkemio')
    sql(f'''INSERT INTO file VALUES ('{uuid.uuid4()}', '{content_hash}', NULL, now(), now(), {len(payload)}, false);''', 'alkemio')
    for directory in ('backup', 'restore'):
        (run / directory).mkdir(mode=0o777)
        (run / directory).chmod(0o777)
    run.chmod(0o755)
    config = f'fileServiceBase: http://127.0.0.1:{server.server_port}\n'
    for key, db in [('alkemioDB', 'alkemio'), ('ledgerDB', 'filebackup')]:
        config += f'{key}:\n  host: 127.0.0.1\n  port: {port}\n  user: postgres\n  password: synthetic\n  dbName: {db}\n  sslMode: disable\n'
    config += 'targets:\n  - name: local\n    type: filesystem\n    path: /fixture/backup\n    compression: zstd\n'
    (run / 'config.yaml').write_text(config)
    def cli(*args, check=True, fixture_permissions=False):
        cli_name = name + '-cli-' + str(len(manifest['container_ids']))
        manifest['planned_names'].append(cli_name)
        save()
        options = ['--user', '0', '--entrypoint', '/bin/sh'] if fixture_permissions else []
        invocation = ['-c', 'chmod -R a+rwX /fixture'] if fixture_permissions else [*args, '--config', '/fixture/config.yaml']
        cid = command('docker', 'create', '--name', cli_name, '--network', 'host', '--label', 'forge.run=091',
            '-v', str(run) + ':/fixture', *options, image, *invocation).stdout.strip()
        manifest['container_ids'].append(cid)
        save()
        try:
            return command('docker', 'start', '-a', cid, check=check)
        finally:
            command('docker', 'rm', '-f', '-v', cid)
    cli('migrate')
    cli('backfill')
    cli('verify', '--from', 'local', '--hash', content_hash)
    cli('restore', 'object', '--from', 'local', '--hash', content_hash, '--to', '/fixture/restore')
    restored = (run / 'restore' / content_hash).read_bytes()
    assert restored == payload and hashlib.sha3_256(restored).hexdigest() == content_hash
    stored = run / 'backup' / content_hash[:2] / content_hash[2:4] / content_hash
    assert stored.read_bytes()[:4] == bytes.fromhex('28b52ffd')
    cli(fixture_permissions=True)
    stored.write_bytes(b'corrupt synthetic backup')
    bad_verify = cli('verify', '--from', 'local', '--hash', content_hash, check=False)
    assert bad_verify.returncode != 0 and 'integrity:' in bad_verify.stdout + bad_verify.stderr
    bad_restore = cli('restore', 'object', '--from', 'local', '--hash', content_hash, '--to', '/fixture/restore-corrupt', check=False)
    assert bad_restore.returncode != 0 and 'integrity:' in bad_restore.stdout + bad_restore.stderr
    assert not (run / 'restore-corrupt' / content_hash).exists()
    manifest.update(passed=True, content_hash=content_hash, restored_sha256=hashlib.sha256(restored).hexdigest(), compressed_backup=True)
finally:
    server.shutdown()
    if pgid:
        command('docker', 'rm', '-f', pgid)
    if 'cli' in locals():
        cli(fixture_permissions=True)
    # A run or create that hit the command timeout may have created a container without returning its ID,
    # so remove this run's planned names too, then confirm none remain before recording cleanup as complete.
    leftovers = []
    for planned in manifest['planned_names']:
        try:
            command('docker', 'rm', '-f', '-v', planned, check=False)
            if command('docker', 'container', 'inspect', planned, check=False).returncode == 0:
                leftovers.append(planned)
        except subprocess.TimeoutExpired:
            leftovers.append(planned)
    if leftovers:
        # Keep the fixture files for investigation and fail instead of reporting a clean run.
        manifest['cleanup_leftovers'] = leftovers
        save()
        raise SystemExit('cleanup incomplete, containers remain: ' + ', '.join(leftovers))
    for child in ('config.yaml', 'backup', 'restore', 'restore-corrupt'):
        path = run / child
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    manifest['cleanup'] = True
    save()
    print(manifest_path)
PY
