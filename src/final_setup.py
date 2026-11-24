"""Obtain the first-delivery SQLite database (source 1) for the ETL.

Raw data is not stored in this repository. The first delivery's public
repository is downloaded once, and its database is checked against a known
SHA-256 so every run extracts exactly the same source data.
"""
from pathlib import Path
import hashlib
import os
import shutil
import urllib.request
import zipfile

FIRST_DELIVERY_URL = os.getenv(
    'INDIA_FIRST_DELIVERY_URL',
    'https://github.com/santbce/etl-calidad-aire-india/archive/refs/heads/main.zip')
FIRST_DELIVERY_DB_SHA256 = 'aecc6c0f28c1113053391c464d5e828862983dc57217888b23f25282177e1be6'


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def _download(archive):
    archive.parent.mkdir(parents=True, exist_ok=True)
    partial = archive.with_suffix('.part')
    print('Downloading the first-delivery repository:', FIRST_DELIVERY_URL)
    with urllib.request.urlopen(FIRST_DELIVERY_URL, timeout=120) as response, open(partial, 'wb') as out:
        shutil.copyfileobj(response, out)
    partial.replace(archive)


def prepare():
    root = Path(os.getenv('INDIA_PROJECT_ROOT', Path(__file__).resolve().parents[1]))
    target = root / 'primera_entrega'
    database = target / 'database/air_quality.db'
    if database.exists() and _sha256(database) == FIRST_DELIVERY_DB_SHA256:
        return
    archive = root / 'data/etl-calidad-aire-india-main.zip'
    if not archive.exists():
        _download(archive)
    with zipfile.ZipFile(archive) as z:
        dbs = [p for p in z.namelist() if p.endswith('/database/air_quality.db')]
        if len(dbs) != 1:
            raise ValueError('First-delivery archive structure changed')
        prefix = dbs[0].split('database/air_quality.db')[0]
        for name in z.namelist():
            if name.startswith(prefix) and not name.endswith('/'):
                relative = Path(name[len(prefix):])
                dest = (target / relative).resolve()
                if not dest.is_relative_to(target.resolve()):
                    raise ValueError('Unsafe archive member')
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(z.read(name))
    if _sha256(database) != FIRST_DELIVERY_DB_SHA256:
        raise ValueError('The first-delivery database does not match the expected source data (SHA-256 mismatch)')
    print('First-delivery database prepared and verified')


if __name__ == '__main__':
    prepare()
