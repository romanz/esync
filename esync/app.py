import os
import subprocess
import tempfile

import logbook

from . import db
from . import store
from . import util

log = logbook.Logger(__name__)


class App:
    def __init__(self, config):
        self.db = db.DB(config['db'])
        self.dirs = config['dirs'] or []
        self.out_dir = config['blobs']
        self.store = store.open_store(config['store'])
        self.prepare_dirs()
        self.sync_blob_ids()

    def prepare_dirs(self):
        os.makedirs(self.out_dir, exist_ok=True)
        assert os.path.isdir(self.out_dir)

    def sync_blob_ids(self):
        log.info('syncing blob IDs from store')
        self.blob_ids = set(self.store.list_files())

    def add(self, path):
        digest = util.hash_file(path)
        log.info('digest: {} <- {}', digest, path)
        blob_id = self.db.blobs.get(digest)
        if (blob_id is None) or (blob_id not in self.blob_ids):
            blob_id = self._send(path, digest)
        log.info('blob id {} <- {}', blob_id, path)
        self.db.insert_file(file_path=path, file_id=digest)

    def commit(self):
        self.db.close()
        self._send(
            path=self.db.path,
            digest=None,  # skip saving DB digest recursively to the DB
            blob_id='index/{}'.format(util.now()))

    def _send(self, path, digest, blob_id=None):
        temp_path = self._encrypt(path)
        if blob_id is None:
            blob_id = util.hash_file(temp_path)
        log.info('encrypt {} <- {}', blob_id, path)
        self.store.put(blob_id=blob_id, path=temp_path)
        if digest is not None:
            self.db.insert_blob(file_id=digest, blob_id=blob_id)
        self.blob_ids.add(blob_id)
        return blob_id

    def _encrypt(self, path):
        out = tempfile.NamedTemporaryFile(mode='wb', dir=self.out_dir,
                                          prefix='blob-', delete=False)
        args = ['gpg2', '--encrypt', '-r', 'roman-backup']
        subprocess.check_call(args=args,
                              stdin=open(path, 'rb'), stdout=out.file)
        return out.name

    def scan(self):
        for d in self.dirs:
            log.info('scanning {}', d)
            d = os.path.expanduser(d)
            for root, dirs, files in os.walk(d, followlinks=True):
                dirs.sort()
                for f in sorted(files):
                    path = os.path.join(root, f)
                    if os.path.isfile(path):
                        yield path
