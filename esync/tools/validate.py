import sys

import logbook
import yaml

from .. import db
from .. import store
from .. import util

log = logbook.Logger(__name__)


class Validator:
    def __init__(self, config):
        self.db = db.DB(config['db'])
        self.store = store.open_store(config['store'])
        self.fail = False

    def _report_error(self, msg, *args):
        log.error(msg, *args)
        self.fail = True

    def check_blobs(self, blob_ids):
        store_blob_ids = set(self.store.list_files())
        for blob_id in blob_ids:
            if blob_id not in store_blob_ids:
                self._report_error('Blob {} is missing from store!', blob_id)

    def check_files(self):
        cursor = self.db.conn.cursor()
        query = "SELECT FilePath, FileId FROM Files"
        for file_path, file_id in cursor.execute(query):
            blob_id = self.db.blobs.get(file_id)
            if blob_id is None:
                self._report_error('Blob {} is missing for file {}!',
                                   file_id, file_path)
            else:
                yield blob_id


def main():
    logbook.StreamHandler(sys.stderr, level='WARNING').push_application()
    config = yaml.load(open('esync.yaml'))
    log.debug('loaded config: {}', config)
    v = Validator(config)
    blobs = v.check_files()
    v.check_blobs(blobs)
    return v.fail

if __name__ == '__main__':
    sys.exit(int(main()))
