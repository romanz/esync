import os

import logbook

from b2.account_info.sqlite_account_info import SqliteAccountInfo
from b2.api import B2Api
from b2.b2http import B2Http
from b2.cache import AuthInfoCache
from b2.progress import TqdmProgressListener
from b2.raw_api import B2RawApi

from . import util

log = logbook.Logger(__name__)


class LocalStore:
    def __init__(self, path):
        self.path = path
        assert os.path.isdir(self.path)

    def list_files(self):
        return os.listdir(self.path)

    def blob_path(self, blob_id):
        return os.path.join(self.path, blob_id)

    def put(self, blob_id, path):
        store_path = self.blob_path(blob_id)
        if not os.path.exists(store_path):
            os.rename(path, store_path)

    def get(self, blob_id):
        store_path = self.blob_path(blob_id)
        assert os.path.exists(store_path)
        return store_path


class B2Store:
    def __init__(self, bucket_id):
        info = SqliteAccountInfo()
        self.api = B2Api(info, AuthInfoCache(info),
                         raw_api=B2RawApi(B2Http()))
        self.bucket = self.api.get_bucket_by_name(bucket_id)

    def get_hash(self, blob_id):
        response = self.bucket.list_file_names(start_filename=blob_id,
                                               max_entries=1)
        log.debug('file {}: {}', blob_id, response)
        item, = response['files']
        # TODO: verify filename
        return item['contentSha1']

    def put(self, blob_id, path):
        expected_sha1 = util.hash_file(path, hasher=util.sha1)
        listener = TqdmProgressListener(blob_id)
        self.bucket.upload_local_file(
            local_file=path,
            file_name=blob_id,
            sha1_sum=expected_sha1,
            progress_listener=listener)
        remote_sha1 = self.get_hash(blob_id)
        if remote_sha1 and remote_sha1 != expected_sha1:
            raise ValueError('Bad SHA1', remote_sha1)
        os.remove(path)

    def list_files(self):
        for version, _ in self.bucket.ls():
            yield version.file_name


STORES = {
    'local': LocalStore,
    'b2': B2Store,
}


def open_store(store):
    prefix, arg = store.split('://', 1)
    return STORES[prefix](arg)
