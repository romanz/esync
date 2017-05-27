import sqlite3

import logbook

from . import util

log = logbook.Logger(__name__)

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS Files (
        SyncDate TEXT NOT NULL,
        FilePath TEXT NOT NULL,
        FileId CHAR(64) NOT NULL,
        PRIMARY Key(SyncDate, FilePath)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Blobs (
        FileId CHAR(64) NOT NULL,
        BlobId CHAR(64) NOT NULL,
        PRIMARY Key(FileId)
    );
    """]


class DB:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.create_tables()
        self.files = {}
        self.blobs = _load_blobs(self.conn.cursor())
        self.sync_date = util.now()

    def create_tables(self):
        cursor = self.conn.cursor()
        for s in SCHEMA:
            cursor.execute(s)

        self.conn.commit()

    def insert_file(self, file_path, file_id):
        c = self.conn.cursor()
        c.execute(
            'INSERT INTO Files (SyncDate, FilePath, FileId) VALUES (?, ?, ?)',
            (self.sync_date, file_path, file_id))
        self.conn.commit()
        self.files[file_path] = file_id

    def insert_blob(self, file_id, blob_id):
        c = self.conn.cursor()
        c.execute('INSERT INTO Blobs (FileId, BlobId) VALUES (?, ?)',
                  (file_id, blob_id))
        self.conn.commit()
        self.blobs[file_id] = blob_id

    def close(self):
        self.conn.commit()
        self.conn.close()


def _load_blobs(cursor):
    cursor.execute("SELECT FileId, BlobId FROM Blobs")
    return dict(cursor)
