# -*- coding: utf-8 -*-
import os
import sqlite3

# since db manager

class SinceDBManager:
    """ since db manager """

    def __init__(self, sincedb_path, logger=None):
        self._sincedb_path = sincedb_path
        self._logger = logger

    def sincedb_init(self):
        """Initializes the sincedb schema in an sqlite db"""
        if not self._sincedb_path:
            return

        if not os.path.exists(self._sincedb_path):
            self._logger.debug('Initializing sincedb sqlite schema')
            conn = sqlite3.connect(self._sincedb_path, isolation_level=None)
            conn.execute("""
            create table sincedb (
                fid      text primary key,
                filename text,
                position integer default 1
            );
            """)
            conn.close()

    def sincedb_update_position(self, filename, fid=None, lines=0):
        """Retrieves the starting position from the sincedb sql db for a given file
        Returns a boolean representing whether or not it updated the record
        """
        if not self._sincedb_path:
            return False

        if not fid:
            fid = self.get_file_id(os.stat(filename))

        self.sincedb_init()

        conn = sqlite3.connect(self._sincedb_path, isolation_level=None)
        cursor = conn.cursor()
        query = "insert or ignore into sincedb (fid, filename) values (:fid, :filename);"
        cursor.execute(query, {
            'fid': fid,
            'filename': filename
        })

        query = "update sincedb set position = :position where fid = :fid and filename = :filename"
        cursor.execute(query, {
            'fid': fid,
            'filename': filename,
            'position': int(lines),
        })
        conn.close()

        self._logger.debug("[{0}] - updated sincedb for logfile {1} pos {2}".format(fid, filename, int(lines)))

        return True

    @staticmethod
    def get_file_id(st):
        return "%xg%x" % (st.st_dev, st.st_ino)


    def sincedb_start_position(self, file, fid=None):
        """Retrieves the starting position from the sincedb sql db
        for a given file
        """
        if not self._sincedb_path:
            return None

        if not fid:
            fid = self.get_file_id(os.stat(file.name))

        self.sincedb_init()
        conn = sqlite3.connect(self._sincedb_path, isolation_level=None)
        cursor = conn.cursor()
        cursor.execute("select position from sincedb where fid = :fid and filename = :filename", {
            'fid': fid,
            'filename': file.name
        })

        start_position = None
        for row in cursor.fetchall():
            start_position, = row

        self._logger.debug("[{0}] - get start pos from sincedb for logfile {1} pos {2}".format(fid, file.name, start_position))

        return start_position

