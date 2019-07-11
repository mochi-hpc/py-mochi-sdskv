import sys
import os
import tempfile
import shutil
import unittest
from pymargo.core import Engine, Address, Handle
import pysdskv.server
from pysdskv.server import SDSKVProvider

class TestServer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._engine = Engine('tcp://127.0.0.1:1234')

    @classmethod
    def tearDownClass(cls):
        cls._engine.finalize()

    def test_attach_databases(self):
        provider = SDSKVProvider(TestServer._engine, 1)
        self.assertEqual(len(provider.databases), 0)
        path = tempfile.mkdtemp()
        db_id1 = provider.attach_database("mydatabase1", path, pysdskv.server.leveldb)
        self.assertEqual(len(provider.databases), 1)
        db_id2 = provider.attach_database("mydatabase2", path, pysdskv.server.leveldb)
        db_id3 = provider.attach_database("mydatabase3", path, pysdskv.server.leveldb)
        self.assertEqual(len(provider.databases), 3)
        self.assertEqual([db_id1, db_id2, db_id3], provider.databases)
        shutil.rmtree(path)

    def test_remote_databases(self):
        provider = SDSKVProvider(TestServer._engine, 2)
        path = tempfile.mkdtemp()
        db_id1 = provider.attach_database("mydatabase1", path, pysdskv.server.leveldb)
        db_id2 = provider.attach_database("mydatabase2", path, pysdskv.server.leveldb)
        db_id3 = provider.attach_database("mydatabase3", path, pysdskv.server.leveldb)
        provider.remove_database(db_id2)
        self.assertEqual([db_id1, db_id3], provider.databases)
        provider.remove_all_databases()
        self.assertEqual(len(provider.databases), 0)
        shutil.rmtree(path)

if __name__ == '__main__':
    unittest.main()
