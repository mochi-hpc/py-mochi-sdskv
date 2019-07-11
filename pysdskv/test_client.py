import sys
import os
import tempfile
import shutil
import unittest
from pymargo.core import Engine, Address, Handle
import pysdskv.server
import pysdskv.client
from pysdskv.server import *
from pysdskv.client import *

class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._engine = Engine('tcp://127.0.0.1:1234')
        cls._provider = SDSKVProvider(cls._engine, 1)
        cls._path = tempfile.mkdtemp()
        cls._db_id = cls._provider.attach_database("mydatabase", cls._path, pysdskv.server.leveldb)
        cls._client = SDSKVClient(cls._engine)
        cls._addr = cls._engine.lookup('tcp://127.0.0.1:1234')
        cls._ph = cls._client.create_provider_handle(cls._addr, 1)

    @classmethod
    def tearDownClass(cls):
        del cls._provider
        del cls._ph
        del cls._addr
        del cls._client
        cls._engine.finalize()
        shutil.rmtree(cls._path)

    def test_open(self):
        db = TestClient._ph.open("mydatabase")
        self.assertEqual(db.get_id(), TestClient._db_id)

    def test_put(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_put_key", "test_put_value")

    def test_get(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_get_key", "test_get_value")
        val = db.get("test_get_key")
        self.assertEqual(val, "test_get_value")
        with self.assertRaises(KeyError):
            val = db.get("test_get_unknown_key")
        
    def test_exists(self):
        db = TestClient._ph.open("mydatabase")
        self.assertFalse(db.exists("test_exists_key"))
        db.put("test_exists_key", "test_exists_value")
        self.assertTrue(db.exists("test_exists_key"))

    def test_erase(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_erase_key", "test_erase_value")
        self.assertTrue(db.exists("test_erase_key"))
        db.erase("test_erase_key")
        self.assertFalse(db.exists("test_erase_key"))

if __name__ == '__main__':
    unittest.main()
