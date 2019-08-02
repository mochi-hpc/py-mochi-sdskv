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
        db.erase("test_put_key")

    def test_get(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_get_key", "test_get_value")
        val = db.get("test_get_key")
        self.assertEqual(val, "test_get_value")
        with self.assertRaises(KeyError):
            val = db.get("test_get_unknown_key")
        db.erase("test_get_key")
        
    def test_exists(self):
        db = TestClient._ph.open("mydatabase")
        self.assertFalse(db.exists("test_exists_key"))
        db.put("test_exists_key", "test_exists_value")
        self.assertTrue(db.exists("test_exists_key"))
        db.erase("test_exists_key")

    def test_length(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_length_key", "test_length_value")
        self.assertEqual(len("test_length_value"), db.length("test_length_key"))
        db.erase("test_length_key")

    def test_erase(self):
        db = TestClient._ph.open("mydatabase")
        db.put("test_erase_key", "test_erase_value")
        self.assertTrue(db.exists("test_erase_key"))
        db.erase("test_erase_key")
        self.assertFalse(db.exists("test_erase_key"))

    def test_put_multi(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_put_multi_1', 'test_put_multi_2', 'test_put_multi_3']
        vals = ['value1', 'value2', 'value3']
        db.put_multi(keys, vals)
        self.assertEqual(db.get('test_put_multi_1'), 'value1')
        self.assertEqual(db.get('test_put_multi_2'), 'value2')
        self.assertEqual(db.get('test_put_multi_3'), 'value3')
        for k in keys:
            db.erase(k)

    def test_get_multi(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_get_multi_1', 'test_get_multi_2', 'test_get_multi_3']
        vals_in = ['value1', 'value2', 'value3']
        db.put_multi(keys, vals_in)
        vals_out = db.get_multi(keys)
        self.assertEqual(vals_in, vals_out)
        for k in keys:
            db.erase(k)

    def test_length_multi(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_length_multi_1', 'test_length_multi_2', 'test_length_multi_3']
        vals = ['value1', 'value2AA', 'value3BBBBBB']
        db.put_multi(keys, vals)
        lengths = db.length_multi(keys)
        self.assertEqual(len(vals), len(lengths))
        for v,l in zip(vals, lengths):
            self.assertEqual(len(v), l)
        for k in keys:
            db.erase(k)

    def test_list_keys(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_lk_1', 'test_lk_2', 'test_lk_3', 'test_lk_4', 'test_lk_5']
        vals = ['val1', 'val2', 'val3', 'val4', 'val5']
        db.put_multi(keys, vals)
        keys_out = db.list_keys('test_lk', num_keys=2)
        self.assertEqual(keys_out, ['test_lk_1', 'test_lk_2'])
        keys_out = db.list_keys('test_lk_2', num_keys=2)
        self.assertEqual(keys_out, ['test_lk_3', 'test_lk_4'])
        keys_out = db.list_keys('test_lk_2', num_keys=10)
        self.assertEqual(keys_out, ['test_lk_3', 'test_lk_4', 'test_lk_5'])
        for k in keys:
            db.erase(k)

    def test_list_keys_with_prefix(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_A_lkp', 'test_lkp_1', 'test_lkp_2', 'test_lkp_3', 'test_lkp_4', 'test_lkp_5', 'test_Z_lk']
        vals = ['valA', 'val1', 'val2', 'val3', 'val4', 'val5', 'valZ']
        db.put_multi(keys, vals)
        keys_out = db.list_keys('test_lkp', num_keys=2, prefix='test_lkp')
        self.assertEqual(keys_out, ['test_lkp_1', 'test_lkp_2'])
        keys_out = db.list_keys('test_lkp', num_keys=2, prefix='test_lkp')
        self.assertEqual(keys_out, ['test_lkp_1', 'test_lkp_2'])
        keys_out = db.list_keys('test_lkp_2', num_keys=2, prefix='test_lkp')
        self.assertEqual(keys_out, ['test_lkp_3', 'test_lkp_4'])
        keys_out = db.list_keys('test_lkp_2', num_keys=10, prefix='test_lkp')
        self.assertEqual(keys_out, ['test_lkp_3', 'test_lkp_4', 'test_lkp_5'])
        for k in keys:
            db.erase(k)

    def test_list_keyvals(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_lkv_1', 'test_lkv_2', 'test_lkv_3', 'test_lkv_4', 'test_lkv_5']
        vals = ['val1', 'val2', 'val3', 'val4', 'val5']
        db.put_multi(keys, vals)
        keys_out, vals_out = db.list_keyvals('', num_keys=2)
        self.assertEqual(keys_out, ['test_lkv_1', 'test_lkv_2'])
        self.assertEqual(vals_out, ['val1', 'val2'])
        keys_out, vals_out = db.list_keyvals('test_lk', num_keys=2)
        self.assertEqual(keys_out, ['test_lkv_1', 'test_lkv_2'])
        self.assertEqual(vals_out, ['val1', 'val2'])
        keys_out, vals_out = db.list_keyvals('test_lkv_2', num_keys=2)
        self.assertEqual(keys_out, ['test_lkv_3', 'test_lkv_4'])
        self.assertEqual(vals_out, ['val3', 'val4'])
        keys_out, vals_out = db.list_keyvals('test_lkv_2', num_keys=10)
        self.assertEqual(keys_out, ['test_lkv_3', 'test_lkv_4', 'test_lkv_5'])
        self.assertEqual(vals_out, ['val3', 'val4', 'val5'])
        for k in keys:
            db.erase(k)

    def test_list_keyvals_with_prefix(self):
        db = TestClient._ph.open("mydatabase")
        keys = ['test_A_lkvp', 'test_lkvp_1', 'test_lkvp_2', 'test_lkvp_3', 'test_lkvp_4', 'test_lkvp_5', 'test_Z_lk']
        vals = ['valA', 'val1', 'val2', 'val3', 'val4', 'val5', 'valZ']
        db.put_multi(keys, vals)
        keys_out, vals_out = db.list_keyvals('', num_keys=2, prefix='test_lkvp')
        self.assertEqual(keys_out, ['test_lkvp_1', 'test_lkvp_2'])
        self.assertEqual(vals_out, ['val1', 'val2'])
        keys_out, vals_out = db.list_keyvals('test_lkvp', num_keys=2, prefix='test_lkvp')
        self.assertEqual(keys_out, ['test_lkvp_1', 'test_lkvp_2'])
        self.assertEqual(vals_out, ['val1', 'val2'])
        keys_out, vals_out = db.list_keyvals('test_lkvp_2', num_keys=2, prefix='test_lkvp')
        self.assertEqual(keys_out, ['test_lkvp_3', 'test_lkvp_4'])
        self.assertEqual(vals_out, ['val3', 'val4'])
        keys_out, vals_out = db.list_keyvals('test_lkvp_2', num_keys=10, prefix='test_lkvp')
        self.assertEqual(keys_out, ['test_lkvp_3', 'test_lkvp_4', 'test_lkvp_5'])
        self.assertEqual(vals_out, ['val3', 'val4', 'val5'])
        for k in keys:
            db.erase(k)

if __name__ == '__main__':
    unittest.main()
