# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pysdskvclient
import pymargo

class SDSKVClient():

    def __init__(self, engine):
        self._client = _pysdskvclient.client_init(engine.get_internal_mid())

    def create_provider_handle(self, addr, provider_id):
        ph = _pysdskvclient.provider_handle_create(self._client, addr.get_internal_hg_addr(), provider_id)
        return SDSKVProviderHandle(ph)

    def shutdown_service(self, addr):
        _pysdskvclient.shutdown_service(self._client, addr.get_internal_hg_addr())

    def __del__(self):
        _pysdskvclient.client_finalize(self._client)

class SDSKVProviderHandle():

    def __init__(self, ph):
        self._ph = ph

    def __del__(self):
        if(self._ph is not None):
            _pysdskvclient.provider_handle_release(self._ph)
            self._ph = None

    def open(self, db_name):
        db_id = _pysdskvclient.open(self._ph, db_name)
        if(db_id != 0):
            return SDSKVDatabase(self, db_id)
        else:
            raise RuntimeError('Could not open database {}'.format(db_name))

class SDSKVDatabase():

    def __init__(self, ph, db_id):
        self._sdskv_ph = ph
        self._db_id = db_id

    def put(self, key, value):
        _pysdskvclient.put(self._sdskv_ph._ph, self._db_id, key, value)

    def put_multi(self, keys, values):
        raise NotImplementedError('put_multi')

    def get(self, key):
        val = _pysdskvclient.get(self._sdskv_ph._ph, self._db_id, key)
        if(val is None):
            raise KeyError(key)
        else:
            return val

    def get_multi(self, keys):
        raise NotImplementedError('get_multi')

    def length(self, key):
        l = _pysdskvclient.length(self._sdskv_ph._ph, self._db_id, key)
        if(l is None):
            raise KeyError(key)
        else:
            return l

    def length_multi(self, key):
        raise NotImplementedError('length_multi')

    def exists(self, key):
        return _pysdskvclient.exists(self._sdskv_ph._ph, self._db_id, key)

    def erase(self, key):
        _pysdskvclient.erase(self._sdskv_ph._ph, self._db_id, key)

    def migrate(self, dest_addr_str, dest_provider_id, dest_root, remove_source=False):
        return _pysdskvclient.migrate_database(self._sdskv_ph._ph, self._db, dest_addr, dest_provider_id, dest_root, remove_source)

    def get_id(self):
        return self._db_id
