# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pysdskvclient
import pymargo

class SDSKVClient():

    def __init__(self, mid):
        self._client = _pysdskvclient.client_init(mid._mid)

    def create_provider_handle(self, addr, provider_id):
        ph = _pysdskvclient.provider_handle_create(self._client, addr.get_hg_addr(), provider_id)
        return SDSKVProviderHandle(ph)

    def shutdown_service(self, addr):
        _pysdskvclient.shutdown_service(self._client, addr.get_hg_addr())

    def finalize(self):
        _pysdskvclient.client_finalize(self._client)

class SDSKVProviderHandle():

    def __init__(self, ph):
        self._ph = ph

    def release(self):
        if not (self._ph is None):
            _pysdskvclient.provider_handle_release(self._ph)
            self._ph = None

    def __del__(self):
        self.release()

    def open(self, db_name):
        db = _pysdskvclient.open(self._ph, db_name)
        if(db != 0):
            return SDSKVDatabase(self, db)
        else:
            return None

class SDSKVDatabase():

    def __init__(self, ph, db):
        self._ph = ph
        self._db = db

    def put(self, key, value):
        _pysdskvclient.put(self._ph._ph, self._db, key, value)

    def get(self, key):
        return _pysdskvclient.get(self._ph._ph, self._db, key)

    def exists(self, key):
        return _pysdskvclient.exists(self._ph._ph, self._db, key)

    def erase(self, key):
        return _pysdskvclient.erase(self._ph._ph, self._db, key)
