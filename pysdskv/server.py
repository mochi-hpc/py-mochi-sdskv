# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pysdskvserver
import pymargo

stdmap     = _pysdskvserver.database_type.map
bwtree     = _pysdskvserver.database_type.bwtree
leveldb    = _pysdskvserver.database_type.leveldb
berkeleydb = _pysdskvserver.database_type.berkeleydb

class SDSKVProvider(pymargo.Provider):

    def __init__(self, engine, provider_id):
        super(SDSKVProvider, self).__init__(engine, provider_id)
        self._provider = _pysdskvserver.register(engine.get_internal_mid(), provider_id)

    def attach_database(self, name, path, db_type):
        return _pysdskvserver.attach_database(self._provider, name, path, db_type)

    def remove_database(self, db_id):
        _pysdskvserver.remove_database(self._provider, db_id)

    def remove_all_databases(self):
        _pysdskvserver.remove_all_databases(self._provider)

    @property
    def databases(self):
        return _pysdskvserver.databases(self._provider)
