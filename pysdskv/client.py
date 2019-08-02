# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pysdskvclient
import pymargo

class SDSKVClient():
    """
    The SDSKVClient is the object that holds the RPCs that can be made
    towards and SDSKVProvider.
    """

    def __init__(self, engine):
        """
        Constructor.

        Args:
            engine (pymargo.Engine): engine used to issue RPCs.
        """
        self._client = _pysdskvclient.client_init(engine.get_internal_mid())

    def create_provider_handle(self, addr, provider_id=0):
        """
        Creates a provider handle givem an address (string) and a provider id (int).

        Args:
            addr (str): Address of the provider.
            provider_id (int): Provider id.

        Returns:
            An SDSKVProviderHandle instance.
        """
        ph = _pysdskvclient.provider_handle_create(self._client, addr.get_internal_hg_addr(), provider_id)
        return SDSKVProviderHandle(ph)

    def shutdown_service(self, addr):
        """
        Shuts down the service remotely at a given address.

        Args:
            addr (pymargo.Address): Address of the engine to shut down.
        """
        _pysdskvclient.shutdown_service(self._client, addr.get_internal_hg_addr())

    def __del__(self):
        """
        Finalizes the client.
        """
        _pysdskvclient.client_finalize(self._client)


class SDSKVProviderHandle():
    """
    The SDSKVProviderHandle object represents the interface to a particular provider.
    """

    def __init__(self, ph):
        """
        Constructor. Not supposed to be called by users. Use SDSKVClient.create_provider_handle
        to create an instance of SDSKVProviderHandle.
        """
        self._ph = ph

    def __del__(self):
        """
        Destructor. Releases the provider handle.
        """
        if(self._ph is not None):
            _pysdskvclient.provider_handle_release(self._ph)
            self._ph = None

    def open(self, db_name):
        """
        Open a database identified by db_name from the provider,
        and returns a SDSKVDatabase instance.
        """
        db_id = _pysdskvclient.open(self._ph, db_name)
        if(db_id != 0):
            return SDSKVDatabase(self, db_id, db_name)
        else:
            raise RuntimeError('Could not open database {}'.format(db_name))

    @property
    def databases(self):
        """
        Lists the Databases held by the provider.
        """
        l = _pysdskvclient.list_databases(self._ph)
        result = []
        for db_info in l:
            db_name, db_id = db_info
            if(db_id != 0):
                result.append(SDSKVDatabase(self, db_id, db_name))
        return result


class SDSKVDatabase():
    """
    The SDSKVDatabase object represents the entry point to a given database.
    """

    def __init__(self, ph, db_id, name):
        """
        Constructor. Not supposed to be called by users. Use SDSKVProviderHandle.open()
        to create an instance of SDSKVDatabase.
        """
        self._sdskv_ph = ph
        self._db_id = db_id
        self._db_name = name

    @property
    def name(self):
        """Name of the database."""
        return self._db_name

    def put(self, key, value):
        """Puts a key (str) value (str) pair in the database."""
        _pysdskvclient.put(self._sdskv_ph._ph, self._db_id, key, value)

    def __setitem__(self, key, value):
        """Equivalent to put()."""
        self.put(key, value)

    def put_multi(self, keys, values):
        """
        Puts multiple key value pairs (keys and values must be lists of strings,
        these lists must be the same size.
        """
        if(len(keys) != len(values)):
            raise RuntimeError("Number of keys and values do not match")
        _pysdskvclient.put_multi(self._sdskv_ph._ph, self._db_id, keys, values)

    def get(self, key, value_size=0):
        """
        Gets the value associated with a key.
        If the value_size argument is 0, a first RPC will query the length of the
        value, and a second RPC will query the value. If the value_size is set to
        a non-zero value, this value_size will be used as hint of the maximum size
        the value can be. If the actual value is larger, an exception will be thrown.

        Note: the value_size is used to allocate a buffer to hold the value, so if
        the user doesn't know the size, it is a very bad practice to conservatively
        set value_size to a large value that could cause the program to run out of
        memory.
        """
        val = _pysdskvclient.get(self._sdskv_ph._ph, self._db_id, key, value_size)
        if(val is None):
            raise KeyError(key)
        else:
            return val

    def __getitem__(self, key):
        """Equivalent to get() with a value_size of 0."""
        return self.get(key)

    def get_multi(self, keys, value_sizes=0):
        """
        Gets the values associated with an array of keys.
        If value_sizes is unspecified or set to 0, a first RPC will query the
        sizes of the values, and a second RPC will query the actual values.
        If value_sizes is set to a non-zero value, this size will be used as a
        hint of the maximum size a value can have.
        If value_sizes is a list of ints, this list should be the same length
        as the keys list and each int will be used as a hint of the size of the
        corresponding value.
        """
        if(len(keys) == 0):
            return []
        if(isinstance(value_sizes, int)):
            value_sizes = [ value_sizes ] * len(keys)
        if(len(keys) != len(value_sizes)):
            raise ValueError("length of value_sizes differs from length of keys list")
        return _pysdskvclient.get_multi(self._sdskv_ph._ph, self._db_id, keys, value_sizes)

    def length(self, key):
        """
        Returns the length of the value associated with a given key.
        Raises a KeyError exception if the key does not exist in the database.
        """
        l = _pysdskvclient.length(self._sdskv_ph._ph, self._db_id, key)
        if(l is None):
            raise KeyError(key)
        else:
            return l

    def length_multi(self, keys):
        """
        Returns the lengths of the values associated with the specified list of keys.
        Note that is a key does not exist in the database, the corresponding length
        will be 0 but the function will not fail.
        """
        return _pysdskvclient.length_multi(self._sdskv_ph._ph, self._db_id, keys)

    def exists(self, key):
        """
        Returns True if the specified key exists in the database, false otherwise.
        """
        return _pysdskvclient.exists(self._sdskv_ph._ph, self._db_id, key)

    def erase(self, key):
        """
        Erases the key from the database.
        """
        _pysdskvclient.erase(self._sdskv_ph._ph, self._db_id, key)

    def __delitem__(self, key):
        """Equivalent to erase()."""
        self.erase(key)


    def list_keys(self, after='', num_keys=1, prefix='', key_size=0):
        """
        Lists up to num_keys keys from the database.
        If "after" is specified, the returned keys will come strictly after
        this specified start key. If prefix is specified, the keys returned
        will be only the ones starting with the specified prefix.
        If key_sizes is a non-zero integer, this value will be used as a hint
        for the maximum size any given key can take. If it is unspecified,
        a first RPC will collect the key sizes before a second RPC actually
        fetches the keys.
        """
        return _pysdskvclient.list_keys(self._sdskv_ph._ph, self._db_id, after, prefix, num_keys, key_size)

    def list_keyvals(self, after='', num_keys=1, prefix='', key_size=0, val_size=0):
        """
        Lists up to num_keys key/value pairs from the database.
        The semantic of this function is the same as list_keys, but the function
        returns a list of pairs (key, value) and the val_size argument is used to
        hint at the maximum value size. If either key_size or val_size is 0, a first
        RPC will query the size of the keys and values.
        """
        return _pysdskvclient.list_keyvals(self._sdskv_ph._ph, self._db_id, after, prefix, num_keys, key_size, val_size)

    def migrate(self, dest_addr_str, dest_provider_id, dest_root, remove_source=False):
        """
        Asks the provider to migrate the database to a destination provider given its
        address and provider id, and a destination root path where to place the database files.
        If remove_source is True, this database instance will not be valid after the migration.
        """
        _pysdskvclient.migrate_database(self._sdskv_ph._ph, self._db, dest_addr, dest_provider_id, dest_root, remove_source)

    def get_id(self):
        """Get the internal database id."""
        return self._db_id

    def keys(self, after='', keys_per_request=1, prefix='', key_size=0):
        """Returns a convenient iterator that will call list_keys to get the next keys."""
        return SDSKVIterator(self, after=after, prefix=prefix,
                items_per_request=keys_per_request,
                include_values=False, 
                key_size=key_size, val_size=0)

    def items(self, after='', keys_per_request=1, prefix='', key_size=0, val_size=0):
        """Returns a convenient iterator that will call list_keyvals to get the next keys and values."""
        return SDSKVIterator(self, after=after, prefix=prefix,
                items_per_request=keys_per_request,
                include_values=True,
                key_size=key_size, val_size=val_size)
        


class SDSKVIterator():
    """This SDSKVIterator is returned by SDSKVDatabase's keys() and items() methods
    to enable iteration over the database's entries."""

    def __init__(self, db, after='', prefix='',
            items_per_request=1, include_values=False,
            key_size=0, val_size=0):
        """
        Constructor. Should not be called by users.
        Users should call keys() or items() on the Database instance.
        """
        self._db = db
        self._after = after
        self._prefix = prefix
        self._items_per_request = items_per_request
        self._include_values = include_values
        self._needs_to_stop = False
        self._key_size = key_size
        self._val_size = val_size
        self._cache_keys = []
        self._cache_vals = []

    def __iter__(self):
        return self

    def __next__(self):
        if(len(self._cache_keys) == 0 and self._needs_to_stop):
            raise StopIteration
        if(len(self._cache_keys) == 0):
            if(self._include_values):
                self._cache_keys, self._cache_vals = self._db.list_keyvals(
                                    after=self._after,
                                    num_keys=self._items_per_request,
                                    prefix=self._prefix,
                                    key_size=self._key_size,
                                    val_size=self._val_size)
            else:
                self._cache_keys = self._db.list_keys(
                                    after=self._after,
                                    num_keys=self._items_per_request,
                                    prefix=self._prefix,
                                    key_size=self._key_size)
        if(len(self._cache_keys) == 0):
            raise StopIteration
        if(len(self._cache_vals) != self._items_per_request):
            self._needs_to_stop = True
        self._after = self._cache_keys[-1]
        if(self._include_values):
            return self._cache_keys.pop(0), self._cache_vals.pop(0)
        else:
            return self._cache_keys.pop(0)


Iterator = SDSKVIterator
Client = SDSKVClient
ProviderHandle = SDSKVProviderHandle
Database = SDSKVDatabase
