# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import sys
sys.path.append('build/lib.linux-x86_64-3.7')
from pymargo.core import Engine
import pysdskv.server
from pysdskv.server import SDSKVProvider

engine = Engine('ofi+tcp')
engine.enable_remote_shutdown()

provider_id = 42
print("Server running at address "+str(engine.addr())+" with provider_id="+str(provider_id))

provider = SDSKVProvider(engine, provider_id)
dbid = provider.attach_database("mydatabase", "/tmp/sdskv", pysdskv.server.leveldb)
print("Created a database with id "+str(dbid))

engine.wait_for_finalize()
