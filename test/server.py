# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
from pymargo import MargoInstance
import pysdskv.server
from pysdskv.server import SDSKVProvider

mid = MargoInstance('tcp')
mid.enable_remote_shutdown()

mplex_id = 42
print "Server running at address "+str(mid.addr())+" with mplex_id="+str(mplex_id)

provider = SDSKVProvider(mid, mplex_id)
dbid = provider.attach_database("mydatabase", "/tmp/sdskv", pysdskv.server.stdmap)
print "Created a database with id ", dbid

mid.wait_for_finalize()
