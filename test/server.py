# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
from pymargo import MargoInstance
from pysdskv.server import SDSKVProvider

mid = MargoInstance('tcp')
mid.enable_remote_shutdown()

mplex_id = 42
print "Server running at address "+str(mid.addr())+"with mplex_id="+str(mplex_id)

provider = SDSKVProvider(mid, mplex_id)

mid.wait_for_finalize()
