# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import sys
sys.path.append('build/lib.linux-x86_64-3.7')
from pymargo.core import Engine
from pysdskv.client import *

engine = Engine('ofi+tcp')

server_addr = sys.argv[1]
provider_id = int(sys.argv[2])

client = SDSKVClient(engine)
addr = engine.lookup(server_addr)
provider_handle = client.create_provider_handle(addr, provider_id)

db = provider_handle.open("mydatabase")

db.put("matthieu", "mdorier@anl.gov")
val = db.get("matthieu")
print("db.get('matthieu') returned "+str(val))
try:
    val = db.get("phil")
    print("db.get('phil') returned "+str(val))
except KeyError:
    print("Correctly throws an exception when trying to lookup 'phil'")
e = db.exists("matthieu")
print("db.exists('matthieu') returned "+str(e))
e = db.exists("phil")
print("db.exists('phil') returned "+str(e))
db.erase("matthieu")
try:
    val = db.get("matthieu")
    print("db.get('matthieu') returned "+str(val))
except KeyError:
    print("Correctly throws an exception when trying to lookup 'matthieu'")

del db
del provider_handle
client.shutdown_service(addr)
del addr
del client
engine.finalize()
