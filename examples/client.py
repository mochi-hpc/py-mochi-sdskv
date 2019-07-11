# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import sys
from pymargo.core import Engine
from pysdskv.client import *

engine = Engine('tcp')

server_addr = sys.argv[1]
provider_id = int(sys.argv[2])

client = SDSKVClient(engine)
addr = engine.lookup(server_addr)
provider_handle = client.create_provider_handle(addr, provider_id)

db = provider_handle.open("mydatabase")

db.put("matthieu", "mdorier@anl.gov")
val = db.get("matthieu")
print("db.get('matthieu') returned "+str(val))
val = db.get("phil")
print("db.get('phil') returned "+str(val))
e = db.exists("matthieu")
print("db.exists('matthieu') returned "+str(e))
e = db.exists("phil")
print("db.exists('phil') returned "+str(e))
db.erase("matthieu")
val = db.get("matthieu")
print("db.get('matthieu') returned "+str(val))

del db
del provider_handle
client.shutdown_service(addr)
del addr
del client
engine.finalize()
