# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import sys
from pymargo import MargoInstance
from pysdskv.client import *

mid = MargoInstance('tcp')

server_addr = sys.argv[1]
mplex_id    = int(sys.argv[2])

client = SDSKVClient(mid)
addr = mid.lookup(server_addr)
provider_handle = client.create_provider_handle(addr, mplex_id)

# do something with the provider handle
db = provider_handle.open("mydatabase")
if(db != None):
    db.put("matthieu", "mdorier@anl.gov")
    val = db.get("matthieu")
    print "db.get('matthieu') returned "+str(val)
    val = db.get("phil")
    print "db.get('phil') returned "+str(val)
    e = db.exists("matthieu")
    print "db.exists('matthieu') returned "+str(e)
    e = db.exists("phil")
    print "db.exists('phil') returned "+str(e)
    db.erase("matthieu")
    val = db.get("matthieu")
    print "db.get('matthieu') returned "+str(val)
else:
    print "Could not open Database"

del db
del provider_handle
client.shutdown_service(addr)
del addr
client.finalize()
mid.finalize()
