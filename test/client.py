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

del provider_handle
client.shutdown_service(addr)
del addr
client.finalize()
mid.finalize()
