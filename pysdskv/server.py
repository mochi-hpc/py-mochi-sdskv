import _pysdskvserver
import pymargo

class SDSKVProvider(pymargo.Provider):

	def __init__(self, mid, mplex_id):
		super(SDSKVProvider, self).__init__(mid, mplex_id)
		self._provider = _pysdskvserver.register(mid._mid, mplex_id)

