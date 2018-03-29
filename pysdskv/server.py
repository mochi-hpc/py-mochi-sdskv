# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pysdskvserver
import pymargo

class SDSKVProvider(pymargo.Provider):

	def __init__(self, mid, provider_id):
		super(SDSKVProvider, self).__init__(mid, provider_id)
		self._provider = _pysdskvserver.register(mid._mid, provider_id)

