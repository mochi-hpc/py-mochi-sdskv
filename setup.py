from distutils.core import setup
from distutils.extension import Extension
from distutils.sysconfig import get_config_vars
import pkgconfig
import os
import os.path
import sys

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(
		    flag for flag in opt.split() if flag != '-Wstrict-prototypes'
		)

pk = pkgconfig.parse('sdskv-server')
server_libraries = pk['libraries']
server_library_dirs = pk['library_dirs']
server_library_dirs = pk['library_dirs']
server_include_dirs = pk['include_dirs']
server_include_dirs.append(".")

pysdskv_server_module = Extension('_pysdskvserver', ["pysdskv/src/server.cpp"],
		           libraries=server_libraries,
                   library_dirs=server_library_dirs,
                   include_dirs=server_include_dirs,
                   depends=["pysdskv/src/server.cpp"])

pk = pkgconfig.parse('sdskv-client')
client_libraries = pk['libraries']
client_library_dirs = pk['library_dirs']
client_library_dirs = pk['library_dirs']
client_include_dirs = pk['include_dirs']
client_include_dirs.append(".")

pysdskv_client_module = Extension('_pysdskvclient', ["pysdskv/src/client.cpp"],
		           libraries=client_libraries,
                   library_dirs=client_library_dirs,
                   include_dirs=client_include_dirs,
                   depends=["pysdskv/src/client.cpp"])
setup(name='pysdskv',
      version='0.1',
      author='Matthieu Dorier',
      description="""Python binding for SDSKV""",      
      ext_modules=[ pysdskv_server_module, pysdskv_client_module ],
      packages=['pysdskv']
     )
