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

server_libs = ['boost_python','margo']
server_libs += pkgconfig.parse('sdskv-server')['libraries']
pysdskv_server_module = Extension('_pysdskvserver', ["pysdskv/src/server.cpp"],
		           libraries=server_libs,
			   include_dirs=['.'],
			   depends=[])

pysdskv_client_module = Extension('_pysdskvclient', ["pysdskv/src/client.cpp"],
		           libraries=['boost_python','margo','sdskv-client'],
			   include_dirs=['.'],
			   depends=[])
setup(name='pysdskv',
      version='0.1',
      author='Matthieu Dorier',
      description="""Python binding for SDSKV""",      
      ext_modules=[ pysdskv_server_module, pysdskv_client_module ],
      packages=['pysdskv']
     )
