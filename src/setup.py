from setuptools import setup, Extension
import pybind11
import numpy as np

recdb = Extension(
    name="recdb",
    sources=["recdb.cpp", "evbuf.cpp"],  
    include_dirs=[np.get_include(), "/path/to/rec-lsm/include", "/path/to/rec-lsm", pybind11.get_include()], 
    libraries=["rocksdb"], 
    library_dirs=["/usr/local/lib"],  
    extra_compile_args=["-g", "-std=c++17", "-O0"],
)

setup(
    name="recdb",
    version="1.0",
    ext_modules=[recdb],
)
