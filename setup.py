
from distutils.core import setup, Extension, DEBUG
 
# from setuptools import find_packages, setup, Command

sfc_module = Extension('buffered_pipe.module', sources = ['cpp/module.cpp'])

setup(name = 'buffered_pipe', version = '0.0.0',
    description = 'Buffered pipe through shared memory.',
    packages=['buffered_pipe'],
    ext_modules = [sfc_module]
)
