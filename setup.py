from setuptools import setup

setup(
    name='multitables',
    version='2.0.1',
    url='https://github.com/ghcollin/multitables',
    description='High performance parallel reading of HDF5 files using PyTables, multiprocessing, and shared memory.',
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
    keywords='tables hdf5 parallel concurrent',
    license='MIT',
    author='ghcollin',
    author_email='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    packages=['multitables'],
    install_requires=['numpy', 'tables', 'msgpack >= 0.6.0', 'wrapt']
)
