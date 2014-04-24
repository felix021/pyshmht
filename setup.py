#!/usr/bin/python
import os
from distutils.core import setup, Extension

#os.putenv("CFLAGS", "-g")

shmht = Extension('shmht',
        sources = ['shmht.c', 'hashtable.c']
)

setup(
    name            = 'pyshmht',
    version         = '0.0.2',
    author          = 'felix021',
    author_email    = 'felix021@gmail.com',
    description     = 'provide sharing memory based hash table for python',
    license         = "BSD",
    keywords        = "python extension sharing memory based hash table",
    url             = "http://github.com/felix021/pyshmht",
    ext_modules     = [shmht],
    packages        = ["pyshmht"]
)
