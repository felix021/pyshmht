pyshmht
=======

**Sharing memory based** Hash Table extension for Python

For examples, see test cases in python files (pyshmht/Cacher.py, pyshmht/HashTable.py), where you can find performance tests as well.

Performance
===========

capacity=200M, 64 bytes key/value tests, tested on (Xeon E5-2670 0 @ 2.60GHz, 128GB ram)

* hashtable.c (raw hash table in c, tested on `malloc`ed memory)
> set: 0.93 Million iops; 
> get: 2.35 Million iops;

* performance\_test.py (raw python binding)
> set: 451k iops; 
> get: 272k iops;

* HashTable.py (simple wrapper, no serialization)
> set: 354k iops; 
> get: 202k iops;

* Cacher.py (cached wrapper, with serialization)
> set: 358k iops (cached), 198k iops (after write\_back); 
> get: 560k iops (cached), 238k iops (no cache);

* python native dict
> set: 741k iops; 
> get: 390k iops;

Notice
======

In hashtable.c, default max key length is `256 - 4`, max value length is `1024 - 4`; you can change `bucket_size` and `max_key_size` manually, but bear in mind that increasing these two arguments will result in larger memory consumption.

If you find any bugs, please submit an issue or send me a pull request, I'll see to it ASAP :)

p.s. `hashtable.c` is independent (i.e. has nothing to do with python), you can use it in other projects if needed. :P
