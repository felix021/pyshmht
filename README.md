pyshmht
=======

**Sharing memory based** Hash Table extension for Python

For examples, see test cases in python files (pyshmht/Cacher.py, pyshmht/HashTable.py), where you can find performance tests as well.

Performance
===========

capacity=200M, 64 bytes key/value tests, tested on (Xeon E5-2670 0 @ 2.60GHz, 128GB ram)

* hashtable.c (raw hash table in c, tested on `malloc`ed memory)
    set: 0.93 Million iops
    get: 2.35 Million iops

* HashTable.py (simple wrapper)
    set: 250k iops
    get: 145k iops

* Cacher.py (cached wrapper, with serialized)
    set: 180k iops (write_through), 83k iops (writ_back)
    get: 135k iops (write_through), 73k iops (writ_back)

Notice
======

In hashtable.c, default max key length is `256 - 4`, max value length is `1024 - 4`; you can change `bucket_size` and `max_key_size` manually, but bear in mind that increasing these two arguments will result in larger memory consumption.

If you find any bugs, please submit an issue or send me a pull request, I'll see to it ASAP :)

p.s. `hashtable.c` is independent (i.e. has nothing to do with python), you can use it in other projects if needed. :P
