#!/usr/bin/python
#coding: utf-8

import marshal
import HashTable

_debug = False

class Cacher(object):
    """
    Cacher: wrap HashTable with serializer and write_back mechanism
        if you intend to modify the cache, call write_back() before the program exits

        notice:
            Cacher tries to simulate dict in most cases, mainly except for:
                (a) no __iter__, please use foreach instead
                (b) key should always be a str, where dict allows all hashable objects 
                (c) no comparation with other 'dict's
            When necessary, you can use .to_dict() to get a real dict object.
    """
    def __init__(self, name, capacity=0, force_init=False, serializer=marshal):
        """
        'name'          the path of the file to be 'mmap'ed
                        use MemCacher(name, ...) to add prefix '/dev/shm' automatically
        'capacity'      optional, if you want to connect to an existing shmht
        'serializer'    should contain loads/dumps (marshal, json, pickle, etc.)
        """

        self.ht = HashTable.HashTable(name, capacity, force_init, serializer)
        self.d = {}
        self.loads = serializer.loads
        self.dumps = serializer.dumps

    def __getitem__(self, key):
        d = self.d
        if key in d:
            val = d[key]
        else:
            val = self.loads(self.ht[key])
            d[key] = val
        return val

    def __setitem__(self, key, val):
        self.d[key] = val

    def __delitem__(self, key):
        if key in self.d:
            del self.d[key]
            try:
                del self.ht[key]
            except:
                pass
        else:
            del self.d[key]

    def __contains__(self, key): #notice: key will be cached here
        return self.get(key) != None

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except:
            return default

    def update(self, dic):
        self.d.update(dic)

    def foreach(self, callback):
        self.write_back()
        return self.ht.foreach(callback, unserialize=True)

    def to_dict(self):
        self.write_back()
        return self.ht.to_dict(unserialize=True)

    def write_back(self):
        self.ht.update(self.d, serialize=True)

    def close(self):
        if self.d:
            global _debug
            if not _debug:
                self.write_back() #commented out for testing
            del self.d
            self.d = None
        if self.ht:
            self.ht.close()
            self.ht = None

    def __del__(self):
        """
        don't rely on this, please call write_back() manually if necessary.
        """
        self.close()

def MemCacher(name, capacity=0, force_init=False, serializer=marshal):
    """
    Add an prefix '/dev/shm/' to `name`, so that the file is saved only in memory
    For more information, see `help(Cacher)`
    """
    name = '/dev/shm/' + name
    return Cacher(name, capacity, force_init, serializer)

if __name__ == "__main__":
    #test cases
    ht = MemCacher('test.Cacher', 1024, True)
    print 'fd:', ht.ht.fd

    #set
    ht['a'] = '1'
    ht['b'] = 2
    c = {'hello': 'world'}
    ht['c'] = c

    #get
    print ht['b'] == 2
    print ht['c'] == c
    print ht.get('c') == c
    print ht.get('d') == None
    try:
        ht['d']
        print False
    except:
        print True

    #contains
    print ('c' in ht) == True
    print ('d' in ht) == False

    #del
    del ht['c']
    print ht.get('c') == None
    try:
        del ht['d']
        print 'del:', False
    except:
        print True

    #update & to_dict & foreach
    dumps = marshal.dumps
    ht['c'] = c
    print ht.to_dict() == {'a': '1', 'b': 2, 'c': c}

    def cb(key, value):
        global s
        s += key + str(value)

    s = ''
    ht.foreach(cb)
    print s == 'a1b2c' + str(c)

    ht.update({'a': 'x', 'b': 1000})

    s = ''
    ht.foreach(cb)
    print s == 'axb1000c' + str(c)

    print ht.to_dict() == {'a': 'x', 'b': 1000, 'c': c}

    #close
    ht.close()
    try:
        ht['a']
        print False
    except:
        print True

    #write_back
    ht = MemCacher('test.Cacher', 1024, True)
    print 'fd:', ht.ht.fd
    ht['a'] = 1
    ht.write_back()
    ht['b'] = 2

    _debug = True
    ht.close() #write_back() is called in close() when not debugging

    ht = MemCacher('test.Cacher', 1024, False)
    print 'fd:', ht.ht.fd
    print ht['a'] == 1
    try:
        print ht['b']
        print False
    except:
        print True
    ht.close()

    #simple performance test
    import time

    capacity = 300000

    ht = MemCacher('test.Cacher', capacity, force_init=True)
    begin_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s] = s
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ set / no write_back '

    ht.write_back()
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ set / after write_back '

    ht.d = {}
    begin_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        if s != ht[s]:
            raise Exception(s)
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ get / no cache '

    begin_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        if s != ht[s]:
            raise Exception(s)
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ get / all cached '

    ht.close()
