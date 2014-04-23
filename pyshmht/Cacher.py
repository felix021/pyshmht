#!/usr/bin/python
#coding: utf-8

import marshal
import HashTable

_debug = False

class Cacher(object):
    """
    Cacher: wrap HashTable with serializer and write_through/write_back mechanism
        if you intend to modify the cache, initialize Cacher with `write_through=True`,
        [OR] call write_back() before the program exits

        notice:
            Cacher tries to simulate dict in most cases, mainly except for:
                (a) no __iter__, please use foreach instead
                (b) key should always be a str, where dict allows all hashable objects 
                (c) no comparation with other 'dict's
            When necessary, you can use .to_dict() to get a real dict object.
    """
    def __init__(self, name, capacity=0, force_init=False, serializer=marshal, write_through=False):
        """
        'name'          the path of the file to be 'mmap'ed
                        use MemCacher(name, ...) to add prefix '/dev/shm' automatically
        'capacity'      optional, if you want to connect to an existing shmht
        'serializer'    should contain loads/dumps (marshal, json, pickle, etc.)
        'write_through' True means that every write goes back to shmht immediately
        """

        self.ht = HashTable.HashTable(name, capacity, force_init, serializer)
        self.d = {}
        self.loads = serializer.loads
        self.dumps = serializer.dumps

        self.write_through = write_through
        if write_through:
            self.__setitem = self.__setitem_write_through
        else:
            self.__setitem = self.__setitem_simple
            self.need_write_back = False

    def __getitem__(self, key):
        d = self.d
        if key in d:
            val = d[key]
        else:
            val = self.loads(self.ht[key])
            d[key] = val
        return val

    def __setitem__(self, key, val):
        self.__setitem(key, val)

    def __setitem_simple(self, key, val):
        self.d[key] = val
        self.need_write_back = True

    def __setitem_write_through(self, key, val):
        self.d[key] = val
        self.ht.setobj(key, val)

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
        if self.write_through:
            self.ht.update(dic, serialize=True)
        else:
            self.need_write_back = True

    def foreach(self, callback):
        if not self.write_through:
            self.write_back()
        return self.ht.foreach(callback, unserialize=True)

    def to_dict(self):
        if not self.write_through:
            self.write_back()
        return self.ht.to_dict(unserialize=True)

    def write_back(self):
        if self.need_write_back:
            self.ht.update(self.d, serialize=True)
            self.need_write_back = False

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

def MemCacher(name, capacity=0, force_init=False, serializer=marshal, write_through=False):
    """
    Add an prefix '/dev/shm/' to `name`, so that the file is saved only in memory
    For more information, see `help(Cacher)`
    """
    name = '/dev/shm/' + name
    return Cacher(name, capacity, force_init, serializer, write_through)

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
    ht = MemCacher('test.Cacher', 1024, True, write_through=False)
    print 'fd:', ht.ht.fd
    ht['a'] = 1
    ht.write_back()
    ht['b'] = 2

    _debug = True
    ht.close() #write_back() is called in close() when not debugging

    #write_through
    ht = MemCacher('test.Cacher', 1024, False, write_through=True)
    print 'fd:', ht.ht.fd
    print ht['a'] == 1
    try:
        print ht['b']
        print False
    except:
        print True
    ht['a'] = 3
    ht['b'] = 4
    ht.close()

    ht = MemCacher('test.Cacher', 1024, False, write_through=True)
    print ht['b'] == 4
    ht.close()

    #simple performance test
    import time

    capacity = 500000

    #write_through
    ht = MemCacher('test.Cacher', capacity, True, write_through=True)

    begin_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s] = s
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ set/write_through'

    begin_timend_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s]
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ get/write_through'

    ht.close()

    #write_back
    ht = MemCacher('test.Cacher', capacity, True, write_through=False)
    begin_timend_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s] = s
    ht.write_back()
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ set/write_back'

    begin_timend_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s]
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ get/write_back'

    ht.close()
