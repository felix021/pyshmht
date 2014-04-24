#!/usr/bin/python
#coding: utf-8

import shmht
import marshal

#basic wrapper: open, close, get, set, remove, foreach
#extended wrapper: getobj, setobj, [], to_dict, update

class HashTable(object):
    """
    Basic wrapper for shmht. For more information, see 'help(Cacher)'
    """
    def __init__(self, name, capacity=0, force_init=False, serializer=marshal):
        force_init = 1 if force_init else 0
        self.fd = shmht.open(name, capacity, force_init)
        self.loads = serializer.loads
        self.dumps = serializer.dumps

    def close(self):
        shmht.close(self.fd)

    def get(self, key, default=None):
        val = shmht.getval(self.fd, key)
        if val == None:
            return default
        return val

    def set(self, key, value):
        return shmht.setval(self.fd, key, value)

    def remove(self, key):
        return shmht.remove(self.fd, key)

    def foreach(self, callback, unserialize=False):
        if not unserialize:
            cb = callback
        else:
            loads = self.loads
            def mcb(key, value):
                return callback(key, loads(value))
            cb = mcb
        return shmht.foreach(self.fd, cb)

    def getobj(self, key, default=None):
        val = self.get(key, default)
        if val == default:
            return default
        return self.loads(val)

    def setobj(self, key, val):
        val = self.dumps(val)
        return self.set(key, val)

    def __getitem__(self, key):
        val = shmht.getval(self.fd, key)
        if val == None:
            raise KeyError(key)
        return val

    def __setitem__(self, key, value):
        return shmht.setval(self.fd, key, value)

    def __delitem__(self, key):
        if False == shmht.remove(self.fd, key):
            raise KeyError(key)

    def __contains__(self, key):
        return shmht.getval(self.fd, key) != None

    def to_dict(self, unserialize=False):
        d = {}
        def insert(k,v):
            d[k] = v
        self.foreach(insert, unserialize)
        return d

    def update(self, d, serialize=False):
        dumps = self.dumps
        if serialize:
            for k in d:
                self[k] = dumps(d[k])
        else:
            for k in d:
                self[k] = d[k]

if __name__ == "__main__":
    loads = marshal.loads
    dumps = marshal.dumps
    #test cases
    ht = HashTable('/dev/shm/test.HashTable', 1024, 1)

    #set
    ht['a'] = '1'
    ht.set('b', '2')
    c = {'hello': 'world'}
    ht.setobj('c', c)

    #get
    print ht['b'] == '2'
    print ht['c'] == marshal.dumps(c)
    print ht.getobj('c') == c
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
    ht.setobj('c', c)
    print ht.to_dict() == {'a': '1', 'b': '2', 'c': dumps(c)}

    s = ''
    def cb(key, value):
        global s
        s += key + str(value)
    ht.foreach(cb)
    print s == 'a1b2c' + dumps(c)

    ht.update({'a': 1, 'b': 2}, serialize=True)

    s = ''
    ht.foreach(cb, unserialize=True)
    print s == 'a1b2c' + str(c)

    print ht.to_dict() == {'a':dumps(1), 'b':dumps(2), 'c':dumps(c)}
    print ht.to_dict(unserialize=True) == {'a': 1, 'b': 2, 'c': c}

    #close
    ht.close()
    try:
        ht['a']
        print False
    except:
        print True

    #simple performance test
    import time

    capacity = 300000

    #write_through
    ht = HashTable('/dev/shm/test.HashTable', capacity, True)

    begin_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        ht[s] = s
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ set'

    begin_timend_time = time.time()
    for i in range(capacity):
        s = '%064d' % i
        if s != ht[s]:
            raise Exception(s)
    end_time = time.time()
    print capacity / (end_time - begin_time), 'iops @ get'

    ht.close()

