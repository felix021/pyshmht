#!/usr/bin/python
#coding: utf-8

import shmht
import time

capacity = 300000

fd = shmht.open('/dev/shm/test.performance', capacity, 1)

begin_time = time.time()
for i in range(capacity):
    s = '%064d' % i
    shmht.setval(fd, s, s)
end_time = time.time()
print capacity / (end_time - begin_time), 'iops @ set'

begin_timend_time = time.time()
for i in range(capacity):
    s = '%064d' % i
    if s != shmht.getval(fd, s):
        raise Exception(s)
end_time = time.time()
print capacity / (end_time - begin_time), 'iops @ get'

shmht.close(fd)
