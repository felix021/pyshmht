#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>

#include <Python.h>

#include "hashtable.h"

struct mapnode {
    int fd;
    size_t mem_size;
    hashtable *ht;
};

#define max_ht_map_entries 2048
static struct mapnode ht_map[max_ht_map_entries];
static int ht_idx = -1;

static PyObject * shmht_open(PyObject *self, PyObject *args);
static PyObject * shmht_close(PyObject *self, PyObject *args);
static PyObject * shmht_getval(PyObject *self, PyObject *args);
static PyObject * shmht_setval(PyObject *self, PyObject *args);
static PyObject * shmht_remove(PyObject *self, PyObject *args);
static PyObject * shmht_foreach(PyObject *self, PyObject *args);

static PyObject *shmht_error; 
PyMODINIT_FUNC initshmht(void);

static PyMethodDef shmht_methods[] = {
    {"open", shmht_open, METH_VARARGS, "create a shared memory hash table"},
    {"close", shmht_close, METH_VARARGS, ""},
    {"getval", shmht_getval, METH_VARARGS, ""},
    {"setval", shmht_setval, METH_VARARGS, ""},
    {"remove", shmht_remove, METH_VARARGS, ""},
    {"foreach", shmht_foreach, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initshmht(void)
{
    PyObject *m = Py_InitModule("shmht", shmht_methods);
    if (m == NULL)
        return;

    shmht_error = PyErr_NewException("shmht.error", NULL, NULL);
    Py_INCREF(shmht_error);
    PyModule_AddObject(m, "error", shmht_error);

    bzero(ht_map, sizeof(ht_map));
}

static PyObject * shmht_open(PyObject *self, PyObject *args)
{
    int fd = 0;
    size_t mem_size = 0;
    hashtable *ht = NULL;

    const char *name;
    size_t i_capacity = 0;
    int force_init = 0;
    if (!PyArg_ParseTuple(args, "s|ii:shmht.create", &name, &i_capacity, &force_init))
        return NULL;

    size_t capacity = i_capacity;

    fd = open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        PyErr_Format(shmht_error, "open file(%s) failed: [%d] %s", name, errno, strerror(errno));
        return NULL;
    }

    struct stat buf;
    fstat(fd, &buf);

    if (force_init == 0) { //try to load from existing shmht
        mem_size = sizeof(hashtable);
        if (buf.st_size >= sizeof(hashtable)) { //may be valid
            ht = mmap(NULL, sizeof(hashtable), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
            if (ht == MAP_FAILED) {
                PyErr_Format(shmht_error, "mmap failed, map_size=sizeof(hashtable)=%lu: [%d] %s",
                                            mem_size, errno, strerror(errno));
                goto create_failed;
            }

            if (ht_is_valid(ht)) {
                if (capacity != 0 && capacity != ht->orig_capacity) {
                    PyErr_Format(shmht_error, "please specify the 3rd arg(force_init=1) to overwrite an existing shmht");
                    goto create_failed;
                }
                capacity = ht->orig_capacity; //loaded capacity
            }
            munmap(ht, sizeof(hashtable));
            ht = NULL;
        }
    }

    if (capacity == 0) {
        PyErr_Format(shmht_error, "please specify 'capacity' when you try to create a shmht");
        goto create_failed;
    }

    mem_size = ht_memory_size(capacity);
    
    if (buf.st_size < mem_size) {
        if (lseek64(fd, mem_size - 1, SEEK_SET) == -1) {
            PyErr_Format(shmht_error, "lseek64 failed: [%d] %s", errno, strerror(errno));
            goto create_failed;
        }
        char t = 0;
        if (write(fd, &t, 1) == -1) {
            PyErr_Format(shmht_error, "write failed: [%d] %s", errno, strerror(errno));
            goto create_failed;
        }
    }

    ht = mmap(NULL, mem_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (ht == MAP_FAILED) {
        PyErr_Format(shmht_error, "mmap failed, mem_size=%lu: [%d] %s",
                                    mem_size, errno, strerror(errno));
        goto create_failed;
    }

    ht_init(ht, capacity, force_init);
    int count;
    for (count = 0; count < max_ht_map_entries; count++)
    {
        ht_idx = (ht_idx + 1) % max_ht_map_entries;
        count += 1;
        if (ht_map[ht_idx].ht == NULL)
            break;
    }
    if (count >= max_ht_map_entries) {
        PyErr_Format(shmht_error, "exceeded max_ht_map_entries(%d) in one process", max_ht_map_entries);
        goto create_failed;
    }
    ht_map[ht_idx].fd       = fd;
    ht_map[ht_idx].mem_size = mem_size;
    ht_map[ht_idx].ht       = ht;

    return PyInt_FromLong(ht_idx);

create_failed:
    if (fd >= 0)
        close(fd);
    if (ht != NULL)
        munmap(ht, mem_size);
    return NULL;
}

static PyObject * shmht_close(PyObject *self, PyObject *args)
{
    int idx;
    if (!PyArg_ParseTuple(args, "i:shmht.create", &idx))
        return NULL;

    if (idx < 0 || idx >= max_ht_map_entries || ht_map[idx].ht == NULL) {
        PyErr_Format(shmht_error, "invalid ht id: (%d)", idx);
        return NULL;
    }

    hashtable *ht = ht_map[idx].ht;

    size_t ref_cnt = ht_destroy(ht);

    if (munmap(ht, ht_map[idx].mem_size) != 0) {
        PyErr_Format(shmht_error, "munmap failed: [%d] %s", errno, strerror(errno));
        //return NULL;
    }

    if (ref_cnt == 0) {
        //TODO: is it necessary to delete the mapping file?
    }

    close(ht_map[idx].fd);

    memset(&ht_map[idx], 0, sizeof(struct mapnode));

    Py_RETURN_TRUE;
}

static PyObject * shmht_getval(PyObject *self, PyObject *args)
{
    int idx, key_size;
    const char *key;
    if (!PyArg_ParseTuple(args, "is#:shmht.getval", &idx, &key, &key_size))
        return NULL;

    if (idx < 0 || idx >= max_ht_map_entries || ht_map[idx].ht == NULL) {
        PyErr_Format(shmht_error, "invalid ht id: (%d)", idx);
        return NULL;
    }

    hashtable *ht = ht_map[idx].ht;

    ht_str* value = ht_get(ht, key, key_size);
    if (value == NULL) {
        Py_RETURN_NONE;
    }
    return PyString_FromStringAndSize(value->str, value->size);
}

static PyObject * shmht_setval(PyObject *self, PyObject *args)
{
    int idx, key_size, value_size;
    const char *key, *value;
    if (!PyArg_ParseTuple(args, "is#s#:shmht.setval", &idx, &key, &key_size, &value, &value_size)) {
        return NULL;
    }

    if (idx < 0 || idx >= max_ht_map_entries || ht_map[idx].ht == NULL) {
        PyErr_Format(shmht_error, "invalid ht id: (%d)", idx);
        return NULL;
    }

    hashtable *ht = ht_map[idx].ht;

    if (ht_set(ht, key, key_size, value, value_size) == False) {
        PyErr_Format(shmht_error, "insert failed for key(%s)", key);
        return NULL;
    }

    Py_RETURN_TRUE;
}

static PyObject * shmht_remove(PyObject *self, PyObject *args)
{
    int idx, key_size;
    const char *key;
    if (!PyArg_ParseTuple(args, "is#:shmht.remove", &idx, &key, &key_size))
        return NULL;

    if (idx < 0 || idx >= max_ht_map_entries || ht_map[idx].ht == NULL) {
        PyErr_Format(shmht_error, "invalid ht id: (%d)", idx);
        return NULL;
    }

    hashtable *ht = ht_map[idx].ht;
    if (ht_remove(ht, key, key_size) == False) {
        Py_RETURN_FALSE;
    }

    Py_RETURN_TRUE;
}

static PyObject * shmht_foreach(PyObject *self, PyObject *args)
{
    int idx;
    static PyObject *cb = NULL;

    if (!PyArg_ParseTuple(args, "iO:shmht.foreach", &idx, &cb))
        return NULL;

    if (idx < 0 || idx >= max_ht_map_entries || ht_map[idx].ht == NULL) {
        PyErr_Format(shmht_error, "invalid ht id: (%d)", idx);
        return NULL;
    }

    if (!PyCallable_Check(cb)) {
        PyErr_SetString(PyExc_TypeError, "parameter must be callable");
        return NULL;
    }

    hashtable *ht = ht_map[idx].ht;
    ht_iter *iter = ht_get_iterator(ht);
    while (ht_iter_next(iter)) {
        ht_str *key = iter->key, *value = iter->value;
        PyObject *arglist = Py_BuildValue("(s#s#)", key->str, key->size, value->str, value->size);
        PyEval_CallObject(cb, arglist);
        Py_DECREF(arglist);
    }
    free(iter);

    Py_RETURN_NONE;
}

