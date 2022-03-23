#pragma GCC optimize("O2")

#include <Python.h>

#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <vector>
#include <deque>
#include <cstring>

const int LENGTH_INFO = 4;
typedef long long OP_TYPE;
typedef long long BASE_TYPE;
typedef long long PTR_LOAD_TYPE;
typedef uintptr_t PTR_TYPE;

#define PTR2LL(x) (long long)(uintptr_t)(x)
#define LL2PTR(x) (void *)(uintptr_t)(x)

// create_sem, delete_sem, attach_sem, detach_sem
// create_shm, delete_shm, attach_shm, detach_shm
// send_bytes, recv_bytes

PyObject* get_id(PyObject *) {
    return PyLong_FromUnsignedLongLong(getpid());
}

PyObject* create_sem(PyObject *, PyObject* args) {
    char *sem_name;
    PyArg_ParseTuple(args, "s", &sem_name);
    sem_t *sem = sem_open(sem_name, O_CREAT | O_EXCL, 0644, 0);
    if(!sem) sem_close(sem);
    return PyLong_FromLongLong(sem == SEM_FAILED);
}

PyObject* delete_sem(PyObject *, PyObject* args) {
    char *sem_name;
    BASE_TYPE result;
    PyArg_ParseTuple(args, "s", &sem_name);
    result = sem_unlink(sem_name);
    return PyLong_FromLongLong(result);
}

PyObject* attach_sem(PyObject *, PyObject* args) {
    char *sem_name;
    PyArg_ParseTuple(args, "s", &sem_name);
    sem_t *sem = sem_open(sem_name, O_CREAT, 0644, 0);
    return PyLong_FromLongLong(PTR2LL(sem));
}

PyObject* detach_sem(PyObject *, PyObject* args) {
    PTR_LOAD_TYPE sem_ptr;
    BASE_TYPE result;
    PyArg_ParseTuple(args, "L", &sem_ptr);
    result = sem_close((sem_t*)LL2PTR(sem_ptr));
    return PyLong_FromLongLong(result);
}

PyObject* create_shm(PyObject *, PyObject* args) {
    BASE_TYPE shm_size, shm_id;
    PyArg_ParseTuple(args, "L", &shm_size);
	shm_id = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0644);
    return PyLong_FromLongLong(shm_id);
}

PyObject* delete_shm(PyObject *, PyObject* args) {
    BASE_TYPE shm_id, result;
    PyArg_ParseTuple(args, "L", &shm_id);
    result = shmctl(shm_id , IPC_RMID , NULL);
    return PyLong_FromLongLong(result);
}

PyObject* attach_shm(PyObject *, PyObject* args) {
    PTR_LOAD_TYPE shmid;
    BASE_TYPE shmaddr;
    PyArg_ParseTuple(args, "L", &shmid);
    shmaddr = PTR2LL(shmat(shmid, NULL, 0));
    return PyLong_FromLongLong(shmaddr);
}

PyObject* detach_shm(PyObject *, PyObject* args) {
    PTR_LOAD_TYPE shmaddr;
    BASE_TYPE result;
    PyArg_ParseTuple(args, "L", &shmaddr);
    result = shmdt(LL2PTR(shmaddr));
    return PyLong_FromLongLong(result);
}

BASE_TYPE read_request(char* shm_buf, OP_TYPE shm_size, OP_TYPE pointer, OP_TYPE length, std::deque<char> &result) {
    if(shm_size < pointer + length) {
        for(char *lookup = shm_buf + pointer, *end = shm_buf + shm_size; lookup != end;)
            result.push_back(*lookup++);
        length -= shm_size - pointer;
        pointer = 0;
    }
    for(char *lookup = shm_buf + pointer, *end = shm_buf + pointer + length; lookup != end;)
        result.push_back(*lookup++);
    return pointer + length;
}

BASE_TYPE read_request(char* shm_buf, OP_TYPE shm_size, OP_TYPE pointer, OP_TYPE length, char*result) {
    OP_TYPE lookup = 0;
    if(shm_size < pointer + length) {
        lookup = shm_size - pointer;
        memmove(result, shm_buf + pointer, lookup);
        length -= lookup;
        pointer = 0;
    }
    memmove(result + lookup, shm_buf + pointer, length);
    return pointer + length;
}

BASE_TYPE write_request(char* shm_buf, OP_TYPE shm_size, OP_TYPE pointer, char* data, OP_TYPE length) {
    OP_TYPE tmp;
    if(shm_size < pointer + length) {
        tmp = shm_size - pointer;
        memmove(shm_buf+pointer, data, tmp);
        data += tmp;
        length -= tmp;
        pointer = 0;
    }
    memmove(shm_buf+pointer, data, length);
    return pointer + length;
}


union int2char {
    char c[8];
    long long v;
};
// BASE_TYPE charsToInt(char *data) {
//     int2char i2c_value;
//     memmove(i2c_value.c, data, LENGTH_INFO);
//     return i2c_value.v;
// }
// char* IntTochars(BASE_TYPE data) {
//     int2char i2c_value;
//     i2c_value.v = data;
//     return i2c_value.c;
// }

BASE_TYPE chunk_size(char *shm_buf, OP_TYPE shm_size, OP_TYPE pointer) {
    int2char i2c_value, i2c_cast;
    i2c_value.v = 0;
    read_request(shm_buf, shm_size, pointer, LENGTH_INFO, i2c_value.c);
    
    if(i2c_value.c[LENGTH_INFO - 1] < 0) {
        i2c_cast.v = -1;
        memmove(i2c_cast.c, i2c_value.c, LENGTH_INFO);
        return i2c_cast.v;
    }
    return i2c_value.v;
}
BASE_TYPE abs_chunk_size(char *shm_buf, OP_TYPE shm_size, OP_TYPE pointer) {
    return std::abs(chunk_size(shm_buf, shm_size, pointer));
}
BASE_TYPE write_int(char *shm_buf, OP_TYPE shm_size, OP_TYPE pointer, OP_TYPE value) {
    int2char i2c_value;
    i2c_value.v = value;
    return write_request(shm_buf, shm_size, pointer, i2c_value.c, LENGTH_INFO);
}
void printf_hex(char *shm_buf, BASE_TYPE shm_size) {
    for (int i = 0; i < shm_size; i++)
        printf("%02x ", (unsigned char)shm_buf[i]);
    printf("\n");
}
PyObject* send_bytes(PyObject *, PyObject *args) {
    // shm_buf, shm_size, sem_a, sem_f, minimum_write, pointer_a, pointer_f, margin, data
    // return pointer_a, pointer_f, margin
    char*      req_data;
    Py_ssize_t req_len = 0;
    PTR_LOAD_TYPE _shm_buf, _sem_a, _sem_f;
    BASE_TYPE shm_size, minimum_write, pointer_a, pointer_f, margin;
    PyArg_ParseTuple(args, "LLLLLLLLy#", &_shm_buf, &shm_size, &_sem_a, &_sem_f, &minimum_write, &pointer_a, &pointer_f, &margin, &req_data, &req_len);
    char* shm_buf = (char*)LL2PTR(_shm_buf);
    sem_t *sem_a = (sem_t*)LL2PTR(_sem_a), *sem_f = (sem_t*)LL2PTR(_sem_f);
    
    OP_TYPE critical_length, write_length, free_length, loop_end_condition;
    while(req_len) {
        critical_length = req_len + LENGTH_INFO;
        while(margin < critical_length && !sem_trywait(sem_f)) {
            free_length = LENGTH_INFO + abs_chunk_size(shm_buf, shm_size, pointer_f);
            margin += free_length;
            pointer_f = (pointer_f + free_length) % shm_size;
        }
        loop_end_condition = std::min((OP_TYPE)minimum_write, critical_length);
        while(margin < loop_end_condition) {
            Py_BEGIN_ALLOW_THREADS 
            sem_wait(sem_f);
            Py_END_ALLOW_THREADS 
            free_length = LENGTH_INFO + abs_chunk_size(shm_buf, shm_size, pointer_f);
            margin += free_length;
            pointer_f = (pointer_f + free_length) % shm_size;
        }
        
        write_length = std::min((OP_TYPE)margin, critical_length);
        margin -= write_length;
        write_length -= LENGTH_INFO;
        if( write_length < req_len )
            pointer_a = write_int(shm_buf, shm_size, pointer_a, -write_length);
        else
            pointer_a = write_int(shm_buf, shm_size, pointer_a, write_length);
        pointer_a = write_request(shm_buf, shm_size, pointer_a, req_data, write_length);
        req_data += write_length;
        req_len -= write_length;
        sem_post(sem_a);
    }
    return Py_BuildValue("LLL", pointer_a, pointer_f, margin);
}
PyObject* recv_bytes(PyObject *, PyObject* args) {
    // shm_buf, shm_size, sem_a, sem_f, pointer
    // return pointer, data
    PTR_LOAD_TYPE _shm_buf, _sem_a, _sem_f;
    BASE_TYPE shm_size, pointer, load_max = 4096;
    PyArg_ParseTuple(args, "LLLLL", &_shm_buf, &shm_size, &_sem_a, &_sem_f, &pointer);
    char* shm_buf = (char*)LL2PTR(_shm_buf);
    sem_t *sem_a = (sem_t*)LL2PTR(_sem_a), *sem_f = (sem_t*)LL2PTR(_sem_f);
    char* buffer = new char[load_max];

    int FLAG = true;
    PyObject *result = PyList_New(0);
    BASE_TYPE length, remain, chunk;
    while(FLAG) {
        if(sem_trywait(sem_a)) {
            Py_BEGIN_ALLOW_THREADS 
            sem_wait(sem_a);
            Py_END_ALLOW_THREADS 
        }

        length = chunk_size(shm_buf, shm_size, pointer);
        pointer = (pointer + LENGTH_INFO) % shm_size;
        remain = std::abs(length);
        FLAG = length < 0;
        
        while(remain) {
            chunk = std::min(load_max, remain);
            remain -= chunk;
            pointer = read_request(shm_buf, shm_size, pointer, chunk, buffer);
            PyList_Append(result, PyBytes_FromStringAndSize(buffer, chunk));
        }

        sem_post(sem_f);
    }
    delete buffer;
    return Py_BuildValue("LO", pointer, result);
}
static PyMethodDef methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "create_sem", (PyCFunction)create_sem, METH_O, nullptr },
    { "delete_sem", (PyCFunction)delete_sem, METH_O, nullptr },
    { "attach_sem", (PyCFunction)attach_sem, METH_O, nullptr },
    { "detach_sem", (PyCFunction)detach_sem, METH_O, nullptr },
    { "create_shm", (PyCFunction)create_shm, METH_O, nullptr },
    { "delete_shm", (PyCFunction)delete_shm, METH_O, nullptr },
    { "attach_shm", (PyCFunction)attach_shm, METH_O, nullptr },
    { "detach_shm", (PyCFunction)detach_shm, METH_O, nullptr },
    { "send_bytes", (PyCFunction)send_bytes, METH_O, nullptr },
    { "recv_bytes", (PyCFunction)recv_bytes, METH_O, nullptr },
    { "get_id", (PyCFunction)get_id, METH_NOARGS, nullptr },
 
    // Terminate the array with an object containing nulls.
    { nullptr, nullptr, 0, nullptr }
};

static PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "module",                                // Module name to use with Python import statements
    "Buffered pipe through shared memory",  // Module description
    -1,
    methods                                 // Structure that defines the methods of the module
};
 
PyMODINIT_FUNC PyInit_module() {
    return PyModule_Create(&module);
}
