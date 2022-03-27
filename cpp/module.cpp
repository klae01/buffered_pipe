#pragma GCC optimize("Ofast")

#include <Python.h>

#include <algorithm>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <string.h>

// init, free
// recv_bytes, send_bytes

struct Pipe_info {
    // placed on shared memory
    unsigned int minimum_write;
    unsigned int shm_size;
    unsigned int LENGTH_INFO;
    unsigned int buf_id;
    unsigned int pointer;
    unsigned int pointer_a;
    unsigned int pointer_f;
    unsigned int margin;
    sem_t sem_a, sem_f;
    pthread_mutex_t mutex_w, mutex_r;
};

struct Pipe {
    // placed on private memory, save on python
    Pipe_info *info;
    char *shm_buf;
    
    unsigned int info_id;
    pid_t pid;
};

void mp_request_init(Pipe &pipe) {
    pid_t pid = getpid();
    if(pipe.pid != pid) {
        pipe.info = (Pipe_info*) shmat(pipe.info_id, NULL, 0);
        pipe.shm_buf = (char*) shmat(pipe.info->buf_id, NULL, 0);
        pipe.pid = pid;
    }
}

PyObject* init(PyObject *, PyObject* args) {
    // 0: shm size-> shared memory ids
    // 1: pipe id -> info ptr, buf ptr
    Pipe pipe;
    unsigned int minimum_write, shm_size;
    PyArg_ParseTuple(args, "II", &minimum_write, &shm_size);
    pipe.info_id = shmget(IPC_PRIVATE, sizeof(Pipe_info), IPC_CREAT | 0644);
    pipe.info = (Pipe_info*) shmat(pipe.info_id, NULL, 0);
    memset(pipe.info, 0, sizeof(Pipe_info));

    pipe.info->buf_id = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0644);
    pipe.info->minimum_write = minimum_write;
    pipe.info->shm_size = shm_size;
    for(unsigned int t = shm_size >> 7; t; t >>= 8)
        pipe.info->LENGTH_INFO++;
    pipe.info->LENGTH_INFO++;
    pipe.info->margin = shm_size;

    sem_init(&pipe.info->sem_a, 1, 0);
    sem_init(&pipe.info->sem_f, 1, 0);

    pthread_mutexattr_t psharedm;
    pthread_mutexattr_init(&psharedm);
    pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&pipe.info->mutex_w, &psharedm);
    pthread_mutex_init(&pipe.info->mutex_r, &psharedm);
    
    pipe.shm_buf = (char*) shmat(pipe.info->buf_id, NULL, 0);
    pipe.pid = getpid();
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}

PyObject* free(PyObject *, PyObject* args) {
    // 0: shm size-> shared memory ids
    // 1: pipe id -> info ptr, buf ptr
    Pipe pipe;
    char*      pipe_bytes;
    Py_ssize_t pipe_len = 0;
    PyBytes_AsStringAndSize(args, &pipe_bytes, &pipe_len);
    memcpy(&pipe, pipe_bytes, sizeof(Pipe));
    shmdt(pipe.shm_buf);
    shmctl(pipe.info->buf_id , IPC_RMID , NULL);
    shmdt(pipe.info);
    shmctl(pipe.info_id , IPC_RMID , NULL);
    Py_RETURN_NONE;
}

void read_request(Pipe &pipe, Pipe_info &info, unsigned int &pointer, unsigned int length, char*result) {
    unsigned int lookup = 0;
    if(info.shm_size < pointer + length) {
        lookup = info.shm_size - pointer;
        memmove(result, pipe.shm_buf + pointer, lookup);
        length -= lookup;
        pointer = 0;
    }
    memmove(result + lookup, pipe.shm_buf + pointer, length);
    pointer += length;
}

inline void PyList_Append_(PyObject* list, PyObject* item) {
    PyList_Append(list, item);
    Py_DECREF(item);
}
void read_request(Pipe &pipe, Pipe_info &info, unsigned int &pointer, unsigned int length, PyObject* result) {
    unsigned int lookup = 0;
    if(info.shm_size < pointer + length) {
        lookup = info.shm_size - pointer;
        if(lookup) PyList_Append_(result, PyBytes_FromStringAndSize(pipe.shm_buf + pointer, lookup));
        length -= lookup;
        pointer = 0;
    }
    if(length) PyList_Append_(result, PyBytes_FromStringAndSize(pipe.shm_buf + pointer, length));
    pointer += length;
}

void write_request(Pipe &pipe, Pipe_info &info, char* data, unsigned int length) {
    unsigned int tmp;
    if(info.shm_size < info.pointer_a + length) {
        tmp = info.shm_size - info.pointer_a;
        memmove(pipe.shm_buf+info.pointer_a, data, tmp);
        data += tmp;
        length -= tmp;
        info.pointer_a = 0;
    }
    memmove(pipe.shm_buf+info.pointer_a, data, length);
    info.pointer_a += length;
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

int chunk_size(Pipe &pipe, Pipe_info &info, unsigned int &pointer) {
    int2char i2c_value, i2c_cast;
    i2c_value.v = 0;
    read_request(pipe, info, pointer, info.LENGTH_INFO, i2c_value.c);
    
    if(i2c_value.c[info.LENGTH_INFO - 1] < 0) {
        i2c_cast.v = -1;
        memmove(i2c_cast.c, i2c_value.c, info.LENGTH_INFO);
        return i2c_cast.v;
    }
    return i2c_value.v;
}

void write_int(Pipe &pipe, Pipe_info &info, unsigned int value) {
    int2char i2c_value;
    i2c_value.v = value;
    write_request(pipe, info, i2c_value.c, info.LENGTH_INFO);
}

// void printf_hex(char *shm_buf, unsigned int shm_size) {
//     for (int i = 0; i < shm_size; i++)
//         printf("%02x ", (unsigned char)shm_buf[i]);
//     printf("\n");
// }

PyObject* recv_bytes(PyObject *, PyObject* args) {
    Pipe pipe;
    char*      pipe_bytes;
    Py_ssize_t pipe_len = 0;
    PyBytes_AsStringAndSize(args, &pipe_bytes, &pipe_len);
    memcpy(&pipe, pipe_bytes, sizeof(Pipe));
    mp_request_init(pipe);
    Pipe_info &info = *pipe.info;
    PyObject* result = PyList_New(0);
    pthread_mutex_lock(&info.mutex_r);
    int length = -1;
    while(length < 0) {
        if(sem_trywait(&info.sem_a)) {
            Py_BEGIN_ALLOW_THREADS 
            sem_wait(&info.sem_a);
            Py_END_ALLOW_THREADS 
        }
        length = chunk_size(pipe, info, info.pointer);
        read_request(pipe, info, info.pointer, std::abs(length), result);
        sem_post(&info.sem_f);
    }
    pthread_mutex_unlock(&info.mutex_r);
    return Py_BuildValue("y#O", &pipe, sizeof(Pipe), result);
}

PyObject* send_bytes(PyObject *, PyObject *args) {
    Pipe pipe;
    char*      pipe_bytes;
    char*      req_data;
    Py_ssize_t pipe_len = 0;
    Py_ssize_t req_len = 0;
    PyArg_ParseTuple(args, "y#y#", &pipe_bytes, &pipe_len, &req_data, &req_len);
    memcpy((char*) &pipe, pipe_bytes, sizeof(Pipe));
    mp_request_init(pipe);
    Pipe_info &info = *pipe.info;

    pthread_mutex_lock(&info.mutex_w);
    unsigned int critical_length, write_length, free_length, minimum_length;
    while(req_len) {
        critical_length = req_len + info.LENGTH_INFO;
        minimum_length = std::min(info.minimum_write, critical_length);
        while(info.margin < critical_length && !sem_trywait(&info.sem_f)) {
            free_length = std::abs(chunk_size(pipe, info, info.pointer_f));
            info.margin += info.LENGTH_INFO + free_length;
            info.pointer_f = (info.pointer_f + free_length) % info.shm_size;
        }
        while(info.margin < minimum_length) {
            Py_BEGIN_ALLOW_THREADS 
            sem_wait(&info.sem_f);
            Py_END_ALLOW_THREADS 
            free_length = std::abs(chunk_size(pipe, info, info.pointer_f));
            info.margin += info.LENGTH_INFO + free_length;
            info.pointer_f = (info.pointer_f + free_length) % info.shm_size;
        }
        
        write_length = std::min(info.margin, critical_length);
        info.margin -= write_length;
        write_length -= info.LENGTH_INFO;
        if( write_length < req_len ) write_int(pipe, info, -write_length);
        else write_int(pipe, info, write_length);
        write_request(pipe, info, req_data, write_length);
        req_data += write_length;
        req_len -= write_length;
        sem_post(&info.sem_a);
    }
    pthread_mutex_unlock(&info.mutex_w);
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}
static PyMethodDef methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "init", (PyCFunction)init, METH_O, nullptr },
    { "free", (PyCFunction)free, METH_O, nullptr },
    { "recv_bytes", (PyCFunction)recv_bytes, METH_O, nullptr },
    { "send_bytes", (PyCFunction)send_bytes, METH_O, nullptr },
 
    // Terminate the array with an object containing nulls.
    { nullptr, nullptr, 0, nullptr }
};

static PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "module",                                // Module name to use with Python import statements
    "Buffered pipe through shared memory",  // Module description
    0,
    methods                                 // Structure that defines the methods of the module
};
 
PyMODINIT_FUNC PyInit_module() {
    return PyModule_Create(&module);
}
