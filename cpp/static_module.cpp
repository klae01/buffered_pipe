#pragma GCC optimize("Ofast")

#include <Python.h>

#include <algorithm>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <string.h>
#include <assert.h>

// init, free
// recv_bytes, send_bytes

struct Pipe_info {
    // placed on shared memory
    unsigned int obj_size;
    unsigned int obj_cnt;
    unsigned int pointer_a;
    unsigned int pointer_f;
    sem_t sem_a, sem_f;
    pthread_mutex_t mutex_w, mutex_r;
};

struct Pipe {
    // placed on private memory, save on python
    void *info; // info with buffer
    
    unsigned int info_id;
    pid_t pid;
};

void mp_request_init(Pipe &pipe) {
    pid_t pid = getpid();
    if(pipe.pid != pid) {
        pipe.info = shmat(pipe.info_id, NULL, 0);
        pipe.pid = pid;
    }
}

PyObject* init(PyObject *, PyObject* args) {
    unsigned int obj_size, obj_cnt;
    PyArg_ParseTuple(args, "II", &obj_size, &obj_cnt);

    Pipe pipe;
    pipe.info_id = shmget(IPC_PRIVATE, sizeof(Pipe_info) + obj_size * obj_cnt, IPC_CREAT | 0644);
    pipe.info = shmat(pipe.info_id, NULL, 0);
    memset(pipe.info, 0, sizeof(Pipe_info));
    Pipe_info &info = *(Pipe_info*)pipe.info;

    info.obj_size = obj_size;
    info.obj_cnt = obj_cnt;

    sem_init(&info.sem_a, 1, 0);
    sem_init(&info.sem_f, 1, info.obj_cnt);

    pthread_mutexattr_t psharedm;
    pthread_mutexattr_init(&psharedm);
    pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&info.mutex_w, &psharedm);
    pthread_mutex_init(&info.mutex_r, &psharedm);
    
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
    shmdt(pipe.info);
    shmctl(pipe.info_id , IPC_RMID , NULL);
    Py_RETURN_NONE;
}

PyObject* recv_bytes(PyObject *, PyObject* args) {
    Pipe pipe;
    char*      pipe_bytes;
    Py_ssize_t pipe_len = 0;
    PyBytes_AsStringAndSize(args, &pipe_bytes, &pipe_len);
    memcpy(&pipe, pipe_bytes, sizeof(Pipe));
    mp_request_init(pipe);
    char* pointer = (char*) pipe.info + sizeof(Pipe_info);
    Pipe_info &info = *(Pipe_info*)pipe.info;

    if(sem_trywait(&info.sem_a)) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(&info.sem_a);
        Py_END_ALLOW_THREADS 
    }
    pthread_mutex_lock(&info.mutex_r);
    pointer += info.pointer_f * info.obj_size;
    if(++info.pointer_f == info.obj_cnt)
        info.pointer_f = 0;
    pthread_mutex_unlock(&info.mutex_r);

    PyObject* result = PyBytes_FromStringAndSize(pointer, info.obj_size);
    sem_post(&info.sem_f);
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
    Pipe_info &info = *(Pipe_info*)pipe.info;
    char* pointer = (char*) pipe.info + sizeof(Pipe_info);
    assert(info.obj_size == req_len);

    if(sem_trywait(&info.sem_f)) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(&info.sem_f);
        Py_END_ALLOW_THREADS 
    }
    pthread_mutex_lock(&info.mutex_w);
    pointer += info.pointer_a * info.obj_size;
    if(++info.pointer_a == info.obj_cnt)
        info.pointer_a = 0;
    pthread_mutex_unlock(&info.mutex_w);

    memmove(pointer, req_data, req_len);
    sem_post(&info.sem_a);
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
    "static_module",                                // Module name to use with Python import statements
    "Buffered pipe through shared memory / static length ",  // Module description
    0,
    methods                                 // Structure that defines the methods of the module
};
 
PyMODINIT_FUNC PyInit_static_module() {
    return PyModule_Create(&module);
}
