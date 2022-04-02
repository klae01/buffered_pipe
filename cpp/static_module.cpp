#pragma GCC optimize("Ofast")

#include <Python.h>

#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>


// init, free
// recv_bytes, send_bytes

// #define DEBUG

#ifdef DEBUG
#include <algorithm>
#include <chrono>
#define pipe_timer_data long long time_save[10]; 
#define local_timer_init std::vector<std::chrono::high_resolution_clock::time_point> time_save; time_save.reserve(10);
#define collect_time time_save.push_back(std::chrono::high_resolution_clock::now()); 
#define update_time(pipe) \
for(int i = 1 ; i < time_save.size(); i++) \
    pipe->time_save[i] += std::chrono::duration_cast<std::chrono::nanoseconds>(time_save[i] - time_save[i-1]).count(); 
#define show_time_spend(pipe) \
printf("free %u\n", pipe->info_id); \
for(int i = 0;i < 10; i ++) \
    if(pipe->time_save[i]) \
        printf("%d : %lld\n", i, pipe->time_save[i]);
#else 
#define pipe_timer_data
#define local_timer_init 
#define collect_time
#define update_time(pipe)
#define show_time_spend(pipe)
#endif

const unsigned int PyBytesObject_SIZE = offsetof(PyBytesObject, ob_sval) + 1;

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
    void *info; // Cached attached shared memory address
    
    unsigned int info_id;
    pid_t pid;
pipe_timer_data
};

void mp_request_init(Pipe &pipe) {
    pid_t pid = getpid();
    if(pipe.pid != pid) {
        pipe.info = shmat(pipe.info_id, NULL, 0);
        pipe.pid = pid;
    }
}

PyObject* __init(PyObject *, PyObject* args) {
    unsigned int obj_size, obj_cnt;
    PyArg_ParseTuple(args, "II", &obj_size, &obj_cnt);

    Pipe pipe;
    memset(&pipe, 0, sizeof(Pipe));
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
    // pthread_mutexattr_setrobust(&psharedm, PTHREAD_MUTEX_ROBUST);
    pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&info.mutex_w, &psharedm);
    pthread_mutex_init(&info.mutex_r, &psharedm);
    
    pipe.pid = getpid();
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}

PyObject* __free(PyObject *, PyObject* args) {
    Py_buffer pipe_obj;
    PyObject_GetBuffer(args, &pipe_obj, PyBUF_SIMPLE);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
show_time_spend(pipe)
    shmdt(pipe->info);
    shmctl(pipe->info_id , IPC_RMID , NULL);
    PyBuffer_Release(&pipe_obj);
    Py_RETURN_NONE;
}

PyObject* recv_bytes(PyObject *, PyObject* args) {
local_timer_init
collect_time
    Py_buffer pipe_obj;
    PyObject_GetBuffer(args, &pipe_obj, PyBUF_SIMPLE);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    mp_request_init(*pipe);
    char* pointer = (char*) pipe->info + sizeof(Pipe_info);
    Pipe_info &info = *(Pipe_info*)pipe->info;

    PyBytesObject *result = (PyBytesObject *)PyObject_Malloc(PyBytesObject_SIZE + info.obj_size);
    PyObject_InitVar((PyVarObject*)result, &PyBytes_Type, info.obj_size);

collect_time
    if(sem_trywait(&info.sem_a)) {
        Py_BEGIN_ALLOW_THREADS
        sem_wait(&info.sem_a);
        Py_END_ALLOW_THREADS 
    }
collect_time
    if(pthread_mutex_trylock(&info.mutex_r)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&info.mutex_r);
        Py_END_ALLOW_THREADS 
    }
    pointer += info.pointer_f * info.obj_size;
    if(++info.pointer_f == info.obj_cnt)
        info.pointer_f = 0;
    pthread_mutex_unlock(&info.mutex_r);
collect_time

    // PyObject* result = PyBytes_FromStringAndSize(pointer, info.obj_size);
    // PyObject* result = PyByteArray_FromStringAndSize(pointer, info.obj_size);
    memcpy(result->ob_sval, pointer, info.obj_size);
    sem_post(&info.sem_f);
collect_time
update_time(pipe)
    PyBuffer_Release(&pipe_obj);
    return (PyObject *)result;
}

PyObject* send_bytes(PyObject *, PyObject *args) {
local_timer_init
collect_time
    Py_buffer pipe_obj, data_obj;
    PyArg_ParseTuple(args, "y*y*", &pipe_obj, &data_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    mp_request_init(*pipe);
    char* pointer = (char*) pipe->info + sizeof(Pipe_info);
    Pipe_info &info = *(Pipe_info*)pipe->info;
    assert(info.obj_size == data_obj.len);
    
collect_time
    if(sem_trywait(&info.sem_f)) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(&info.sem_f);
        Py_END_ALLOW_THREADS 
    }
collect_time
    if(pthread_mutex_trylock(&info.mutex_w)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&info.mutex_w);
        Py_END_ALLOW_THREADS 
    }
    pointer += info.pointer_a * info.obj_size;
    if(++info.pointer_a == info.obj_cnt)
        info.pointer_a = 0;
    pthread_mutex_unlock(&info.mutex_w);
collect_time

    memcpy(pointer, data_obj.buf, info.obj_size);
    sem_post(&info.sem_a);
collect_time
update_time(pipe)
    PyBuffer_Release(&data_obj);
    PyBuffer_Release(&pipe_obj);
    Py_RETURN_NONE;
}

static PyMethodDef methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "init", (PyCFunction)__init, METH_O, nullptr },
    { "free", (PyCFunction)__free, METH_O, nullptr },
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
