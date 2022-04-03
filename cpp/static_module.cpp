#pragma GCC optimize("Ofast")

#include <Python.h>

#include <fcntl.h>
// #include <stdlib.h>
// #include <unistd.h>
#include <sys/mman.h>
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

#define NULL_PID -1
#define NULL_FD -1

const unsigned int PyBytesObject_SIZE = offsetof(PyBytesObject, ob_sval) + 1;

struct Pipe_info {
    // placed on shared memory
    volatile unsigned int pointer_a;
    volatile unsigned int pointer_f;
    sem_t sem_a, sem_f;
    pthread_mutex_t mutex_a, mutex_f, mutex_m;
};

struct Pipe {
    // placed on private memory, save on python
    void *info; // Cached attached shared memory address
    
    unsigned int obj_cnt;
    unsigned int obj_size;
    unsigned int shm_fd;
    pid_t pid;
pipe_timer_data

    void init(void *shm_fn){
        pid = getpid();
        if ((shm_fd = shm_open((char *)shm_fn, O_RDWR|O_CREAT, 0666)) == -1) {
            fprintf(stderr, "Open failed: %s\n", strerror(errno));
            exit(1);
        }
        if (ftruncate(shm_fd, PAGESIZE()) == -1) {
            fprintf(stderr, "ftruncate : %s\n", strerror(errno));
            exit(1);
        }
        info = mmap(0, PAGESIZE(), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    }
    void reinit(void *shm_fn){
        pid_t _pid = getpid();
        if(pid != _pid) {
            pid = _pid;
            shm_fd = shm_open((char *)shm_fn, O_RDWR, 0666);
            info = mmap(0, PAGESIZE(), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        }
    }
    inline unsigned int PAGESIZE(){return sizeof(Pipe_info) + obj_size * obj_cnt;}
    inline char* GET_BUF(){return (char*)info;}
    inline Pipe_info* GET_INFO(){return (Pipe_info*) (info + obj_size * obj_cnt);}
};

PyObject* __init(PyObject *, PyObject* args) {
    Py_buffer fn_obj;
    unsigned int obj_cnt, obj_size;
    PyArg_ParseTuple(args, "IIs*", &obj_size, &obj_cnt, &fn_obj);

    Pipe pipe;
    memset(&pipe, 0, sizeof(Pipe));
    pipe.obj_size = obj_size;
    pipe.obj_cnt = obj_cnt;
    pipe.init(fn_obj.buf);
    Pipe_info &info = *pipe.GET_INFO();

    sem_init(&info.sem_a, 1, 0);
    sem_init(&info.sem_f, 1, pipe.obj_cnt);

    pthread_mutexattr_t psharedm;
    pthread_mutexattr_init(&psharedm);
    pthread_mutexattr_setrobust(&psharedm, PTHREAD_MUTEX_ROBUST);
    pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&info.mutex_a, &psharedm);
    pthread_mutex_init(&info.mutex_f, &psharedm);
    pthread_mutex_init(&info.mutex_m, &psharedm);

    PyBuffer_Release(&fn_obj);
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}

PyObject* __free(PyObject *, PyObject* args) {
    Py_buffer pipe_obj, fn_obj;
    PyArg_ParseTuple(args, "y*s*", &pipe_obj, &fn_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
show_time_spend(pipe)
    if(pipe->pid != NULL_PID) {
        sem_destroy(&((Pipe_info*)pipe->info)->sem_a);
        sem_destroy(&((Pipe_info*)pipe->info)->sem_f);
        munmap(pipe->info, pipe->PAGESIZE());
        // close(pipe->shm_fd);
        shm_unlink((char*)fn_obj.buf);
        pipe->shm_fd = NULL_FD;
        pipe->pid = NULL_PID;
    }

    PyBuffer_Release(&pipe_obj);
    PyBuffer_Release(&fn_obj);
    Py_RETURN_NONE;
}

struct read_request_data {
    PyObject* result;
    Pipe* pipe;
    void* pointer;
    sem_t* sem;
    read_request_data(Pipe* v1, void* v2, sem_t* v3):pipe(v1), pointer(v2), sem(v3){}
};
void *read_request(void *_data) {
    read_request_data* data = (read_request_data*) _data;
    data->result = PyBytes_FromStringAndSize((char *)data->pointer, data->pipe->obj_size);
    // PyObject* result = PyByteArray_FromStringAndSize(pointer, info.obj_size);

    // PyBytesObject *result = (PyBytesObject *)PyObject_Malloc(PyBytesObject_SIZE + pipe->obj_size);
    // PyObject_InitVar((PyVarObject*)result, &PyBytes_Type, pipe->obj_size);
    // unsigned int len = pipe->obj_size;
    // char *d_buf = (char *)result->ob_sval;
    // for(volatile char *pt = pointer; len--; *(d_buf++) = *(pt++));
    // memcpy(result->ob_sval, pointer, pipe->obj_size);
    sem_post(data->sem);
    
    return NULL; // pthread_exit(NULL);
}
PyObject* recv_bytes(PyObject *, PyObject* args) {
local_timer_init
collect_time
    Py_buffer pipe_obj, fn_obj;
    PyArg_ParseTuple(args, "y*s*", &pipe_obj, &fn_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    pipe->reinit(fn_obj.buf);
    char* pointer = pipe->GET_BUF();
    Pipe_info *info = pipe->GET_INFO();

collect_time
    if(sem_trywait(&info->sem_a)) {
        Py_BEGIN_ALLOW_THREADS
        sem_wait(&info->sem_a);
        Py_END_ALLOW_THREADS 
    }
collect_time
    if(pthread_mutex_trylock(&info->mutex_f)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&info->mutex_f);
        Py_END_ALLOW_THREADS 
    }
    pointer += info->pointer_f * pipe->obj_size;
    if(++info->pointer_f == pipe->obj_cnt)
        info->pointer_f = 0;

    read_request_data data(pipe, pointer, &info->sem_f);
    {
        pthread_t thread;
        if (pthread_create(&thread, NULL, read_request, (void *)&data) < 0) {
            perror("thread create error : ");
            exit(0);
        }
        msync((void*)&info->pointer_f, sizeof(info->pointer_f), MS_SYNC | MS_INVALIDATE);
        pthread_mutex_unlock(&info->mutex_f);

        pthread_join(thread, nullptr);
    }
collect_time
update_time(pipe)
    PyBuffer_Release(&pipe_obj);
    PyBuffer_Release(&fn_obj);
    return data.result;
}

PyObject* send_bytes(PyObject *, PyObject *args) {
local_timer_init
collect_time
    Py_buffer pipe_obj, data_obj, fn_obj;
    PyArg_ParseTuple(args, "y*y*s*", &pipe_obj, &data_obj, &fn_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    pipe->reinit(fn_obj.buf);
    char* pointer = pipe->GET_BUF();
    Pipe_info *info = pipe->GET_INFO();
    assert(pipe->obj_size == data_obj.len);
    
collect_time
    if(sem_trywait(&info->sem_f)) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(&info->sem_f);
        Py_END_ALLOW_THREADS 
    }
collect_time
    if(pthread_mutex_trylock(&info->mutex_a)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&info->mutex_a);
        Py_END_ALLOW_THREADS 
    }
    pointer += info->pointer_a * pipe->obj_size;
    if(++info->pointer_a == pipe->obj_cnt)
        info->pointer_a = 0;

    msync((void*)&info->pointer_a, sizeof(info->pointer_a), MS_ASYNC | MS_INVALIDATE);
    
    memcpy(pointer, data_obj.buf, pipe->obj_size);
    msync(pointer, pipe->obj_size, MS_SYNC | MS_INVALIDATE);
    msync((void*)&info->pointer_a, sizeof(info->pointer_a), MS_SYNC);
    sem_post(&info->sem_a);

    pthread_mutex_unlock(&info->mutex_a);
collect_time
update_time(pipe)
    PyBuffer_Release(&data_obj);
    PyBuffer_Release(&fn_obj);
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
