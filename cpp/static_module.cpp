#pragma GCC optimize("Ofast")

#include <Python.h>

#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <pthread.h>
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
#define TOMB_PID -2
#define SHM_SIZE(obj_size, obj_cnt, concurrency) ( \
    sizeof(Pipe_info) \
 + (sizeof(sem_t) + sizeof(pthread_mutex_t)) * concurrency * 2 \
 + obj_size * obj_cnt \
)
#define pythread_mutex_lock(lock) \
if(pthread_mutex_trylock(lock)) { \
    Py_BEGIN_ALLOW_THREADS \
    pthread_mutex_lock(lock); \
    Py_END_ALLOW_THREADS \
}

const u_int32_t Giga = 1000000000;
const unsigned int PyBytesObject_SIZE = offsetof(PyBytesObject, ob_sval) + 1;

struct Ticket {
    unsigned int pointer;
    unsigned int ticket;
};
struct Pipe_info {
    // placed on shared memory
    unsigned int obj_size;
    unsigned int obj_cnt;
    char UUID[16];
    sem_t M_sem[2];

    u_int32_t polling;
    unsigned int concurrency;
    unsigned int M_pointer[2];
    unsigned int M_lookup[2];
    unsigned int M_ticket[2];
    pthread_mutex_t M_mutex_c[2];
    pthread_mutex_t M_mutex_t[2];
    uintptr_t M_reserve[2];
    uintptr_t M_elder_lock[2];

    uintptr_t INIT (
                        unsigned int _obj_size, 
                        unsigned int _obj_cnt, 
                        unsigned int _concurrency, 
                        unsigned int _polling
                    ) {

        memset(this, 0, sizeof(*this));

        obj_size = _obj_size;
        obj_cnt = _obj_cnt;
        concurrency = _concurrency;
        polling = _polling;

        sem_init(&M_sem[0], 1, 0);
        sem_init(&M_sem[1], 1, obj_cnt);

        pthread_mutexattr_t psharedm;
        pthread_mutexattr_init(&psharedm);
        pthread_mutexattr_setrobust(&psharedm, PTHREAD_MUTEX_ROBUST);
        pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);

        // Manager attributes
        pthread_mutex_init(&M_mutex_c[0], &psharedm);
        pthread_mutex_init(&M_mutex_c[1], &psharedm);
        pthread_mutex_init(&M_mutex_t[0], &psharedm);
        pthread_mutex_init(&M_mutex_t[1], &psharedm);

        sem_t *sem_PT = (sem_t *) ((char*)this + sizeof(*this));
        M_reserve[0] = (uintptr_t)sem_PT - (uintptr_t)this;
        for(unsigned int i = _concurrency; i--; ) sem_init(sem_PT++, 1, 0);
        M_reserve[1] = (uintptr_t)sem_PT - (uintptr_t)this;
        for(unsigned int i = _concurrency; i--; ) sem_init(sem_PT++, 1, 0);
        
        pthread_mutex_t *mutex_PT = (pthread_mutex_t *) sem_PT;
        M_elder_lock[0] = (uintptr_t)mutex_PT - (uintptr_t)this;
        for(unsigned int i = _concurrency; i--; ) pthread_mutex_init(mutex_PT++, &psharedm);
        M_elder_lock[1] = (uintptr_t)mutex_PT - (uintptr_t)this;
        for(unsigned int i = _concurrency; i--; ) pthread_mutex_init(mutex_PT++, &psharedm);

        return (uintptr_t)mutex_PT - (uintptr_t)this;
    }

    // manager functions
    void collect(int index) {
        pthread_mutex_t *lock = M_mutex_c + index;

        if(!pthread_mutex_trylock(lock)) {
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[index]);
            sem_t *post_target = M_sem + !index;
            unsigned int lookup = M_lookup[index];
            while(!sem_trywait(reserve + lookup)) {
                if(++lookup == concurrency)
                    lookup = 0;
                sem_post(post_target);
            }
            M_lookup[index] = lookup;
            pthread_mutex_unlock(lock);
        }
    }
    Ticket wait_ticket (int index) {
        sem_t *sem = M_sem + index;
        pthread_mutex_t *lock = &M_mutex_t[index];
        if(concurrency) {
            // Out-of-order protocol
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[index]);
            if(sem_trywait(sem)) {
                int result;
                struct timespec ts;
                do {
                    if (clock_gettime(CLOCK_REALTIME, &ts)) {
                        perror("clock_gettime");
                        exit(-1);
                    }
                    ts.tv_sec += (u_int32_t)(polling + ts.tv_nsec) / Giga;
                    ts.tv_nsec = (u_int32_t)(polling + ts.tv_nsec) % Giga;

                    collect(!index);
                    
                    if(result = sem_trywait(sem)) {
                        Py_BEGIN_ALLOW_THREADS
                        result = sem_timedwait(sem, &ts);
                        Py_END_ALLOW_THREADS
                    }
                } while(result && errno == ETIMEDOUT && !PyErr_CheckSignals());
            }
            pythread_mutex_lock(lock)
            Ticket RET = {M_pointer[index], M_ticket[index]};
            pythread_mutex_lock(elder_lock + RET.ticket)
            M_pointer[index] = (RET.pointer + 1 == obj_cnt? 0 : RET.pointer + 1);
            M_ticket[index] = (RET.ticket + 1 == concurrency? 0 : RET.ticket + 1);
            pthread_mutex_unlock(lock);
            return RET;
        }
        
        else {
            // In-order protocol
            if(sem_trywait(sem)) {
                Py_BEGIN_ALLOW_THREADS
                sem_wait(sem);
                Py_END_ALLOW_THREADS
            }
            pythread_mutex_lock(lock)
            Ticket RET = {M_pointer[index], 0};
            M_pointer[index] = (RET.pointer + 1 == obj_cnt? 0 : RET.pointer + 1);
            return RET;
        }
    }
    void post_ticket (int index, unsigned int T) {
        if(concurrency) {
            // Out-of-order protocol
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[index]);
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[index]);
            sem_post(reserve + T);
            pthread_mutex_unlock(elder_lock + T);
            collect(index);
        }
        else {
            // In-order protocol
            sem_t *sem = M_sem + !index;
            pthread_mutex_t *lock = &M_mutex_t[index];
            sem_post(sem);
            pthread_mutex_unlock(lock);
        }
    }
};

struct Pipe {
    // placed on private memory, save on python
    void *info; // Cached attached shared memory address
    char UUID[16];
    uintptr_t buf_offset;
    unsigned int info_id;
    pid_t pid;
pipe_timer_data
};

int mp_request_init(Pipe &pipe) {
    if(pipe.pid == TOMB_PID) {
        PyErr_SetString(PyExc_BrokenPipeError, "Pipe deallocated before attach");
        return -1;
    }
    pid_t pid = getpid();
    if(pipe.pid != pid) {
        if( (intptr_t)(pipe.info = shmat(pipe.info_id, NULL, 0)) == -1 ) {
            switch (errno)
            {
                case EACCES:
                    PyErr_SetString(PyExc_PermissionError,
"Cannot attach shared memory.\n"
"Operation permission is denied to the calling process.");
                    break;
                case EINVAL:
                    PyErr_SetString(PyExc_BrokenPipeError, 
"Cannot attach shared memory.\n"
"The value of shmid is not a valid shared memory identifier.");
                    break;
                case EMFILE:
                    PyErr_SetString(PyExc_ConnectionError, 
"Cannot attach shared memory.\n"
"The number of shared memory segments attached to the calling process would exceed the system-imposed limit.");
                    break;
                case ENOMEM:
                    PyErr_SetString(PyExc_MemoryError, 
"Cannot attach shared memory.\n"
"The available data space is not large enough to accommodate the shared memory segment.");
                    break;
            }
            pipe.pid = TOMB_PID;
            return -1;
        }
        pipe.pid = pid;
    }
    return 0;
}

int check_pipe_validation(Pipe &pipe) {
    if( memcmp(pipe.UUID, ((Pipe_info*)pipe.info)->UUID, 16) ) {
        PyErr_SetString(PyExc_BrokenPipeError, "Pipe deallocated before use");
        return -1;
    }
    return 0;
}

PyObject* __init(PyObject *, PyObject* args) {
    unsigned int obj_size, obj_cnt, concurrency;
    float polling;
    Py_buffer UUID_obj;
    PyArg_ParseTuple(args, "IIIfy*", &obj_size, &obj_cnt, &concurrency, &polling, &UUID_obj);

    Pipe pipe;
    memset(&pipe, 0, sizeof(Pipe));
    memcpy(pipe.UUID, UUID_obj.buf, UUID_obj.len);
    // info init SHM
    if( (pipe.info_id = shmget(IPC_PRIVATE, SHM_SIZE(obj_size, obj_cnt, concurrency), IPC_CREAT | 0644)) < 0) {
        switch (errno)
        {
            case EINVAL:
                PyErr_SetString(PyExc_PermissionError,
"Cannot create shared memory.\n"
"Operation permission is denied to the calling process.");
                break;
            case ENOMEM:
                PyErr_SetString(PyExc_MemoryError, 
"Cannot create shared memory.\n"
"The value of size is greater than the system-imposed maximum.");
                break;
            case ENOSPC:
                PyErr_WarnEx(PyExc_ResourceWarning, 
"A shared memory identifier is to be created but the system-imposed limit on the maximum number of allowed shared memory identifiers system-wide would be exceeded.",
1);
                break;
        }
        PyBuffer_Release(&UUID_obj);
        Py_RETURN_NONE;
    }
    pipe.pid = NULL_PID;
    if( mp_request_init(pipe) ) {
        shmctl(pipe.info_id , IPC_RMID , NULL);
        PyBuffer_Release(&UUID_obj);
        Py_RETURN_NONE;
    }
    shmctl(pipe.info_id , IPC_RMID , NULL);
    
    Pipe_info &info = *(Pipe_info*)pipe.info;
    pipe.buf_offset = info.INIT(obj_size, obj_cnt, concurrency, polling * Giga);
    memcpy(info.UUID, UUID_obj.buf, UUID_obj.len);
    PyBuffer_Release(&UUID_obj);
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}

PyObject* __free(PyObject *, PyObject* args) {
    Py_buffer pipe_obj;
    PyObject_GetBuffer(args, &pipe_obj, PyBUF_SIMPLE);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
show_time_spend(pipe)
    if(pipe->pid == getpid()) {
        if( shmdt(pipe->info) ) {
            switch (errno)
            {
                case EINVAL:
                    PyErr_SetString(PyExc_BrokenPipeError,
"Cannot detach shared memory.\n"
"The value of shmaddr is not the data segment start address of a shared memory segment.");
                    break;
            }
        }
        pipe->pid = TOMB_PID;
    }
    PyBuffer_Release(&pipe_obj);
    Py_RETURN_NONE;
}

PyObject* __register(PyObject *, PyObject* args) {
    Py_buffer pipe_obj, inst_obj;
    PyArg_ParseTuple(args, "y*s*", &pipe_obj, &inst_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    if(pipe->pid == TOMB_PID)
        PyErr_SetString(PyExc_BrokenPipeError, "The pipe has already been deleted.");
    else if(strcmp((char*)inst_obj.buf, "FORK") == 0) {
        pipe->pid = NULL_PID;
        if( !mp_request_init(*pipe) )
            check_pipe_validation(*pipe);
    }
    else if(strcmp((char*)inst_obj.buf, "UPDATE") == 0) {
        if(pipe->pid != getpid())
            if( !mp_request_init(*pipe) )
                check_pipe_validation(*pipe);
    }
    else {
        char str[120];
        sprintf(
            str, 
            "instruction for register : \n"
            "-----(start)\n"
            "%.20s\n"
            "====(end)\n"
            "Is not defined in instruction set", (char*)inst_obj.buf);
        PyErr_SetString(PyExc_NotImplementedError, str);
    }
    PyBuffer_Release(&pipe_obj);
    PyBuffer_Release(&inst_obj);
    Py_RETURN_NONE;
}

PyObject* recv_bytes(PyObject *, PyObject* args) {
local_timer_init
collect_time
    Py_buffer pipe_obj;
    PyObject_GetBuffer(args, &pipe_obj, PyBUF_SIMPLE);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    if( mp_request_init(*pipe) || check_pipe_validation(*pipe)) {
        PyBuffer_Release(&pipe_obj);
        Py_RETURN_NONE;
    }
    Pipe_info &info = *(Pipe_info*)pipe->info;

    PyBytesObject *result = (PyBytesObject *)PyObject_Malloc(PyBytesObject_SIZE + info.obj_size);
    PyObject_InitVar((PyVarObject*)result, &PyBytes_Type, info.obj_size);

collect_time
    Ticket T = info.wait_ticket(0);
collect_time
    void* pointer = (char*)pipe->info + pipe->buf_offset + T.pointer * info.obj_size;

    // unsigned int len = info.obj_size;
    // char *d_buf = (char *)result->ob_sval;
    // for(volatile char *pt = pointer; len--; *(d_buf++) = *(pt++));
    memcpy(result->ob_sval, pointer, info.obj_size);
    info.post_ticket(0, T.ticket);
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
    if( mp_request_init(*pipe) || check_pipe_validation(*pipe)) {
        PyBuffer_Release(&data_obj);
        PyBuffer_Release(&pipe_obj);
        Py_RETURN_NONE;
    }
    Pipe_info &info = *(Pipe_info*)pipe->info;
    assert(info.obj_size == data_obj.len);
    
collect_time
    Ticket T = info.wait_ticket(1);
collect_time
    void* pointer = (char*)pipe->info + pipe->buf_offset + T.pointer * info.obj_size;

    // unsigned int len = info.obj_size;
    // char *d_buf = (char *)data_obj.buf;
    // for(volatile char *pt = pointer; len--; *(pt++) = *(d_buf++));
    memcpy(pointer, data_obj.buf, info.obj_size);
    info.post_ticket(1, T.ticket);
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
    { "register", (PyCFunction)__register, METH_O, nullptr },
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
