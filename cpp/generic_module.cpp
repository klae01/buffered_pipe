#pragma GCC optimize("Ofast")

#include <Python.h>

#include <fcntl.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <semaphore.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>

#include <functional>


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
#define SHM_SIZE(buffer_size, concurrency) ( \
    sizeof(Pipe_info) \
 + (sizeof(sem_t) + sizeof(pthread_mutex_t)) * concurrency * 2 \
 + buffer_size \
)
#define pythread_mutex_lock(lock) \
if(pthread_mutex_trylock(lock)) { \
    Py_BEGIN_ALLOW_THREADS \
    pthread_mutex_lock(lock); \
    Py_END_ALLOW_THREADS \
}
#define pysem_wait(sem) \
if(sem_trywait(sem)) { \
    Py_BEGIN_ALLOW_THREADS \
    sem_wait(sem); \
    Py_END_ALLOW_THREADS \
}
#define SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION

const u_int32_t Giga = 1000000000;
const unsigned int PyBytesObject_SIZE = offsetof(PyBytesObject, ob_sval) + 1;

struct sem_c {
#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
    pthread_mutex_t wait_mutex;
#endif
    pthread_mutex_t value_mutex;
    pthread_cond_t cond;
    u_int64_t value;
    void init(u_int64_t _value = 0) {
        pthread_mutexattr_t psharedm;
        pthread_condattr_t psharedc;

        pthread_mutexattr_init(&psharedm);
        pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);
        pthread_mutexattr_settype(&psharedm, PTHREAD_MUTEX_RECURSIVE);
        pthread_condattr_init(&psharedc);
        pthread_condattr_setpshared(&psharedc, PTHREAD_PROCESS_SHARED);

#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
        pthread_mutex_init(&wait_mutex, &psharedm);
#endif
        pthread_mutex_init(&value_mutex, &psharedm);
        pthread_cond_init(&cond, &psharedc);
        value = _value;
    }
    void post(u_int64_t _value = 1) {
        pythread_mutex_lock(&value_mutex)
        if(value == 0) {
            value = _value;
            pthread_cond_signal(&cond);
        } else value += _value;
        pthread_mutex_unlock(&value_mutex);
    }
    u_int64_t strategy_wait(u_int64_t min_val, u_int64_t max_val) {
#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
        pythread_mutex_lock(&wait_mutex)
#endif
        pythread_mutex_lock(&value_mutex)
        u_int64_t RET = value;
        value = 0;
        while(RET < min_val) {
            Py_BEGIN_ALLOW_THREADS
            pthread_cond_wait(&cond, &value_mutex);
            Py_END_ALLOW_THREADS
            RET += value; value = 0;
        }
        if(RET >= max_val) {
            value += RET - max_val;
            RET = max_val;
        }
#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
        pthread_mutex_unlock(&wait_mutex);
#endif
        pthread_mutex_unlock(&value_mutex);
        return RET;
    }
    template <class F>
    u_int64_t strategy_timedwait(u_int64_t min_val, u_int64_t max_val, u_int32_t polling, F collect) {
        min_val = std::min(min_val, max_val);
#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
        pythread_mutex_lock(&wait_mutex)
#endif
        pythread_mutex_lock(&value_mutex)
        u_int64_t RET = value; value = 0;
        struct timespec ts;
        
        while(RET < min_val) {
            if (clock_gettime(CLOCK_REALTIME, &ts)) {
                perror("clock_gettime");
                exit(-1);
            }
            ts.tv_sec += (u_int32_t)(polling + ts.tv_nsec) / Giga;
            ts.tv_nsec = (u_int32_t)(polling + ts.tv_nsec) % Giga;

            collect();
            RET += value; value = 0;
            if(RET >= min_val) break;

            Py_BEGIN_ALLOW_THREADS
            pthread_cond_timedwait(&cond, &value_mutex, &ts);
            Py_END_ALLOW_THREADS

            RET += value; value = 0;
        }
        if(RET >= max_val) {
            value = RET - max_val;
            RET = max_val;
        }
#ifndef SEM_C_WAIT_GUARRENTY_CRITICAL_SESSION
        pthread_mutex_unlock(&wait_mutex);
#endif
        pthread_mutex_unlock(&value_mutex);
        return RET;
    }
};
void circular_memcpy(void *dst_B,
                     uintptr_t dst_size,
                     void *src_B,
                     uintptr_t src_size,
                     uintptr_t dst_index,
                     uintptr_t src_index,
                     uintptr_t copy_length) {
    assert(dst_size >= copy_length);
    assert(src_size >= copy_length);
    uintptr_t lookup;
    do {
        lookup = std::min(dst_size - dst_index, src_size - src_index);
        lookup = std::min(lookup, copy_length);
        if(lookup) {
            memcpy(dst_B + dst_index, src_B + src_index, lookup);
            copy_length -= lookup;
            src_index = (src_index + lookup) % src_size;
            dst_index = (dst_index + lookup) % dst_size;
        }
    } while(lookup);
}
int protocol_overhead(uint64_t length) {
    int binary_length = 0;
    int use_bytes;
    for(int i = 0; i <64; i++)
        if (length & (1LL<<i))
            binary_length = i;
    // number of chars to represent info
    for(use_bytes = 1; 1 + (binary_length + use_bytes + 1) / 8 != use_bytes; use_bytes ++);
    return use_bytes;
}

struct Pipe_info {
    // placed on shared memory
    unsigned int minimum_write;
    unsigned int buf_size;
    unsigned int LENGTH_INFO;
    u_int32_t polling;

    unsigned int free_pointer;
    unsigned int concurrency;
    unsigned int M_pointer[2];
    unsigned int M_lookup[2];
    unsigned int M_ticket[2];
    sem_t obj_alloc;
    sem_c mem_free;
    pthread_mutex_t M_mutex_c[2];
    pthread_mutex_t M_mutex_t[2];
    uintptr_t M_reserve[2];
    uintptr_t M_elder_lock[2];

    uintptr_t INIT (
                        unsigned int _minimum_write, 
                        unsigned int _buf_size, 
                        unsigned int _concurrency, 
                        unsigned int _polling
                    ) {

        memset(this, 0, sizeof(*this));

        minimum_write = _minimum_write;
        buf_size = _buf_size;
        concurrency = _concurrency;
        polling = _polling;

        sem_init(&obj_alloc, 1, 0);
        mem_free.init(buf_size);

        pthread_mutexattr_t psharedm;
        pthread_mutexattr_init(&psharedm);
        pthread_mutexattr_setrobust(&psharedm, PTHREAD_MUTEX_ROBUST);
        pthread_mutexattr_setpshared(&psharedm, PTHREAD_PROCESS_SHARED);

        // Manager attributes
        pthread_mutex_init(&M_mutex_c[0], &psharedm);
        pthread_mutex_init(&M_mutex_c[1], &psharedm);
        pthread_mutex_init(&M_mutex_t[0], &psharedm);
        pthread_mutex_init(&M_mutex_t[1], &psharedm);

        sem_t *sem_PT = (sem_t *) ((void*)this + sizeof(*this));
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

    void read_collect(void *buf) {
        pthread_mutex_t *lock = M_mutex_c + 0;

        if(!pthread_mutex_trylock(lock)) {
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[0]);
            unsigned int lookup = M_lookup[0];
            while(!sem_trywait(reserve + lookup)) {
                auto pointer = free_pointer;
                uint64_t chunk_info;
                circular_memcpy(&chunk_info, 8, buf, buf_size, 0, pointer, 8);
                int info_length = 0;
                while(chunk_info & (1LL << info_length++));
                uint64_t chunk_length = (chunk_info & (1LL << 8 * info_length) - 1) >> info_length + 1;
                free_pointer = (pointer + chunk_length) % buf_size;

                if(++lookup == concurrency)
                    lookup = 0;
                mem_free.post(chunk_length);
            }
            M_lookup[0] = lookup;
            pthread_mutex_unlock(lock);
        }
    }
    bool read_request(void* buf, PyObject* LIST) {
        if(concurrency) {
            if(sem_trywait(&obj_alloc)) {
                int result;
                struct timespec ts;
                do {
                    if (clock_gettime(CLOCK_REALTIME, &ts)) {
                        perror("clock_gettime");
                        exit(-1);
                    }
                    ts.tv_sec += (u_int32_t)(polling + ts.tv_nsec) / Giga;
                    ts.tv_nsec = (u_int32_t)(polling + ts.tv_nsec) % Giga;
                    
                    write_collect();
                    
                    if(result = sem_trywait(&obj_alloc)) {
                        Py_BEGIN_ALLOW_THREADS
                        result = sem_timedwait(&obj_alloc, &ts);
                        Py_END_ALLOW_THREADS
                    }
                } while(result && errno == ETIMEDOUT && !PyErr_CheckSignals());
            }

        } else pysem_wait(&obj_alloc);
        
        auto pointer = M_pointer[0];
        auto ticket = M_ticket[0];
        uint64_t chunk_info;
        circular_memcpy(&chunk_info, 8, buf, buf_size, 0, pointer, 8);

        int info_length = 0;
        while(chunk_info & (1LL << info_length++));
        char finish = chunk_info >> info_length & 1;
        uint64_t chunk_length = (chunk_info & (1LL << 8 * info_length) - 1) >> info_length + 1;
        uint64_t data_length = chunk_length - info_length;
        M_pointer[0] = (pointer + chunk_length)%buf_size;
        if(concurrency) {
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[0]);
            pythread_mutex_lock(elder_lock + ticket)
            M_ticket[0] = (ticket + 1 == concurrency? 0 : ticket + 1);
            if(finish)
                pthread_mutex_unlock(&M_mutex_t[0]);
        } else {

        }
        
        PyBytesObject *result = (PyBytesObject *)PyObject_Malloc(PyBytesObject_SIZE + data_length);
        PyObject_InitVar((PyVarObject*)result, &PyBytes_Type, data_length);

        circular_memcpy(result->ob_sval, data_length, buf, buf_size, 0, (pointer + info_length)%buf_size, data_length);
        PyList_Append(LIST, (PyObject*)result);
        Py_DECREF(result);

        if(concurrency) {
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[0]);
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[0]);
            sem_post(reserve + ticket);
            pthread_mutex_unlock(elder_lock + ticket);
            read_collect(buf);
        } else {
            if(finish)
                pthread_mutex_unlock(&M_mutex_t[0]);
            mem_free.post(chunk_length);
        }
        return ! finish;
    }

    void write_collect() {
        pthread_mutex_t *lock = M_mutex_c + 1;

        if(!pthread_mutex_trylock(lock)) {
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[1]);
            sem_t *post_target = &obj_alloc;
            unsigned int lookup = M_lookup[1];
            while(!sem_trywait(reserve + lookup)) {
                if(++lookup == concurrency)
                    lookup = 0;
                sem_post(post_target);
            }
            M_lookup[1] = lookup;
            pthread_mutex_unlock(lock);
        }
    }
    uintptr_t write_request(void* buf, void* data, uint64_t length) {
        uint64_t chunk_length = concurrency? mem_free.strategy_timedwait(minimum_write, length + protocol_overhead(length), polling, [this, &buf](){this->read_collect(buf);}) 
                                            : mem_free.strategy_wait(minimum_write, length + protocol_overhead(length));
        uint64_t chunk_info;
        auto pointer = M_pointer[1];
        auto ticket = M_ticket[1];
        M_pointer[1] = (pointer + chunk_length)%buf_size;
        int info_length = protocol_overhead(chunk_length);
        char finish = (length == chunk_length - info_length);
        if(concurrency) {
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[1]);
            pythread_mutex_lock(elder_lock + ticket)
            M_ticket[1] = (ticket + 1 == concurrency? 0 : ticket + 1);
            if(finish)
                pthread_mutex_unlock(&M_mutex_t[1]);
        } else {

        }

        chunk_info = (chunk_length << 1 | finish) << 1;
        for(int i = info_length; --i;chunk_info = chunk_info << 1 | 1);

        circular_memcpy(buf, buf_size, &chunk_info, 8, pointer, 0, info_length);
        circular_memcpy(buf, buf_size, data, chunk_length - info_length, (pointer + info_length)%buf_size, 0, chunk_length - info_length);
        
        if(concurrency) {
            sem_t *reserve = (sem_t *)((uintptr_t)this + M_reserve[1]);
            pthread_mutex_t *elder_lock = (pthread_mutex_t *)((uintptr_t)this + M_elder_lock[1]);
            sem_post(reserve + ticket);
            pthread_mutex_unlock(elder_lock + ticket);
            write_collect();
        } else {
            if(finish)
                pthread_mutex_unlock(&M_mutex_t[1]);
            sem_post(&obj_alloc);
        }
        return chunk_length - info_length;
    }
};

struct Pipe {
    // placed on private memory, save on python
    void *info; // Cached attached shared memory address
    uintptr_t buf_offset;
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
    unsigned int minimum_write, buffer_size, concurrency;
    float polling;
    PyArg_ParseTuple(args, "IIIf", &minimum_write, &buffer_size, &concurrency, &polling);

    Pipe pipe;
    memset(&pipe, 0, sizeof(Pipe));
    pipe.info_id = shmget(IPC_PRIVATE, SHM_SIZE(buffer_size, concurrency), IPC_CREAT | 0644);
    pipe.pid = NULL_PID;
    mp_request_init(pipe);
    pipe.buf_offset = ((Pipe_info*)pipe.info)->INIT(minimum_write, buffer_size, concurrency, polling * Giga);
    return PyBytes_FromStringAndSize((char*) &pipe, sizeof(Pipe));
}

PyObject* __free(PyObject *, PyObject* args) {
    Py_buffer pipe_obj;
    PyObject_GetBuffer(args, &pipe_obj, PyBUF_SIMPLE);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
show_time_spend(pipe)
    if(pipe->pid != NULL_PID) {
        shmdt(pipe->info);
        shmctl(pipe->info_id , IPC_RMID , NULL);
        pipe->pid = NULL_PID;
    }
    PyBuffer_Release(&pipe_obj);
    Py_RETURN_NONE;
}

PyObject* __register(PyObject *, PyObject* args) {
    Py_buffer pipe_obj, inst_obj;
    PyArg_ParseTuple(args, "y*s*", &pipe_obj, &inst_obj);
    Pipe *pipe = (Pipe*)pipe_obj.buf;
    if(strcmp((char*)inst_obj.buf, "NULL_PID") == 0) {
        if(pipe->pid != NULL_PID) {
            pipe->pid = NULL_PID;
            mp_request_init(*pipe);
        }
        else
            PyErr_SetString(PyExc_BrokenPipeError, "The pipe has already been deleted.");
    }
    else if(strcmp((char*)inst_obj.buf, "EQUAL") == 0) {
        // if(pipe->pid != getpid())
            mp_request_init(*pipe);
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
    mp_request_init(*pipe);
    Pipe_info &info = *(Pipe_info*)pipe->info;
    PyObject *result = PyList_New(0);
collect_time
    pythread_mutex_lock(&info.M_mutex_t[0])
    while(info.read_request(pipe->info + pipe->buf_offset, result));
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
    Pipe_info &info = *(Pipe_info*)pipe->info;
    uint64_t lookup = 0;
collect_time
    pythread_mutex_lock(&info.M_mutex_t[1]);
    do {
        lookup += info.write_request(pipe->info + pipe->buf_offset, data_obj.buf + lookup, data_obj.len - lookup);
    } while(lookup != data_obj.len);
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
    "generic_module",                                // Module name to use with Python import statements
    "Buffered pipe through shared memory / generic implementation ",  // Module description
    0,
    methods                                 // Structure that defines the methods of the module
};
 
PyMODINIT_FUNC PyInit_generic_module() {
    return PyModule_Create(&module);
}
