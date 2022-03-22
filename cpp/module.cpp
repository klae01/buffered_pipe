#include <semaphore.h>
#include <unistd.h>
#include <Python.h>

#include <fcntl.h> 
#include <sys/ipc.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <sys/wait.h>
#include <vector>
#include <deque>
#include <cstring>
#include <string>
#include <iostream>

const int LENGTH_INFO = 4;
typedef long long BASE_TYPE;

#define PTR2LL(x) (long long)(uintptr_t)(x)
#define LL2PTR(x) (void *)(uintptr_t)(x)

// create_sem, delete_sem, attach_sem, detach_sem
// create_shm, delete_shm, attach_shm, detach_shm
// send_bytes, recv_bytes

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
    // // printf("semaphore %s: %p %llx\n ", sem_name, sem, PTR2LL(sem));
    return PyLong_FromLongLong(PTR2LL(sem));
}

PyObject* detach_sem(PyObject *, PyObject* args) {
    BASE_TYPE sem_ptr, result;
    PyArg_ParseTuple(args, "L", &sem_ptr);
    result = sem_close((sem_t*)LL2PTR(sem_ptr));
    return PyLong_FromLongLong(result);
}

PyObject* create_shm(PyObject *, PyObject* args) {
    BASE_TYPE shm_size, shm_id;
    PyArg_ParseTuple(args, "L", &shm_size);
	shm_id = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0666);
    return PyLong_FromLongLong(shm_id);
}

PyObject* delete_shm(PyObject *, PyObject* args) {
    BASE_TYPE shm_id, result;
    PyArg_ParseTuple(args, "L", &shm_id);
    result = shmctl(shm_id , IPC_RMID , NULL);
    return PyLong_FromLongLong(result);
}

PyObject* attach_shm(PyObject *, PyObject* args) {
    BASE_TYPE shmaddr, result;
    PyArg_ParseTuple(args, "L", &shmaddr);
    result = PTR2LL(shmat(shmaddr, NULL, 0));
    return PyLong_FromLongLong(result);
}

PyObject* detach_shm(PyObject *, PyObject* args) {
    BASE_TYPE shmaddr, result;
    PyArg_ParseTuple(args, "L", &shmaddr);
    result = shmdt(LL2PTR(shmaddr));
    return PyLong_FromLongLong(result);
}

BASE_TYPE read_request(char* shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer, BASE_TYPE length, std::deque<char> &result) {
    if(shm_size < pointer + length) {
        for(char *lookup = shm_buf + pointer, *end = shm_buf + shm_size; lookup != end;)
            result.push_back(*lookup++);
        length -= shm_size - pointer;
        pointer = 0;
    }
    // // // printf("read_request %p %p\n", shm_buf + pointer, shm_buf + pointer + length);
    // // printf("read_request %p %p %lld %lld\n", shm_buf + pointer, shm_buf + pointer + length, pointer, length);
    for(char *lookup = shm_buf + pointer, *end = shm_buf + pointer + length; lookup != end;)
        result.push_back(*lookup++);
    return pointer + length;
}

BASE_TYPE read_request(char* shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer, BASE_TYPE length, char*result) {
    BASE_TYPE lookup = 0;
    if(shm_size < pointer + length) {
        lookup = shm_size - pointer;
        std::memcpy(result, shm_buf + pointer, lookup);
        length -= lookup;
        pointer = 0;
    }
    std::memcpy(result + lookup, shm_buf + pointer, length);
    return pointer + length;
}

BASE_TYPE write_request(char* shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer, char* data, BASE_TYPE length) {
    BASE_TYPE tmp;
    if(shm_size < pointer + length) {
        tmp = shm_size - pointer;
        std::memcpy(shm_buf+pointer, data, tmp);
        data += tmp;
        length -= tmp;
        pointer = 0;
    }
    std::memcpy(shm_buf+pointer, data, length);
    return pointer + length;
}


union int2char {
    char c[8];
    long long v;
};
// BASE_TYPE charsToInt(char *data) {
//     int2char i2c_value;
//     std::memcpy(i2c_value.c, data, LENGTH_INFO);
//     return i2c_value.v;
// }
// char* IntTochars(BASE_TYPE data) {
//     int2char i2c_value;
//     i2c_value.v = data;
//     return i2c_value.c;
// }

BASE_TYPE chunk_size(char *shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer) {
    int2char i2c_value;
    i2c_value.v = 0;
    read_request(shm_buf, shm_size, pointer, LENGTH_INFO, i2c_value.c);
    return i2c_value.v;
}
BASE_TYPE abs_chunk_size(char *shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer) {
    return std::abs(chunk_size(shm_buf, shm_size, pointer));
}
BASE_TYPE write_int(char *shm_buf, BASE_TYPE shm_size, BASE_TYPE pointer, BASE_TYPE value) {
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
    BASE_TYPE _shm_buf, shm_size, _sem_a, _sem_f, minimum_write, pointer_a, pointer_f, margin;
    PyArg_ParseTuple(args, "LLLLLLLLy#", &_shm_buf, &shm_size, &_sem_a, &_sem_f, &minimum_write, &pointer_a, &pointer_f, &margin, &req_data, &req_len);
    char* shm_buf = (char*)LL2PTR(_shm_buf);
    sem_t *sem_a = (sem_t*)LL2PTR(_sem_a), *sem_f = (sem_t*)LL2PTR(_sem_f);
    
    BASE_TYPE critical_length, write_length, free_length, loop_end_condition;
    while(req_len) {
        // // printf("req_len = %4d LENGTH_INFO = %4d\n", (int)req_len, LENGTH_INFO);
        critical_length = req_len + LENGTH_INFO;
        // // printf("critical_length = %4d %4d\n", (int)critical_length, req_len + LENGTH_INFO);
        // std::cout<<critical_length<<' '<<req_len<<' '<<req_len + LENGTH_INFO<<std::endl;
        int sem_val;
        sem_getvalue(sem_f, &sem_val);
        // // printf("send 1 sema = %4d, margin = %4d\n", sem_val, (int)margin);
        // printf("send: ");
        // printf_hex(shm_buf, shm_size);
        while(margin < critical_length && !sem_trywait(sem_f)) {
            free_length = LENGTH_INFO + abs_chunk_size(shm_buf, shm_size, pointer_f);
            // std::cout<<"non blocking free = " << free_length<<std::endl;
            margin += free_length;
            pointer_f = (pointer_f + free_length) % shm_size;
        }
        loop_end_condition = std::min(critical_length, minimum_write);
        // // printf("send 2, margin = %4d, remain = %4d\n", (int)margin, (int)critical_length);
        while(margin < loop_end_condition) {
            Py_BEGIN_ALLOW_THREADS 
            sem_wait(sem_f);
            Py_END_ALLOW_THREADS 
            free_length = LENGTH_INFO + abs_chunk_size(shm_buf, shm_size, pointer_f);
            // std::cout<<"blocking free = " << free_length<<std::endl;
            margin += free_length;
            pointer_f = (pointer_f + free_length) % shm_size;
        }
        // // printf("send 3 pointer = %4d / %4d\n", (int)pointer_f, (int)pointer_a);
        
        write_length = std::min(margin, critical_length);
        // std::cout<<write_length<<"=min("<<margin<<", "<<critical_length<<")"<<std::endl;
        margin -= write_length;
        write_length -= LENGTH_INFO;
        if( write_length < req_len )
            pointer_a = write_int(shm_buf, shm_size, pointer_a, -write_length);
        else
            pointer_a = write_int(shm_buf, shm_size, pointer_a, write_length);
        // // printf("send 4 pointer_a = %lld write_length = %lld\n",  pointer_a, write_length);
        pointer_a = write_request(shm_buf, shm_size, pointer_a, req_data, write_length);
        // // printf("send 5 pointer_a = %lld\n",  pointer_a);
        req_data += write_length;
        req_len -= write_length;
        sem_post(sem_a);
    }
    // // printf("send finish\n");
    return Py_BuildValue("LLL", pointer_a, pointer_f, margin);
}
PyObject* recv_bytes(PyObject *, PyObject* args) {
    // shm_buf, shm_size, sem_a, sem_f, pointer
    // return pointer, data
    // // printf("enter\n");
    BASE_TYPE _shm_buf, shm_size, _sem_a, _sem_f, pointer;
    PyArg_ParseTuple(args, "LLLLL", &_shm_buf, &shm_size, &_sem_a, &_sem_f, &pointer);
    // printf("var init 0\n");
    char* shm_buf = (char*)LL2PTR(_shm_buf);
    sem_t *sem_a = (sem_t*)LL2PTR(_sem_a), *sem_f = (sem_t*)LL2PTR(_sem_f);
    // // printf("var init 1\n");

    int FLAG = true;
    std::deque<char> buffer;
    // printf("var init 2 sema = %p %llx\n", sem_a, _sem_a);
    int sem_val;
    sem_getvalue(sem_a, &sem_val);
    // printf("recv 1 semaphore: %4d pointer = %lld\n", sem_val, pointer);
    BASE_TYPE length;
    while(FLAG) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(sem_a);
        Py_END_ALLOW_THREADS 

        length = chunk_size(shm_buf, shm_size, pointer);
        pointer = (pointer + LENGTH_INFO) % shm_size;
        
        // printf("recv: ");
        // printf_hex(shm_buf, shm_size);

        // printf("recv 2 length = %4lld\n", length);
        pointer = read_request(shm_buf, shm_size, pointer, std::abs(length), buffer);
        FLAG = length < 0;

        // printf("recv reading flag = %d\n", FLAG);
        sem_post(sem_f);
        // printf("recv sema post \n");
    }
    // printf("recv finish\n");

    std::string output_string(buffer.begin(), buffer.end());
    return Py_BuildValue("Ly#", pointer, output_string.c_str(), (Py_ssize_t)output_string.size());
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
