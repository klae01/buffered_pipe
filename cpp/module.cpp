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

std::vector<char> buffer(1<<14);
union intc32 {
    char c[4];
    int v;
} intc32_value;
int charsToInt(char *data) {
    std::memcpy(intc32_value.c, data, 4);
    return intc32_value.v;
}
char* IntTochars(int data) {
    intc32_value.v = data;
    return intc32_value.c;
}

class Pipe {
private:
    int shm_id;
    int size;
    int pointer;
    char *shm_buf;

public : 
    sem_t *sem_a; // allocate
    sem_t *sem_f; // free
    int margin;
    int minimum_write;
    std::deque<int>deque;
    
    Pipe(char* sem_name1, char* sem_name2, key_t shm_key, int size, int minimum_write) ;
    char* read_request(char* buf, int length) ;
    void read_request(int length) ;
    void write_request(char* data, int length) ;
};

Pipe::Pipe(char* sem_name1, char* sem_name2, int _shm_id, int _size, int _minimum_write)
{
    pointer = 0;
    minimum_write = _minimum_write;
    size = _size;
    margin = size;
    shm_id = _shm_id;
    if ( ( shm_buf = (char*)shmat( shm_id , NULL , 0 ) ) == (char *)-1) {
        printf("Error attaching shared memory id");
        exit(1);
    }
    sem_a = sem_open(sem_name1, O_CREAT, 0644);
    sem_f = sem_open(sem_name2, O_CREAT, 0644);
}
char* Pipe::read_request(char* buf, int length) {
    int lookup = 0;
    if(size < pointer + length) {
        length -= size - pointer;
        for(; pointer < size; pointer++, lookup++ )
            buf[lookup] = shm_buf[pointer];
        pointer = 0;
    }
    for(; length; pointer++, length--, lookup++ )
        buf[lookup] = shm_buf[pointer];
    return buf;
}
void Pipe::read_request(int length) {
    if(size < pointer + length) {
        length -= size - pointer;
        for(; pointer < size; pointer++ )
            buffer.push_back(shm_buf[pointer]);
        pointer = 0;
    }
    for(; length; pointer++, length-- )
        buffer.push_back(shm_buf[pointer]);
}
void Pipe::write_request(char* data, int length) {
    int lookup = 0;
    if(size < pointer + length) {
        length -= size - pointer;
        for(; pointer < size; pointer++, lookup++ )
            shm_buf[pointer] = data[lookup];
        pointer = 0;
    }
    for(; length; pointer++, length--, lookup++ )
        shm_buf[pointer] = data[lookup];
}

std::vector<Pipe> pipe_vector;

PyObject* init_sema(PyObject *, PyObject* args) {
    char *sem_name;
    PyArg_ParseTuple(args, "s", &sem_name);
    sem_t *sem = sem_open(sem_name, O_CREAT, 0644, 0);
    if(!sem) sem_close(sem);
    return PyLong_FromUnsignedLongLong(sem == SEM_FAILED);
}
PyObject* create_shm(PyObject *, PyObject *args) {
    // sem1, sem2, shm_id, shm_size, minimum_write 
    int size, shm_id;
    PyArg_ParseTuple(args, "i", &size);
    shm_id = shmget(IPC_PRIVATE, size, IPC_CREAT | 0666);
    return PyLong_FromUnsignedLongLong(shm_id);
}
PyObject* delete_shm(PyObject *, PyObject *args) {
    // sem1, sem2, shm_id, shm_size, minimum_write 
    int shm_id, result;
    PyArg_ParseTuple(args, "i", &shm_id);
    result = shmctl(shm_id, IPC_RMID, NULL);
    return PyLong_FromUnsignedLongLong(result);
}
PyObject* create_pipe(PyObject *, PyObject *args) {
    // sem1, sem2, shm_id, shm_size, minimum_write 
    unsigned long long id = pipe_vector.size();
    char *sem1, *sem2;
    int shm_id, size, minimum_write;
    PyArg_ParseTuple(args, "ssiii", &sem1, &sem2, &shm_id, &size, &minimum_write);
    pipe_vector.push_back(Pipe(sem1, sem2, shm_id, size, minimum_write));
    return PyLong_FromUnsignedLongLong(id);
}
PyObject* send_bytes(PyObject *, PyObject *args) {
    // pipe_id, data
    char*      req_data;
    Py_ssize_t req_len;
    int pipe_id;
    PyArg_ParseTuple(args, "iy#", &pipe_id, &req_data, &req_len);
    // printf("pipes= %d\n", pipe_vector.size());
    class Pipe& P = pipe_vector[pipe_id];
    int remain_length, write_length;
    while(req_len) {
        // printf("%d\n", req_len);
        remain_length = req_len + 4;
        // printf("send 1\n");
        int sem_val;
        // printf("%p, %p\n", P.sem_a, P.sem_f);
        sem_getvalue(P.sem_f, &sem_val);
        // printf("deque = %d sema = %d\n", P.deque.size(), sem_val);
        while(P.margin < remain_length && !sem_trywait(P.sem_f)) {
            P.margin += P.deque.front();
            P.deque.pop_front();
        }
        // printf("send 2\n");
        while(P.margin < remain_length && P.margin < P.minimum_write) {
            sem_wait(P.sem_f);
            P.margin += P.deque.front();
            P.deque.pop_front();
        }
        // printf("send 3\n");
        
        write_length = std::min(P.margin, remain_length) - 4;
        if( write_length < req_len )
            P.write_request(IntTochars(-write_length), 4);
        else
            P.write_request(IntTochars(write_length), 4);
        // printf("send 4\n");
        P.write_request(req_data, write_length);
        P.deque.push_back(write_length);
        req_data += write_length;
        P.margin -= write_length + 4;
        req_len -= write_length;
        sem_post(P.sem_a);
    }
    // printf("send finish\n");
    return PyLong_FromUnsignedLongLong(0);
}
PyObject* recv_bytes(PyObject *, PyObject* args) {
    // pipe_id
    int pipe_id;
    PyArg_ParseTuple(args, "i", &pipe_id);
    class Pipe& P = pipe_vector[pipe_id];
    char buf[5];
    int FLAG = true;
    buffer.resize(0);
    // printf("recv start\n");
    while(FLAG) {
        sem_wait(P.sem_a);
        int length = charsToInt(P.read_request(buf, 4));
        P.read_request(std::abs(length));
        sem_post(P.sem_f);
        FLAG = length < 0;
    }
    // printf("recv finish\n");
    return PyBytes_FromStringAndSize(&buffer[0], buffer.size());
}
 
static PyMethodDef methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "init_sema", (PyCFunction)init_sema, METH_O, nullptr },
    { "create_shm", (PyCFunction)create_shm, METH_O, nullptr },
    { "delete_shm", (PyCFunction)delete_shm, METH_O, nullptr },
    { "create_pipe", (PyCFunction)create_pipe, METH_O, nullptr },
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
