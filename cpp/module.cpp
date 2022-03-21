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
    int shm_size;
    char *shm_buf;

public : 
    sem_t *sem_a; // allocate
    sem_t *sem_f; // free
    int pointer;
    int margin;
    int minimum_write;
    std::deque<int>deque;
    
    Pipe(char* sem_name1, char* sem_name2, key_t shm_key, int shm_size, int minimum_write) ;
    char* read_request(char* buf, int length) ;
    void read_request(int length) ;
    void write_request(char* data, int length) ;
};

Pipe::Pipe(char* sem_name1, char* sem_name2, int _shm_id, int _shm_size, int _minimum_write)
{
    pointer = 0;
    minimum_write = _minimum_write;
    shm_size = _shm_size;
    margin = shm_size;
    shm_id = _shm_id;
    if ( ( shm_buf = (char*)shmat( shm_id , NULL , 0 ) ) == (char *)-1) {
        // printf("Error attaching shared memory id");
        exit(1);
    }
    sem_a = sem_open(sem_name1, O_CREAT, 0644);
    sem_f = sem_open(sem_name2, O_CREAT, 0644);
}
char* Pipe::read_request(char* buf, int length) {
    int lookup = 0;
    if(shm_size < pointer + length) {
        length -= shm_size - pointer;
        for(; pointer < shm_size;)
            buf[lookup++] = shm_buf[pointer++];
        pointer = 0;
    }
    for(; length; length--)
        buf[lookup++] = shm_buf[pointer++];
    return buf;
}
void Pipe::read_request(int length) {
    if(shm_size < pointer + length) {
        length -= shm_size - pointer;
        for(; pointer < shm_size; pointer++ )
            buffer.push_back(shm_buf[pointer]);
        pointer = 0;
    }
    for(; length; pointer++, length-- )
        buffer.push_back(shm_buf[pointer]);
}
void Pipe::write_request(char* data, int length) {
    int lookup = 0;
    // printf("write_request 1 | pointer = %d | lookup = %d | length = %d | size %d \n", pointer, lookup, length, shm_size );
    if(shm_size < pointer + length) {
        length -= shm_size - pointer;
        for(; pointer != shm_size;)
            shm_buf[pointer++] = data[lookup++];
        pointer = 0;
    }
    // printf("write_request 2 | pointer = %d | lookup = %d | length = %d \n", pointer, lookup, length);
    for(; length; length-- )
        shm_buf[pointer++] = data[lookup++];
    // printf("write_request 3 | pointer = %d | lookup = %d | length = %d \n", pointer, lookup, length);
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
    int shm_size, shm_id;
    PyArg_ParseTuple(args, "i", &shm_size);
    shm_id = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0666);
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
    int shm_id, shm_size, minimum_write;
    PyArg_ParseTuple(args, "ssiii", &sem1, &sem2, &shm_id, &shm_size, &minimum_write);
    pipe_vector.push_back(Pipe(sem1, sem2, shm_id, shm_size, minimum_write));
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
    int critical_length, write_length;
    while(req_len) {
        // printf("%d\n", req_len);
        critical_length = req_len + 4;
        int sem_val;
        sem_getvalue(P.sem_f, &sem_val);
        // printf("send 1 deque = %d, sema = %d, margin = %d\n", P.deque.size(), sem_val, P.margin);
        while(P.margin < critical_length && !sem_trywait(P.sem_f)) {
            P.margin += P.deque.front();
            P.deque.pop_front();
        }
        // printf("send 2, margin = %d, remain = %d\n", P.margin, critical_length);
        Py_BEGIN_ALLOW_THREADS 
        while(P.margin < critical_length && P.margin < P.minimum_write) {
            sem_wait(P.sem_f);
            P.margin += P.deque.front();
            P.deque.pop_front();
        }
        Py_END_ALLOW_THREADS 
        // printf("send 3 pointer = %d\n", P.pointer);
        
        write_length = std::min(P.margin, critical_length) - 4;
        if( write_length < req_len )
            P.write_request(IntTochars(-write_length), 4);
        else
            P.write_request(IntTochars(write_length), 4);
        // printf("send 4\n");
        P.write_request(req_data, write_length);
        // printf("send 5\n");
        P.deque.push_back(write_length + 4);
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
    int sem_val;
    sem_getvalue(P.sem_a, &sem_val);
    // printf("recv 1 semaphore: %d\n", sem_val);
    while(FLAG) {
        Py_BEGIN_ALLOW_THREADS 
        sem_wait(P.sem_a);
        Py_END_ALLOW_THREADS 
        int length = charsToInt(P.read_request(buf, 4));
        // printf("recv 2 length = %d\n", length);
        P.read_request(std::abs(length));
        FLAG = length < 0;

        // printf("recv reading flag = %d\n", FLAG);
        sem_post(P.sem_f);
        // printf("recv sema post \n");
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
