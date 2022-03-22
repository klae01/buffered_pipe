import pickle
import threading
import multiprocessing
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import resource_tracker
import copy
import collections
from typing import Tuple, Any

class connection:
    def recv_bytes(self) -> bytes: ...
    def send_bytes(self, data: bytes) -> None: ...
    def recv(self) -> Any: ...
    def send(self, item: bytes) -> None: ...

class SPipe(connection):
    lock = threading.RLock()
    def __init__(self, minimum_write = 64, size = 2 ** 25):
        with SPipe.lock:
            self.minimum_write = minimum_write
            self.size = size
            self.shm = SharedMemory(create=True, size = size)
            resource_tracker.unregister( self.shm._name, 'shared_memory')
            self.resource_owner = None
            self.Plock = multiprocessing.Lock()
            self.sem_a = multiprocessing.Semaphore(0)
            self.sem_f = multiprocessing.Semaphore(0)
            self.mode = None
            self.is_init = False
    def __read_request_bytes(self, length):
        result = b""
        if self.size < self.__pointer + length:
            result += self.shm.buf[self.__pointer:]
            length -= self.size - self.__pointer
            self.__pointer = 0
        result += self.shm.buf[self.__pointer:self.__pointer + length]
        self.__pointer += length
        return result
    def __read_request(self, length, result):
        # shared memory 
        if self.size < self.__pointer + length:
            result.append(bytes(self.shm.buf[self.__pointer:]))
            length -= self.size - self.__pointer
            self.__pointer = 0
        result.append(bytes(self.shm.buf[self.__pointer:self.__pointer + length]))
        self.__pointer += length
    def __write_request(self, data):
        if self.size <= self.__pointer + len(data):
            split = self.size - self.__pointer
            self.shm.buf[self.__pointer:] = data[:split]
            self.__pointer = len(data) - (self.size - self.__pointer)
            if self.__pointer:
                self.shm.buf[:self.__pointer] = data[split:]
        else:
            self.shm.buf[self.__pointer:self.__pointer + len(data)] = data
            self.__pointer += len(data)
    # def __write_request(self, data):
    #     if self.size < self.__pointer + len(data):
    #         split = self.size - self.__pointer
    #         self.shm.buf[self.__pointer:] = data[:split]
    #         data = data[split:]
    #         self.__pointer = 0
    #     self.shm.buf[self.__pointer:self.__pointer + len(data)] = data
    #     self.__pointer += len(data)
    @staticmethod
    def get_id():
        # return multiprocessing.current_process().ident
        return (multiprocessing.current_process().ident, threading.current_thread().ident)
    def init(self):
        if self.resource_owner == None:
            self.resource_owner = SPipe.get_id()
        assert self.resource_owner == SPipe.get_id()
        if not self.is_init:
            self.__pointer = 0
            self.lock = threading.RLock()
            if self.mode == 'W':
                self.deque = collections.deque()
                self.margin = self.size
            self.is_init = True

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ['R', 'W']
        self.mode = mode
    
    def recv_bytes(self):
        assert 'R' == self.mode
        self.init()
        FLAG = True
        result = []
        with self.lock:
            while FLAG:
                self.sem_a.acquire()
                length = self.__read_request_bytes(4)
                # print(f"recv length info = ", length)
                length = int.from_bytes(length, 'big', signed = True)
                # print(f"recv length = {length}, sem_a = {self.sem_a.get_value()}, sem_f = {self.sem_f.get_value()} ")
                self.__read_request(abs(length), result)
                FLAG = length < 0
                self.sem_f.release()
        # print(f"recv pointer = {self.__pointer}, data_len = {sum(map(len, result))}, data_spans = {len(result)}")
        # print(f"recv data = ", b''.join(result))
        return b''.join(result)
        
    def send_bytes(self, data):
        assert 'W' == self.mode
        self.init()
        with self.lock:
            data_remain = len(data)
            data_pointer = 0
            while(data_remain):
                critical_length = data_remain + 4
                while(self.margin < critical_length and self.sem_f.acquire(block=False)):
                    self.margin += self.deque.popleft()
                loop_end_condition = min(self.minimum_write, critical_length)
                while(self.margin < loop_end_condition):
                    self.sem_f.acquire()
                    self.margin += self.deque.popleft()
                write_length = min(self.margin, critical_length)
                self.deque.append(write_length)
                self.margin -= write_length

                write_length -= 4
                data_info = write_length if data_remain == write_length else -write_length
                self.__write_request(data_info.to_bytes(4, 'big', signed=True))
                self.__write_request(data[data_pointer:data_pointer+write_length])
                data_remain -= write_length
                data_pointer += write_length

                self.sem_a.release()
        # print(f"send margin = {self.margin}, pointer = {self.__pointer}, data_len = {len(data)}")
        # mem_state = ' '.join(f'{n:02x}' for n in self.shm.buf)
        # print(f"send memory change: {mem_state}")
    def recv(self) -> Any:
        return pickle.loads(self.recv_bytes())
    def send(self, item):
        self.send_bytes(pickle.dumps(item))

    def __del__(self):
        with self.Plock:
            if self.resource_owner == self.get_id() and self.mode == 'R':
                print("DELETE")
                self.shm.close()

class BPipe(connection):
    def __init__(self, minimum_write=64, size=2 ** 25):
        self.pipe_1 = SPipe(minimum_write, size)
        self.pipe_2 = SPipe(minimum_write, size)
        self.is_init = False
        self.mode = None
    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ['RW', 'WR']
        self.mode = mode
    def init(self):
        if not self.is_init:
            list(map([self.pipe_1.set_mode, self.pipe_2.set_mode],self.mode))
    def recv_bytes(self):
        self.init()
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == 'W']
        return pipe.recv_bytes()
    def send_bytes(self, data):
        self.init()
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == 'R']
        pipe.send_bytes(data)
    def recv(self):
        return pickle.loads(self.recv_bytes())
    def send(self, item):
        self.send_bytes(pickle.dumps(item))
    

def Pipe(duplex : bool = True, minimum_write : int = 64, size : int = 2 ** 25) -> Tuple[connection, connection]:
    if duplex:
        pipe_1 = BPipe(minimum_write, size)
        pipe_2 = copy.copy(pipe_1)
        pipe_1.set_mode('RW')
        pipe_2.set_mode('WR')
        return pipe_1, pipe_2
    else:
        pipe_r = SPipe(minimum_write, size)
        pipe_w = copy.copy(pipe_r)
        pipe_r.set_mode('R')
        pipe_w.set_mode('W')
        return pipe_r, pipe_w