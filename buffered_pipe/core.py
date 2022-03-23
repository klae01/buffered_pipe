import os
import pickle
import threading
import multiprocessing
import copy
import random
from typing import Tuple, Any

from .module import create_sem, delete_sem, attach_sem, detach_sem
from .module import create_shm, delete_shm, attach_shm, detach_shm
from .module import send_bytes, recv_bytes, get_id

def random_string(length = 5, use_upper = False, use_lower = True, use_number = True):
    set = []
    if use_upper: set += list(range(ord('A'),ord('Z') + 1))
    if use_lower: set += list(range(ord('a'),ord('z') + 1))
    if use_number: set += list(range(ord('0'),ord('9') + 1))
    return ''.join(map(chr, random.choices(set, k = length)))
class connection:
    def recv_bytes(self) -> bytes: ...
    def send_bytes(self, data: bytes) -> None: ...
    def recv(self) -> Any: ...
    def send(self, item: bytes) -> None: ...

class SPipe(connection):
    lock = threading.Lock()
    @staticmethod
    def __random_sema_create(format):
        while True:
            s = random_string(5, use_upper = False, use_lower = True, use_number = True)
            X = format.format(s)
            if not create_sem((X, )):
                break
        return X
    def __init__(self, minimum_write = 64, size = 2 ** 25):
        with SPipe.lock:
            self.minimum_write = minimum_write
            self.shm_size = size

            self.shm_id = create_shm((self.shm_size, ))
            print("shm_id", self.shm_id)

            sema_fn_base = f"BP_{os.getpid()}_{id(self)}"
            self.sema1_fn = SPipe.__random_sema_create(sema_fn_base+"_1_{}.sema")
            self.sema2_fn = SPipe.__random_sema_create(sema_fn_base+"_2_{}.sema")
            print(self.sema1_fn)
            print(self.sema2_fn)

            self.shm_ptr = None
            self.sema1_ptr = None
            self.sema2_ptr = None
            self.resource_owner = None

            self.mode = None
    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ['R', 'W']
        self.mode = mode
        
        if self.mode == 'R':
            self.pointer = 0
        elif self.mode == 'W':
            self.pointer_a = 0
            self.pointer_f = 0
            self.margin = self.shm_size
    def init(self):
        current_id = get_id()
        if self.resource_owner != current_id:
            with SPipe.lock:
                if self.resource_owner != current_id:
                    print("SPIPE ID", current_id)
                    self.resource_owner = current_id
                    assert self.mode is not None
                    self.lock = threading.Lock()

                    self.shm_ptr = attach_shm((self.shm_id,))
                    self.sema1_ptr = attach_sem((self.sema1_fn,))
                    self.sema2_ptr = attach_sem((self.sema2_fn,))
                    
                    print("shm_ptr:", self.shm_ptr)
                    print("sem1_ptr:", self.sema1_ptr)
                    print("sem2_ptr:", self.sema2_ptr)
                

    def recv_bytes(self):
        # shm_buf, shm_size, sem_a, sem_f, pointer
        # return pointer, data
        assert 'R' == self.mode
        self.init()
        with self.lock:
            self.pointer, data = recv_bytes((self.shm_ptr, self.shm_size, self.sema1_ptr, self.sema2_ptr, self.pointer))
        return b''.join(data)
    def send_bytes(self, data):
        # shm_buf, shm_size, sem_a, sem_f, pointer_a, pointer_f, margin, data
        # return margin, pointer_a, pointer_f
        assert 'W' == self.mode
        self.init()
        with self.lock:
            self.pointer_a, self.pointer_f, self.margin = send_bytes((self.shm_ptr, self.shm_size, self.sema1_ptr, self.sema2_ptr, self.minimum_write, self.pointer_a, self.pointer_f, self.margin, data))
    def recv(self):
        return pickle.loads(self.recv_bytes())
    def send(self, item):
        self.send_bytes(pickle.dumps(item))
    def __del__(self):
        detach_shm((self.shm_ptr, )); self.shm_ptr = None
        detach_sem((self.sema1_ptr, )); self.sema1_ptr = None
        detach_sem((self.sema2_ptr, )); self.sema2_ptr = None

        delete_shm((self.shm_id,)); self.shm_id = None
        delete_sem((self.sema1_fn,)); self.sema1_fn = None
        delete_sem((self.sema2_fn,)); self.sema2_fn = None

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
    assert minimum_write <= size
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