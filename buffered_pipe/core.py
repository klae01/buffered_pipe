import os
import pickle
import tempfile
import threading
import copy
import time
import random

from .module import init_sema, create_shm, delete_shm, create_pipe, send_bytes, recv_bytes

def __random_string(length = 5, use_upper = False, use_lower = True, use_number = True):
    set = []
    if use_upper: set += list(range(ord('A'),ord('Z') + 1))
    if use_lower: set += list(range(ord('a'),ord('z') + 1))
    if use_number: set += list(range(ord('0'),ord('9') + 1))
    return ''.join(map(chr, random.choices(set, k = length)))
class SPipe:
    lock = threading.RLock()
    @staticmethod
    def __random_string(length = 5, use_upper = False, use_lower = True, use_number = True):
        set = []
        if use_upper: set += list(range(ord('A'),ord('Z') + 1))
        if use_lower: set += list(range(ord('a'),ord('z') + 1))
        if use_number: set += list(range(ord('0'),ord('9') + 1))
        return ''.join(map(chr, random.choices(set, k = length)))
    @staticmethod
    def __random_sema_create(format):
        while True:
            s = SPipe.__random_string(5, use_upper = False, use_lower = True, use_number = True)
            X = format.format(s)
            if not init_sema((X, )):
                break
        return X
    def __init__(self, minimum_write = 64, size = 2 ** 25):
        with SPipe.lock:
            self.shm_id = create_shm((size, ))
            print("shm_id", self.shm_id)
            self.size = size

            self.minimum_write = minimum_write
            sema_fn_base = f"{os.getpid()}_{id(self)}"
            self.sema1_fn = SPipe.__random_sema_create(sema_fn_base+"_1_{}.sema")
            self.sema2_fn = SPipe.__random_sema_create(sema_fn_base+"_2_{}.sema")
            print(self.sema1_fn)
            print(self.sema2_fn)
            self.pipe_id = None
            self.mode = None
    def set_mode(self, mode):
        assert mode in ['R', 'W']
        self.mode = mode
    def init(self):
        if self.pipe_id is None:
            assert self.mode is not None
            self.lock = threading.RLock()
            self.pipe_id = create_pipe((self.sema1_fn, self.sema2_fn, self.shm_id, self.size, self.minimum_write))
    def recv_bytes(self):
        assert 'R' == self.mode
        self.init()
        with self.lock:
            return recv_bytes((self.pipe_id, ))
    def send_bytes(self, data):
        assert 'W' == self.mode
        self.init()
        with self.lock:
            send_bytes((self.pipe_id, data))
    def recv(self):
        return pickle.loads(self.recv_bytes())
    def send(self, item):
        self.send_bytes(pickle.dumps(item))

class BPipe:
    def __init__(self, minimum_write=64, size=2 ** 25):
        self.pipe_1 = SPipe(minimum_write, size)
        self.pipe_2 = SPipe(minimum_write, size)
        self.is_init = False
    def set_mode(self, mode):
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
    

def Pipe(duplex : bool = True):
    if duplex:
        pipe_1 = BPipe()
        pipe_2 = copy.copy(pipe_1)
        pipe_1.set_mode('RW')
        pipe_2.set_mode('WR')
        return pipe_1, pipe_2
    else:
        pipe_r = SPipe()
        pipe_w = copy.copy(pipe_r)
        pipe_r.set_mode('R')
        pipe_w.set_mode('W')
        return pipe_r, pipe_w