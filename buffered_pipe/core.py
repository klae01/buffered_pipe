import copy
import multiprocessing
import pickle
import random
import threading
from typing import Any, Tuple

from .module import create_sem, delete_sem, attach_sem, detach_sem
from .module import create_shm, delete_shm, attach_shm, detach_shm
from .module import recv_bytes, send_bytes
from .module import getpid


def _random_string(length=5, use_upper=False, use_lower=True, use_number=True):
    set = []
    if use_upper:
        set += list(range(ord("A"), ord("Z") + 1))
    if use_lower:
        set += list(range(ord("a"), ord("z") + 1))
    if use_number:
        set += list(range(ord("0"), ord("9") + 1))
    return "".join(map(chr, random.choices(set, k=length)))


def _random_sema_create(format):
    while True:
        s = _random_string(5, use_upper=False, use_lower=True, use_number=True)
        X = format.format(s)
        if not create_sem((X,)):
            break
    return X


class connection:
    def recv_bytes(self) -> bytes:
        ...

    def send_bytes(self, data: bytes) -> None:
        ...

    def recv(self) -> Any:
        ...

    def send(self, item: bytes) -> None:
        ...


class SPipe(connection):
    lock = threading.Lock()

    def __init__(self, minimum_write, size):
        with SPipe.lock:
            self.minimum_write = minimum_write
            self.shm_size = size

            self.shm_id = create_shm((self.shm_size,))
            print("shm_id", self.shm_id)

            sema_fn_base = f"BP_{getpid()}_{id(self)}"
            self.sema1_fn = _random_sema_create(sema_fn_base + "_1_{}.sema")
            self.sema2_fn = _random_sema_create(sema_fn_base + "_2_{}.sema")
            print(self.sema1_fn)
            print(self.sema2_fn)

            self.shm_ptr = None
            self.sema1_ptr = None
            self.sema2_ptr = None
            self.resource_owner = None

            self.mode = None

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["R", "W"]

        self.mode = mode
        if self.mode == "R":
            self.pointer = 0
        elif self.mode == "W":
            self.pointer_a = 0
            self.pointer_f = 0
            self.margin = self.shm_size

        self.lock = multiprocessing.Lock()

    def init(self):
        current_id = getpid()
        if self.resource_owner != current_id:
            print("SPIPE ID", current_id)
            self.resource_owner = current_id
            assert self.mode is not None

            self.shm_ptr = attach_shm((self.shm_id,))
            self.sema1_ptr = attach_sem((self.sema1_fn,))
            self.sema2_ptr = attach_sem((self.sema2_fn,))

            print("shm_ptr:", self.shm_ptr)
            print("sem1_ptr:", self.sema1_ptr)
            print("sem2_ptr:", self.sema2_ptr)

    def recv_bytes(self):
        # shm_buf, shm_size, sem_a, sem_f, pointer
        # return pointer, data
        assert "R" == self.mode
        with self.lock:
            self.init()
            self.pointer, data = recv_bytes(
                (
                    self.shm_ptr,
                    self.shm_size,
                    self.sema1_ptr,
                    self.sema2_ptr,
                    self.pointer,
                )
            )
        return b"".join(data)

    def send_bytes(self, data):
        # shm_buf, shm_size, sem_a, sem_f, pointer_a, pointer_f, margin, data
        # return pointer_a, pointer_f, margin
        assert "W" == self.mode
        with self.lock:
            self.init()
            self.pointer_a, self.pointer_f, self.margin = send_bytes(
                (
                    self.shm_ptr,
                    self.shm_size,
                    self.sema1_ptr,
                    self.sema2_ptr,
                    self.minimum_write,
                    self.pointer_a,
                    self.pointer_f,
                    self.margin,
                    data,
                )
            )

    def recv(self):
        return pickle.loads(self.recv_bytes())

    def send(self, item):
        self.send_bytes(pickle.dumps(item))

    def __del__(self):
        if self.resource_owner == getpid():
            if self.shm_ptr:
                detach_shm((self.shm_ptr,))
                self.shm_ptr = None
            if self.shm_id:
                delete_shm((self.shm_id,))
                self.shm_id = None
            if self.sema1_ptr:
                detach_sem((self.sema1_ptr,))
                self.sema1_ptr = None
            if self.sema1_fn:
                delete_sem((self.sema1_fn,))
                self.sema1_fn = None
            if self.sema2_ptr:
                detach_sem((self.sema2_ptr,))
                self.sema2_ptr = None
            if self.sema2_fn:
                delete_sem((self.sema2_fn,))
                self.sema2_fn = None


class BPipe(connection):
    def __init__(self, minimum_write, size):
        self.pipe_1 = SPipe(minimum_write, size)
        self.pipe_2 = SPipe(minimum_write, size)
        self.mode = None

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["RW", "WR"]
        self.mode = mode
        self.pipe_1.set_mode(self.mode[0])
        self.pipe_2.set_mode(self.mode[1])

    def recv_bytes(self):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv_bytes()

    def send_bytes(self, data):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "R"]
        pipe.send_bytes(data)

    def recv(self):
        return pickle.loads(self.recv_bytes())

    def send(self, item):
        self.send_bytes(pickle.dumps(item))


def Pipe(
    duplex: bool = True, minimum_write: int = 64, size: int = 2 ** 16
) -> Tuple[connection, connection]:
    assert minimum_write <= size
    if duplex:
        pipe_1 = BPipe(minimum_write, size)
        pipe_2 = copy.copy(pipe_1)
        pipe_1.set_mode("RW")
        pipe_2.set_mode("WR")
        return pipe_1, pipe_2
    else:
        pipe_r = SPipe(minimum_write, size)
        pipe_w = copy.copy(pipe_r)
        pipe_r.set_mode("R")
        pipe_w.set_mode("W")
        return pipe_r, pipe_w
