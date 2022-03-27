import copy
import multiprocessing
import pickle
import random
import threading
from typing import Any, Tuple

from .module import init, free, recv_bytes, send_bytes


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
    def __init__(self, minimum_write, size):
        self.fd_pipe = init((minimum_write, size))

    def recv_bytes(self):
        self.fd_pipe, result = recv_bytes(self.fd_pipe)
        return b''.join(result)

    def send_bytes(self, data):
        self.fd_pipe = send_bytes((self.fd_pipe, data))

    def recv(self):
        return pickle.loads(self.recv_bytes())

    def send(self, item):
        self.send_bytes(pickle.dumps(item))

    def __del__(self):
        if self.fd_pipe:
            self.fd_pipe = free(self.fd_pipe)


class BPipe(connection):
    def __init__(self, minimum_write, size):
        self.pipe_1 = SPipe(minimum_write, size)
        self.pipe_2 = SPipe(minimum_write, size)
        self.mode = None

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["RW", "WR"]
        self.mode = mode

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
        pipe = SPipe(minimum_write, size)
        return pipe, pipe
