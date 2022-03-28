import pickle
from typing import Any, Tuple

from .utils import get_duplex_Pipe
from .generic_module import init, free, recv_bytes, send_bytes


class connection:
    def recv_bytes(self) -> bytes:
        ...

    def send_bytes(self, data: bytes) -> None:
        ...

    def recv(self) -> Any:
        ...

    def send(self, item: bytes) -> None:
        ...


class _Pipe(connection):
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

def Pipe(
    minimum_write: int = 64, size: int = 2 ** 16, duplex: bool = False
) -> Tuple[connection, connection]:
    assert minimum_write <= size
    if duplex:
        get_duplex_Pipe(lambda : _Pipe(minimum_write, size))
    else:
        pipe = _Pipe(minimum_write, size)
        return pipe, pipe
