import pickle
from typing import Any, Tuple

from .utils import get_duplex_Pipe
from .generic_module import init, free, recv_bytes, send_bytes


class _Pipe:
    def __init__(self, minimum_write, buffer_size, concurrency, polling):
        assert minimum_write > 0
        assert buffer_size > 0
        assert minimum_write <= buffer_size
        assert concurrency > 0
        assert polling < 1
        if concurrency == 1:
            concurrency = 0
        self.fd_pipe = init((minimum_write, buffer_size, concurrency, polling))

    def recv_bytes(self) -> bytes:
        return b''.join(recv_bytes(self.fd_pipe))

    def send_bytes(self, data: bytes) -> None:
        send_bytes((self.fd_pipe, data))

    def recv(self) -> Any:
        return pickle.loads(self.recv_bytes())

    def send(self, item) -> None:
        self.send_bytes(pickle.dumps(item))

    def __del__(self):
        free(self.fd_pipe)

def Pipe(
    minimum_write: int = 64, buffer_size: int = 2 ** 16, concurrency: int = 16, polling: float = 0.1, duplex: bool = False
) -> Tuple[_Pipe, _Pipe]:
    if duplex:
        get_duplex_Pipe(lambda : _Pipe(minimum_write, buffer_size, concurrency, polling))
    else:
        pipe = _Pipe(minimum_write, buffer_size, concurrency, polling)
        return pipe, pipe
