from typing import Tuple

from .utils import get_duplex_Pipe
from .static_module import init, free, recv_bytes, send_bytes


class _Pipe:
    def __init__(self, object_size, object_count, concurrency, polling):
        assert object_size > 0
        assert object_count > 0
        assert concurrency > 0
        assert polling < 1
        if concurrency == 1:
            concurrency = 0
        self.fd_pipe = init((object_size, object_count, concurrency, polling))
        self.recv = self.recv_bytes
        self.send = self.send_bytes

    def recv_bytes(self) -> bytes:
        return recv_bytes(self.fd_pipe)

    def send_bytes(self, data: bytes) -> None:
        send_bytes((self.fd_pipe, data))

    def __del__(self):
        free(self.fd_pipe)

def Pipe(
    object_size: int, object_count: int, concurrency: int = 16, polling: float = 0.1, duplex = False
) -> Tuple[_Pipe, _Pipe]:
    concurrency = min(object_count, concurrency)
    if duplex:
        get_duplex_Pipe(lambda : _Pipe(object_size, object_count, concurrency, polling))
    else:
        pipe = _Pipe(object_size, object_count, concurrency, polling)
        return pipe, pipe