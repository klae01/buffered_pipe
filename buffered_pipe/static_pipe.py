from typing import Tuple

from .utils import get_duplex_Pipe
from .static_module import init, free, recv_bytes, send_bytes


class _Pipe:
    def __init__(self, object_size, object_count):
        self.fd_pipe = init((object_size, object_count))
        self.recv = self.recv_bytes
        self.send = self.send_bytes

    def recv_bytes(self) -> bytes:
        self.fd_pipe, result = recv_bytes(self.fd_pipe)
        return result

    def send_bytes(self, data: bytes) -> None:
        self.fd_pipe = send_bytes((self.fd_pipe, data))

    def __del__(self):
        if self.fd_pipe:
            self.fd_pipe = free(self.fd_pipe)

def Pipe(
    object_size: int, object_count: int, duplex = False
) -> Tuple[_Pipe, _Pipe]:
    assert object_size > 0
    assert object_count > 0
    if duplex:
        get_duplex_Pipe(lambda : _Pipe(object_size, object_count))
    else:
        pipe = _Pipe(object_size, object_count)
        return pipe, pipe