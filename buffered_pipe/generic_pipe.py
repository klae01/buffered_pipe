from __future__ import annotations

import copy
import pickle
from typing import Tuple, Any

from .utils import get_duplex_Pipe
from .generic_module import init, free, register, recv_bytes, send_bytes


class _Pipe:
    def __init__(self, minimum_write, buffer_size, SMT_recv, SMT_send, polling):
        assert minimum_write > 0
        assert buffer_size > 1
        assert SMT_recv == SMT_send  # version 0.1.0
        assert SMT_recv >= 0
        assert SMT_send >= 0
        assert polling < 1
        SMT_recv = min(buffer_size // 2, SMT_recv)
        SMT_send = min(buffer_size // 2, SMT_send)
        SMT_recv = 0 if SMT_recv <= 1 else SMT_recv
        SMT_send = 0 if SMT_send <= 1 else SMT_send
        self.fd_pipe = init((minimum_write, buffer_size, SMT_recv, polling))
        self.recv = self.recv_bytes
        self.send = self.send_bytes

    def recv_bytes(self) -> bytes:
        return b"".join(recv_bytes(self.fd_pipe))

    def send_bytes(self, data: bytes) -> None:
        send_bytes((self.fd_pipe, data))

    def recv(self) -> Any:
        return pickle.loads(self.recv_bytes())

    def send(self, item) -> None:
        self.send_bytes(pickle.dumps(item))

    def fork(self) -> _Pipe:
        new = copy.deepcopy(self)
        register((new.fd_pipe, "NULL_PID"))
        return new
    
    def register(self) -> None:
        register((self.fd_pipe, "EQUAL"))

    def __del__(self):
        free(self.fd_pipe)


def Pipe(
    minimum_write: int,
    buffer_size: int,
    SMT_recv: int = 16,
    SMT_send: int = 16,
    polling: float = 0.1,
    duplex: bool = False,
) -> Tuple[_Pipe, _Pipe]:
    if duplex:
        get_duplex_Pipe(
            lambda: _Pipe(minimum_write, buffer_size, SMT_recv, SMT_send, polling)
        )
    else:
        pipe = _Pipe(minimum_write, buffer_size, SMT_recv, SMT_send, polling)
        return pipe, pipe
