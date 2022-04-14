from __future__ import annotations

import copy
from typing import Tuple

from .utils import get_duplex_Pipe, UUID
from .static_module import init, free, register, recv_bytes, send_bytes


class _Pipe:
    def __init__(self, object_size, object_count, SMT_recv, SMT_send, polling):
        assert object_size > 0
        assert object_count > 0
        assert SMT_recv == SMT_send  # version 0.1.0
        assert SMT_recv >= 0
        assert SMT_send >= 0
        assert polling < 1
        SMT_recv = min(object_count, SMT_recv)
        SMT_send = min(object_count, SMT_send)
        SMT_recv = 0 if SMT_recv <= 1 else SMT_recv
        SMT_send = 0 if SMT_send <= 1 else SMT_send
        self.fd_pipe = None
        self.fd_pipe = init((object_size, object_count, SMT_recv, polling, UUID()))
        self.recv = self.recv_bytes
        self.send = self.send_bytes

    def recv_bytes(self) -> bytes:
        return recv_bytes(self.fd_pipe)

    def send_bytes(self, data: bytes) -> None:
        send_bytes((self.fd_pipe, data))

    def fork(self) -> _Pipe:
        new = copy.deepcopy(self)
        register((new.fd_pipe, "NULL_PID"))
        return new
    
    def register(self) -> None:
        register((self.fd_pipe, "EQUAL"))

    def __del__(self):
        if self.fd_pipe:
            free(self.fd_pipe)

def Pipe(
    object_size: int,
    object_count: int,
    SMT_recv: int = 16,
    SMT_send: int = 16,
    polling: float = 0.1,
    duplex: bool = False,
) -> Tuple[_Pipe, _Pipe]:
    if duplex:
        get_duplex_Pipe(
            lambda: _Pipe(object_size, object_count, SMT_recv, SMT_send, polling)
        )
    else:
        pipe = _Pipe(object_size, object_count, SMT_recv, SMT_send, polling)
        return pipe, pipe.fork()
