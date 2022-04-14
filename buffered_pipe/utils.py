from __future__ import annotations

import copy
import os
import struct
import time
from typing import Any


class Duplex_Pipe:
    def __init__(self, P1, P2, mode = None):
        self.pipe_1 = P1
        self.pipe_2 = P2
        self.mode = mode

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["RW", "WR"]
        self.mode = mode

    def recv_bytes(self) -> bytes:
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv_bytes()

    def send_bytes(self, data: bytes) -> None:
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "R"]
        pipe.send_bytes(data)

    def recv(self) -> Any:
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv()

    def send(self, item: Any) -> None:
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "R"]
        pipe.send(item)

    def fork(self) -> Duplex_Pipe:
        return Duplex_Pipe(self.pipe_1.fork(), self.pipe_2.fork(), copy.deepcopy(self.mode))
    
    def register(self) -> None:
        self.pipe_1.register()
        self.pipe_2.register()

def get_duplex_Pipe(create_fn):
    pipe_1 = Duplex_Pipe(create_fn(), create_fn())
    pipe_2 = pipe_1.fork()
    pipe_1.set_mode("RW")
    pipe_2.set_mode("WR")
    return pipe_1, pipe_2

def UUID():
    data = struct.pack("d", time.time())
    return data + os.urandom(16 - len(data))
