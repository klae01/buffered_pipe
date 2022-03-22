import pickle
import threading
import multiprocessing
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import resource_tracker
import copy
import collections
from typing import Tuple, Any


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
    lock = threading.RLock()

    def __init__(self, minimum_write=64, size=2 ** 25):
        with SPipe.lock:
            self.minimum_write = minimum_write
            self.size = size
            self.shm = SharedMemory(create=True, size=size)
            resource_tracker.unregister(self.shm._name, "shared_memory")
            self.sem_a = multiprocessing.Semaphore(0)
            self.sem_f = multiprocessing.Semaphore(0)
            self.mode = None
            self.is_init = False

    def __read_request_bytes(self, length):
        result = b""
        if self.size < self.__pointer + length:
            result += self.shm.buf[self.__pointer :]
            length -= self.size - self.__pointer
            self.__pointer = 0
        result += self.shm.buf[self.__pointer : self.__pointer + length]
        self.__pointer += length
        return result

    def __read_request(self, length, result):
        # split of shared memory is mutable
        if self.size < self.__pointer + length:
            w_length = self.size - self.__pointer
            result.extend(self.shm.buf[self.__pointer :])
            length -= w_length
            self.__pointer = 0
        result.extend(self.shm.buf[self.__pointer : self.__pointer + length])
        self.__pointer += length

    def __write_request(self, data):
        if self.size <= self.__pointer + len(data):
            split = self.size - self.__pointer
            self.shm.buf[self.__pointer :] = data[:split]
            self.__pointer = len(data) - (self.size - self.__pointer)
            if self.__pointer:
                self.shm.buf[: self.__pointer] = data[split:]
        else:
            self.shm.buf[self.__pointer : self.__pointer + len(data)] = data
            self.__pointer += len(data)

    def init(self):
        if not self.is_init:
            self.__pointer = 0
            self.lock = threading.RLock()
            if self.mode == "W":
                self.deque = collections.deque()
                self.margin = self.size
            self.is_init = True

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["R", "W"]
        self.mode = mode

    def recv_bytes(self):
        assert "R" == self.mode
        self.init()
        FLAG = True
        result = collections.deque()
        with self.lock:
            while FLAG:
                self.sem_a.acquire()
                length = self.__read_request_bytes(4)
                length = int.from_bytes(length, "big", signed=True)
                self.__read_request(abs(length), result)
                FLAG = length < 0
                self.sem_f.release()
        return bytes(result)

    def send_bytes(self, data):
        assert "W" == self.mode
        self.init()
        with self.lock:
            data_remain = len(data)
            data_pointer = 0
            while data_remain:
                critical_length = data_remain + 4
                while self.margin < critical_length and self.sem_f.acquire(block=False):
                    self.margin += self.deque.popleft()
                loop_end_condition = min(self.minimum_write, critical_length)
                while self.margin < loop_end_condition:
                    self.sem_f.acquire()
                    self.margin += self.deque.popleft()
                write_length = min(self.margin, critical_length)
                self.deque.append(write_length)
                self.margin -= write_length

                write_length -= 4
                data_info = write_length * [-1, 1][data_remain == write_length]
                self.__write_request(data_info.to_bytes(4, "big", signed=True))
                self.__write_request(data[data_pointer : data_pointer + write_length])
                data_remain -= write_length
                data_pointer += write_length

                self.sem_a.release()

    def recv(self) -> Any:
        return pickle.loads(self.recv_bytes())

    def send(self, item):
        self.send_bytes(pickle.dumps(item))

    def __del__(self):
        if self.mode == "R" or self.mode is None:
            # print("DELETE")
            self.shm.close()


class BPipe(connection):
    def __init__(self, minimum_write=64, size=2 ** 25):
        self.pipe_1 = SPipe(minimum_write, size)
        self.pipe_2 = SPipe(minimum_write, size)
        self.is_init = False
        self.mode = None

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["RW", "WR"]
        self.mode = mode

    def init(self):
        if not self.is_init:
            self.pipe_1.set_mode(self.mode[0])
            self.pipe_2.set_mode(self.mode[1])
            self.is_init = True

    def recv_bytes(self):
        self.init()
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv_bytes()

    def send_bytes(self, data):
        self.init()
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "R"]
        pipe.send_bytes(data)

    def recv(self):
        return pickle.loads(self.recv_bytes())

    def send(self, item):
        self.send_bytes(pickle.dumps(item))


def Pipe(
    duplex: bool = True, minimum_write: int = 64, size: int = 2 ** 20
) -> Tuple[connection, connection]:
    assert size >= minimum_write
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
