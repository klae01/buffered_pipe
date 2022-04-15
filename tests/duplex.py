import gc
import multiprocessing
import os
import random
import sys
import threading
import unittest

from buffered_pipe import Generic_Pipe, Static_Pipe

CTX = ["fork", "spawn", "forkserver"]
CTX = [multiprocessing.get_context(I) for I in CTX]
TYPE = ["Generic_Pipe", "Static_Pipe"]
PIPE = [lambda: Generic_Pipe(1024, 64, duplex=True), lambda: Static_Pipe(64, 4, duplex=True)]
DATA = [lambda: os.urandom(random.randrange(512)), lambda: os.urandom(64)]


class test_pipe_alive:
    @staticmethod
    def send_fn(barrier, pipe, data, result):
        barrier[0].wait()
        try:
            pipe.register()
        except Exception as e:
            result.put(e)
            barrier[1].wait()
            barrier[2].wait()
            return
        try:
            barrier[1].wait()
            barrier[2].wait()
            list(map(pipe.send, data))
        except Exception as e:
            result.put(e)
            return
        result.put(None)

    @staticmethod
    def recv_fn(barrier, pipe, data, result):
        barrier[0].wait()
        try:
            pipe.register()
        except Exception as e:
            result.put(e)
            barrier[1].wait()
            barrier[2].wait()
            return
        try:
            barrier[1].wait()
            barrier[2].wait()
            for I in data:
                assert pipe.recv() == I
        except Exception as e:
            result.put(e)
            return
        result.put(None)

    def __init__(self, pipe_1, pipe_2, data1, data2, ctx, lazy_start):
        self.barrier = [ctx.Barrier(5) for _ in range(3)]
        self.Q_send = ctx.Queue()
        self.Q_recv = ctx.Queue()
        self.Ps = [
            ctx.Process(target=test_pipe_alive.send_fn, args=(self.barrier, pipe_1, data1, self.Q_send)),
            ctx.Process(target=test_pipe_alive.recv_fn, args=(self.barrier, pipe_2, data1, self.Q_recv)),
            ctx.Process(target=test_pipe_alive.send_fn, args=(self.barrier, pipe_2, data2, self.Q_send)),
            ctx.Process(target=test_pipe_alive.recv_fn, args=(self.barrier, pipe_1, data2, self.Q_recv)),
        ]
        self.started = False
        self.barrier[0].resolved = False
        if not lazy_start:
            self.started = True
            for P in self.Ps:
                P.start()

    def resolve_register(self):
        if not self.started:
            self.started = True
            for P in self.Ps:
                P.start()
        self.barrier[0].wait()
        self.barrier[0].resolved = True
        self.barrier[1].wait()

    def execute(self):
        if not self.started:
            self.started = True
            for P in self.Ps:
                P.start()

        if not self.barrier[0].resolved:
            self.barrier[0].wait()
            self.barrier[1].wait()

        self.barrier[2].wait()
        result = {"send": self.Q_send.get(), "recv": self.Q_recv.get()}
        for P in self.Ps:
            P.join()
        return result


class Type_0(unittest.TestCase):
    # risk case
    # Main -> recv
    # main send / main delete / recv

    def test_0(self):
        # P1 P2 register
        # P1 P2 communicate
        gc.collect()
        for Pipe_gen, Data_gen, test_type in zip(PIPE, DATA, TYPE):
            for ctx in CTX:
                for _ in range(10):
                    pipe_r, pipe_w = Pipe_gen()
                    datas = [Data_gen() for _ in range(1000)], [Data_gen() for _ in range(1000)]
                    tester = test_pipe_alive(pipe_r, pipe_w, *datas, ctx, lazy_start=False)
                    tester.resolve_register()
                    self.assertDictEqual(tester.execute(), {"send": None, "recv": None})
        gc.collect()

    def test_1(self):
        # P1 P2 register
        # main delete & gc
        # P1 P2 communicate
        gc.collect()
        for delete_r in [True, False]:
            for delete_w in [True, False]:
                for Pipe_gen, Data_gen, test_type in zip(PIPE, DATA, TYPE):
                    for ctx in CTX:
                        for _ in range(10):
                            pipe_r, pipe_w = Pipe_gen()
                            datas = [Data_gen() for _ in range(1000)], [Data_gen() for _ in range(1000)]
                            tester = test_pipe_alive(pipe_r, pipe_w, *datas, ctx, lazy_start=False)
                            tester.resolve_register()
                            if delete_r:
                                del pipe_r
                            if delete_w:
                                del pipe_w
                            gc.collect()
                            result = tester.execute()
                            str_result = {K: str(V) for K, V in result.items()}
                            self.assertDictEqual(
                                result,
                                {"send": None, "recv": None},
                                msg=f"fail on ctx = {type(ctx).__name__} / {test_type}{['', ' / delete_r'][delete_r]}{['', ' / delete_w'][delete_w]}\n detail = {str_result}",
                            )
        gc.collect()

    def test_2(self):
        # P1 P2 register
        # main delete & gc
        # P1 P2 communicate
        gc.collect()
        for delete_r in [True, False]:
            for delete_w in [True, False]:
                for Pipe_gen, Data_gen, test_type in zip(PIPE, DATA, TYPE):
                    for ctx in CTX:
                        for _ in range(10):
                            pipe_r, pipe_w = Pipe_gen()
                            datas = [Data_gen() for _ in range(1000)], [Data_gen() for _ in range(1000)]
                            tester = test_pipe_alive(pipe_r, pipe_w, *datas, ctx, lazy_start=False)
                            if delete_r:
                                del pipe_r
                            if delete_w:
                                del pipe_w
                            gc.collect()
                            tester.resolve_register()
                            result = tester.execute()
                            str_result = {K: str(V) for K, V in result.items()}
                            if delete_r and delete_w and ctx != multiprocessing.get_context("fork"):
                                self.assertNotEqual(
                                    result,
                                    {"send": None, "recv": None},
                                    msg=f"fail on ctx = {type(ctx).__name__} / {test_type}{['', ' / delete_r'][delete_r]}{['', ' / delete_w'][delete_w]}\n detail = {str_result}",
                                )
                            else:
                                self.assertDictEqual(
                                    result,
                                    {"send": None, "recv": None},
                                    msg=f"fail on ctx = {type(ctx).__name__} / {test_type}{['', ' / delete_r'][delete_r]}{['', ' / delete_w'][delete_w]}\n detail = {str_result}",
                                )
        gc.collect()


if __name__ == "__main__":
    path_info = __file__.split("/")
    path_info = "/".join(path_info[path_info.index("tests") :])
    print(path_info)
    unittest.main(argv=[""])
