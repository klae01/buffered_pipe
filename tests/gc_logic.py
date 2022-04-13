import unittest
import random 
import multiprocessing
import threading
import os
import gc

from buffered_pipe import Generic_Pipe, Static_Pipe

def recv(barrier1, barrier2, pipe, data, result, use_fork = False):
    try:
        barrier1.wait()
        if use_fork: pipe = pipe.fork()
        barrier2.wait()
        if pipe.recv() == data:
            result.put("SUCCESS")
        else:
            result.put("DIFFERENT")
    except:
        result.put("EXCEPTION")

def send(barrier1, barrier2, pipe, data, result, use_fork = False):
    try:
        barrier1.wait()
        if use_fork: pipe = pipe.fork()
        barrier2.wait()
        pipe.send(data)
        result.put("SUCCESS")
    except:
        result.put("EXCEPTION")

CTX = ["fork", "spawn", "forkserver"]
CTX = [multiprocessing.get_context(I) for I in CTX]
PIPE = [lambda : Generic_Pipe(64, 1024), lambda : Static_Pipe(64, 4)]
DATA = [lambda : os.urandom(random.randrange(512)), lambda : os.urandom(64)]

class RISK(unittest.TestCase):
    def test_0(self):
        # risk case
        # Main -> recv
        # main send / main delete / recv
        
        # for Pipe_gen, Data_gen in zip(PIPE, DATA):
        #     for ctx in CTX:
        #         barrier1 = ctx.Barrier(2)
        #         barrier2 = ctx.Barrier(2)
        #         pipe_r, pipr_w = Pipe_gen()
        #         data = Data_gen()
        #         ctx.Process(target=recv, args = (barrier1, barrier2, pipe_r, data, result))
        ...
        
    def test_4(self):
        # recv 1 : wait
        # Main -> recv 1
        # recv 1 : finish
        # recv 2 : wait
        # Main -> recv 2
        # recv 2 : finish
        # Main : GC
        ...
    def test_5(self):
        # recv 1 : wait
        # Main -> recv 1
        # recv 1 : finish
        # send 1 / recv 2 : wait
        # Main : GC
        # send 1 -> recv 2
        ...