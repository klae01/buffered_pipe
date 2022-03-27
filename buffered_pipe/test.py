from . import Pipe
import multiprocessing
import time
import threading
import copy
import tqdm
def f(*args):
    stream, queue, cnt = args
    queue.put(time.time())
    for I in range(cnt):
        # print("f:", I)
        stream.send(I)
    queue.put(time.time())
    # print("finish processing")
def g(*args):
    queue1, queue, cnt = args
    queue.put(time.time())
    list(map(queue1.put, range(cnt)))
    queue.put(time.time())

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    # multiprocessing.set_start_method('fork')
    # multiprocessing.set_start_method('forkserver')
    virtural_method = threading.Thread
    virtural_method = multiprocessing.Process

    cnt = 100000
    pipe_r, pipe_w = Pipe(False, minimum_write=32, size = 2**20)
    queue = multiprocessing.Queue()
    P = virtural_method(target=f, args = (pipe_w, queue, cnt))

    offset = time.time()
    P.start()
    print(queue.get() - offset, time.time() - offset)
    # for exp in range(cnt):
    #     ret = cons.recv()
    #     if exp != ret:
    #         print(f"{exp} != {ret}")
    stream = (pipe_r.recv() for _ in range(cnt))
    gen = ((exp, ret) for exp, ret in zip(stream, tqdm.trange(cnt, mininterval=0.02)))
    gen = ((exp, ret) for exp, ret in gen if exp != ret)
    for exp, ret in gen:
        print(f"{exp} != {ret}")
    print(queue.get() - offset, time.time() - offset)
    P.join()
    print(threading.active_count(), time.time() - offset)
    # stream.destruction()
    print(threading.active_count(), time.time() - offset)



    print("multiprocessing queue")
    queue1 = multiprocessing.Queue()
    queue = multiprocessing.Queue()
    P = virtural_method(target=g, args = (queue1, queue, cnt))

    offset = time.time()
    P.start()
    print(queue.get() - offset, time.time() - offset)
    gen = ((exp, queue1.get()) for exp in tqdm.trange(cnt, mininterval=0.02))
    gen = ((exp, ret) for exp, ret in gen if exp != ret)
    for exp, ret in gen:
        print(f"{exp} != {ret}")
    print(queue.get() - offset, time.time() - offset)
    P.join()
    print(threading.active_count(), time.time() - offset)


    print("multiprocessing pipe")
    pipe_r, pipe_w = multiprocessing.Pipe(False)
    queue = multiprocessing.Queue()
    P = virtural_method(target=f, args = (pipe_w, queue, cnt))

    offset = time.time()
    P.start()
    print(queue.get() - offset, time.time() - offset)
    stream = (pipe_r.recv() for _ in range(cnt))
    gen = ((exp, ret) for exp, ret in zip(stream, tqdm.trange(cnt, mininterval=0.02)))
    gen = ((exp, ret) for exp, ret in gen if exp != ret)
    for exp, ret in gen:
        print(f"{exp} != {ret}")
    print(queue.get() - offset, time.time() - offset)
    P.join()
    print(threading.active_count(), time.time() - offset)