from . import Pipe
import multiprocessing
import time
import threading
import copy
import tqdm
import random
import traceback

import numpy as np


def small_int_transfer(*args):
    stream, queue, cnt, seed = args
    random.seed(seed)
    queue.put(time.time())
    for I in range(cnt):
        stream.send(random.randrange(10000000))
    queue.put(time.time())
def small_int_answer(*args):
    cnt, seed = args
    random.seed(seed)
    return (random.randrange(10000000) for I in range(cnt))

def random_string(length = 5, use_upper = False, use_lower = True, use_number = True):
    set = []
    if use_upper: set += list(range(ord('A'),ord('Z') + 1))
    if use_lower: set += list(range(ord('a'),ord('z') + 1))
    if use_number: set += list(range(ord('0'),ord('9') + 1))
    return ''.join(map(chr, random.choices(set, k = length)))

def static_length_transfer(*args):
    stream, queue, length, cnt, seed = args
    random.seed(seed)
    queue.put(time.time())
    for I in range(cnt):
        target = random_string(length, True, True, True)
        stream.send(target)
    queue.put(time.time())
def static_length_answer(*args):
    length, cnt, seed = args
    random.seed(seed)
    return (random_string(length, True, True, True) for I in range(cnt))

def large_data_transfer(*args):
    stream, queue, cnt, seed = args
    random.seed(seed)
    queue.put(time.time())
    for I in range(cnt):
        target = random_string(random.randrange(10, 50000), True, True, True)
        stream.send(target)
    queue.put(time.time())
def large_data_answer(*args):
    cnt, seed = args
    random.seed(seed)
    return (random_string(random.randrange(10, 50000), True, True, True) for I in range(cnt))

def large_array_generate(max_element):
    X = [random.randrange(2, 10) for I in range(10)]
    S = [I for I in np.cumprod(X) if I <= max_element]
    length = random.choice(range(len(S)))
    return np.random.uniform(0, 1, X[:length])
def large_array_transfer(*args):
    stream, queue, max_element, cnt, seed = args
    random.seed(seed)
    np.random.seed(seed)
    queue.put(time.time())
    for I in range(cnt):
        stream.send(large_array_generate(max_element))
    queue.put(time.time())
def large_array_answer(*args):
    max_element, cnt, seed = args
    random.seed(seed)
    np.random.seed(seed)
    return (large_array_generate(max_element) for I in range(cnt))


def equality_test(exp, ret):
    if isinstance(exp, np.ndarray):
        return (exp == ret).all()
    else: return exp == ret
def test(transfer, answer, *args, size = 2 ** 20):
    test.test_id += 1
    pipe_r, pipe_w = Pipe(False, minimum_write=32, size = size)
    queue = multiprocessing.Queue()

    P = virtural_method(target=transfer, args = (pipe_w, queue, *args))

    offset = time.time()
    P.start()
    print(queue.get() - offset, time.time() - offset)
    gen = ((pipe_r.recv(), ret) for ret in answer(*args))
    gen = ((exp, ret) for exp, ret in gen if not equality_test(exp, ret))
    fail = False
    try:
        for exp, ret in gen:
            fail = True
            print(f"{exp} != {ret}")
    except Exception as e:
        fail = True
        print("TEST EXCEPTION:", e)
        print(traceback.format_exc())

    print(queue.get() - offset, time.time() - offset)
    P.join()
    print(threading.active_count(), time.time() - offset)
    if fail:
        raise Exception(f"FAIL on test = {test.test_id} with {transfer.__name__}, {args}, {size}")
test.test_id = 0
if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    # multiprocessing.set_start_method('fork')
    # multiprocessing.set_start_method('forkserver')
    virtural_method = threading.Thread
    virtural_method = multiprocessing.Process

    print("\n\ntest GROUP 1")
    test(small_int_transfer, small_int_answer, 100000, 166)
    test(small_int_transfer, small_int_answer, 100000, 15777)
    test(small_int_transfer, small_int_answer, 100000, 8433)

    print("\n\ntest GROUP 2")
    test(small_int_transfer, small_int_answer, 100000, 166, size = 2 ** 7)
    test(small_int_transfer, small_int_answer, 100000, 15777, size = 2 ** 7)
    test(small_int_transfer, small_int_answer, 100000, 8433, size = 2 ** 7)

    print("\n\ntest GROUP 3")
    test(static_length_transfer, static_length_answer, 1024, 10000, 166)
    test(static_length_transfer, static_length_answer, 1024, 10000, 15777)
    test(static_length_transfer, static_length_answer, 1024, 10000, 8433)

    print("\n\ntest GROUP 4")
    test(static_length_transfer, static_length_answer, 1024, 10, 166, size = 2 ** 7)
    test(static_length_transfer, static_length_answer, 1024, 10, 15777, size = 2 ** 7)
    test(static_length_transfer, static_length_answer, 1024, 10, 8433, size = 2 ** 7)

    print("\n\ntest GROUP 5")
    test(static_length_transfer, static_length_answer, 1024, 10000, 166, size = 2 ** 7)
    test(static_length_transfer, static_length_answer, 1024, 10000, 15777, size = 2 ** 7)
    test(static_length_transfer, static_length_answer, 1024, 10000, 8433, size = 2 ** 7)

    print("\n\ntest GROUP 6")
    test(large_data_transfer, large_data_answer, 10000, 166)
    test(large_data_transfer, large_data_answer, 10000, 15777)
    test(large_data_transfer, large_data_answer, 10000, 8433)

    print("\n\ntest GROUP 7")
    test(large_data_transfer, large_data_answer, 10000, 166, size = 2 ** 7)
    test(large_data_transfer, large_data_answer, 10000, 15777, size = 2 ** 7)
    test(large_data_transfer, large_data_answer, 10000, 8433, size = 2 ** 7)

    print("\n\ntest GROUP 8")
    test(large_array_transfer, large_array_answer, 2**10, 10000, 166)
    test(large_array_transfer, large_array_answer, 2**15, 10000, 15777)
    test(large_array_transfer, large_array_answer, 2**20, 10000, 8433)

    print("\n\ntest GROUP 9")
    test(large_array_transfer, large_array_answer, 2**5, 10000, 166, size = 2 ** 7)
    test(large_array_transfer, large_array_answer, 2**10, 10000, 15777, size = 2 ** 7)
    test(large_array_transfer, large_array_answer, 2**15, 10000, 8433, size = 2 ** 7)

    print("\n\ntest GROUP 10")
    test(large_array_transfer, large_array_answer, 2**20, 100, 166, size = 2 ** 16)
    test(large_array_transfer, large_array_answer, 2**25, 100, 15777, size = 2 ** 16)
    test(large_array_transfer, large_array_answer, 2**30, 100, 8433, size = 2 ** 16)

    print("\n\ntest GROUP 11")
    test(large_array_transfer, large_array_answer, 2**20, 100, 166, size = 2 ** 7)
    test(large_array_transfer, large_array_answer, 2**25, 100, 15777, size = 2 ** 7)
    test(large_array_transfer, large_array_answer, 2**30, 100, 8433, size = 2 ** 7)