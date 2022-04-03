import collections
import copy
import time
import unittest
import random 
import multiprocessing
import threading
import itertools
import hashlib
import os

from buffered_pipe import Static_Pipe as Pipe

# random_bytes = lambda n: bytes(random.randint(0, 255) for _ in range(n))
random_bytes = lambda n: os.urandom(n)

def dataset(length, count):
    return [random_bytes(length) for _ in range(count)]

def producer(pipe, data):
    for I in data:
        pipe.send(I)

def mpmt_producer(pipe, data:list, barrier:multiprocessing.Barrier):
    # print(f"prod reach barrier", "%x"%threading.current_thread().ident)
    barrier.wait()
    # print(f"prod pass barrier", "%x"%threading.current_thread().ident)
    for I in data:
        # time.sleep(0.0004)
        pipe.send(I)
    # print(f"prod finish")
    # print(f"prod finish with send {len(data)} elements")

def mpmt_consumer(pipe, RTQ:multiprocessing.Queue, finished:bytes, barrier:multiprocessing.Barrier):
    # print(f"cons reach barrier", "%x"%threading.current_thread().ident)
    barrier.wait()
    # print(f"cons pass barrier", "%x"%threading.current_thread().ident)
    items = []
    while True:
        data = pipe.recv()
        if data == finished:
            break
        items.append(data)
    # print(f"cons finish")
    # print(f"cons finish with recv {len(items)} elements")
    RTQ.put(items)
    # print(f"cons Qin finish")

def type_tester(pipe, data):
    # single prod, single cons
    for idx, exp in enumerate(data):
        ret = pipe.recv()
        if exp != ret:
            raise Exception(f"{exp} !=  {ret} at {idx}")
    return True

def joinable_test(Idatas, Odatas):
    # 1. count
    # 2. each Idata possible in Odatas
    def _sub(I_set, Odatas):
        def pruning(selected, options):
            update = False
            S_pos = [-1] * len(Odatas)
            for i in range(len(selected)):
                if selected[i] is None:
                    options[i] = [(r, c) for r, c in options[i] if S_pos[r] < c]
                    if len(options[i]) == 1:
                        selected[i] = options[i][0]
                        update = True
                if selected[i] is not None:
                    S_pos[selected[i][0]] = selected[i][1]

            E_pos = [len(I) for I in Odatas]
            for i in range(len(selected)-1, -1, -1):
                if selected[i] is None:
                    options[i] = [(r, c) for r, c in options[i] if E_pos[r] > c]
                    if len(options[i]) == 1:
                        selected[i] = options[i][0]
                        update = True
                if selected[i] is not None:
                    E_pos[selected[i][0]] = selected[i][1]
            return update

        def rec_solver(selected, options):
            update = True
            while update:
                update = pruning(selected, options)
            if all(map(lambda x: x is not None, selected)):
                return True
            for i in range(len(selected)):
                if selected[i] is None:
                    for o in options[i]:
                        # print("try random")
                        s_cp, o_cp = copy.copy(selected), copy.copy(options)
                        s_cp[i] = o
                        if rec_solver(s_cp, o_cp):
                            return True
            return False

        E_pos = collections.defaultdict(list)
        E_rpos = collections.defaultdict(list)
        for i, I in enumerate(I_set):
            E_pos[I].append(i)
        for i, OD in enumerate(Odatas):
            for j, O in enumerate(OD):
                if O in E_pos:
                    E_rpos[O].append((i, j))
        return rec_solver([None] * len(I_set), [E_rpos[I] for I in I_set])
        
        
    Idatas = [[hashlib.sha256(J).digest() for J in I] for I in Idatas]
    Odatas = [[hashlib.sha256(J).digest() for J in I] for I in Odatas]
    Iset = set(itertools.chain.from_iterable(Idatas))
    Oset = set(itertools.chain.from_iterable(Odatas))
    if Oset - (Iset & Oset):
        # recv unsended data
        return -1
    if len(Oset) != len(Iset):
        # unique count do not match
        return -2

    if collections.Counter(itertools.chain.from_iterable(Idatas)) != collections.Counter(itertools.chain.from_iterable(Odatas)):
        return -3
    return all(_sub(I, Odatas) for I in Idatas)    


def type20_tester(pipe_r, pipe_w, *data, mp_prod = 0, mt_prod = 0, mp_cons = 0, mt_cons = 0, end_Data = None, ctx = None):
# def type20_tester(pipe_r, pipe_w, *data, mp_prod = 0, mt_prod = 0, ctx = None):
    # multi prod, main thread cons
    # print(f"mp_prod = {mp_prod} , mt_prod = {mt_prod}")#, end_Data = {end_Data} ")
    assert len(data) == mp_prod + mt_prod

    ctx = ctx or multiprocessing.get_context()
    prod_barrier = ctx.Barrier(mp_prod + mt_prod)

    mp_prods = [ctx.Process(target = mpmt_producer, args = (pipe_w, data[idx], prod_barrier)) for idx in range(mp_prod)]
    mt_prods = [threading.Thread(target = mpmt_producer, args = (pipe_w, data[mp_prod + idx], prod_barrier)) for idx in range(mt_prod)]
    

    # print(f"MPST prod ({len(mp_prods + mt_prods)}) start with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")
    for MPMT in itertools.chain(mp_prods, mt_prods):
        MPMT.start()
    # time.sleep(0.02)
    # print("MPSC cons collect")
    result = []
    for _ in range(sum(map(len, data))):
        result.append(pipe_r.recv())

    # print("MPSC prod join")
    for MPMT in itertools.chain(mp_prods, mt_prods):
        MPMT.join()
        
    # print("joinable result")  
    R = joinable_test(data, [result])

    # print(f"MPST prod ({len(mp_prods + mt_prods)}) end with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")

    return R

def type02_tester(pipe_r, pipe_w, *data, mp_prod = 0, mt_prod = 0, mp_cons = 0, mt_cons = 0, end_Data = None, ctx = None):
# def type02_tester(pipe_r, pipe_w, *data, mp_cons = 0, mt_cons = 0, end_Data = None, ctx = None):
    # main thread prod, multi cons
    # print(f"mp_cons = {mp_cons}, mt_cons = {mt_cons}")#, end_Data = {end_Data} ")
    assert len(data) == 1
    ctx = ctx or multiprocessing.get_context()
    RTQ = ctx.SimpleQueue()
    cons_barrier = ctx.Barrier(mp_cons + mt_cons)
    
    mp_conss = [ctx.Process(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mp_cons)]
    mt_conss = [threading.Thread(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mt_cons)]

    # print(f"SPMT cons ({len(mp_conss + mt_conss)}) start with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")
    for MPMT in itertools.chain(mp_conss, mt_conss):
        MPMT.start()

    # print("SPMC send data")
    for I in data[0]:
        pipe_w.send(I)

    # print("SPMC cons send end_Data")        
    for MPMT in itertools.chain(mp_conss, mt_conss):
        pipe_w.send(end_Data)

    # print("SPMC cons collect")   
    results = []
    for MPMT in itertools.chain(mp_conss, mt_conss):
        results.append(RTQ.get())
    
    # print("SPMC cons join")
    for MPMT in itertools.chain(mp_conss, mt_conss):
        MPMT.join()
    
    # print("joinable result")  
    R = joinable_test(data, results)

    # print(f"MPST prod ({len(mp_conss + mt_conss)}) end with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")

    return R
    
def type22_tester(pipe_r, pipe_w, *data, mp_prod = 0, mt_prod = 0, mp_cons = 0, mt_cons = 0, end_Data = None, ctx = None):
    # multi prod, multi cons
    # print(f"mp_prod = {mp_prod} , mt_prod = {mt_prod}, mp_cons = {mp_cons}, mt_cons = {mt_cons}")#, end_Data = {end_Data} ")
    assert len(data) == mp_prod + mt_prod

    ctx = ctx or multiprocessing.get_context()
    RTQ = ctx.SimpleQueue()
    prod_barrier = ctx.Barrier(mp_prod + mt_prod)
    cons_barrier = ctx.Barrier(mp_cons + mt_cons)
    # prod_barrier = cons_barrier = ctx.Barrier(mp_prod + mt_prod + mp_cons + mt_cons)

    mp_prods = [ctx.Process(target = mpmt_producer, args = (pipe_w, data[idx], prod_barrier)) for idx in range(mp_prod)]
    mt_prods = [threading.Thread(target = mpmt_producer, args = (pipe_w, data[mp_prod + idx], prod_barrier)) for idx in range(mt_prod)]
    
    mp_conss = [ctx.Process(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mp_cons)]
    mt_conss = [threading.Thread(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mt_cons)]


    # print(f"MPMC prod ({len(mp_prods + mt_prods)}) / cons ({len(mp_conss + mt_conss)}) start with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")
    for MPMT in itertools.chain(mp_prods, mt_prods, mp_conss, mt_conss):
        MPMT.start()
    # time.sleep(0.02)
    # print(f"MPMC prod join")
    for MPMT in itertools.chain(mp_prods, mt_prods):
        MPMT.join()
    # print(f"MPMC cons finish final data send")        
    for MPMT in itertools.chain(mp_conss, mt_conss):
        pipe_w.send(end_Data)
    # print(f"MPMC cons collect")   
    results = []
    for MPMT in itertools.chain(mp_conss, mt_conss):
        results.append(RTQ.get())
    # print(f"MPMC cons join")   
    for MPMT in itertools.chain(mp_conss, mt_conss):
        MPMT.join()
    # print(f"MPMC joinable result")  
    # print('\n----------------\n', data, '\n', results, '\n=============\n')
    # print(f"\n----------------\n {[len(I) for I in data]} \n {[len(I) for I in results]} \n=============\n")
    R = joinable_test(data, results)
    # if R is not True:
    #     print(f"\n----------------\n {data} \n {results} \n=============\n")

    # print(f"MPMC prod ({len(mp_prods + mt_prods)}) / cons ({len(mp_conss + mt_conss)}) end with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")

    return R

class TestCase_SPSC:
    spend_time = collections.defaultdict(float)
    def __init__(self, length, count, buf_size, seed):
        self.length = length
        self.count = count
        self.buf_size = buf_size
        self.seed = seed
    def mp_test(self, data, ctx = multiprocessing.get_context()):
        TestCase_SPSC.spend_time[type(ctx).__name__] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        P = ctx.Process(target = producer, args = (pipe_w, data))
        P.start()
        result = type_tester(pipe_r, data)
        TestCase_SPSC.spend_time[type(ctx).__name__] += time.time()
        return result
    def mt_test(self, data):
        TestCase_SPSC.spend_time['threading'] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        P = threading.Thread(target = producer, args = (pipe_w, data))
        P.start()
        TestCase_SPSC.spend_time['threading'] += time.time()
        return type_tester(pipe_r, data)
    @classmethod
    def test_all(cls, length, count, buf_size, seed, utc):
        TC = cls(length, count, buf_size, seed)

        TestCase_SPSC.spend_time['data gen'] -= time.time()
        random.seed(seed)
        data = dataset(length, count)
        TestCase_SPSC.spend_time['data gen'] += time.time()

        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("fork")), True)
        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("spawn")), True)
        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("forkserver")), True)
        utc.assertEqual(TC.mt_test(data), True)

class Type_0(unittest.TestCase):
    def test_small1(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(4, 2**15, 1, seed, self)
    def test_small2(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(4, 2**15, 2, seed, self)
    def test_small3(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(4, 2**16, 4, seed, self)
    def test_small4(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(2**7, 2**14, 4, seed, self)
    def test_small5(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(2**7, 2**14, 1024, seed, self)
    def test_small6(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(2**10, 2**12, 4, seed, self)
    def test_small7(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase_SPSC.test_all(2**16, 2**8, 4, seed, self)

class TestCase_MPMC_base:
    # mtmc_seed = 0
    # spend_time = collections.defaultdict(float)
    # target_fn = type22_tester
    def __init__(self, length, buf_size, seed):
        self.length = length
        self.buf_size = buf_size
        self.seed = seed
    def run_test(self, data, end_Data, cons_cnt, ctx = multiprocessing.get_context(), spend_time = None):
        spend_time[type(ctx).__name__] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        random.seed(type(self).mtmc_seed)
        type(self).mtmc_seed += 1
        mp_prod = random.randrange(0, len(data))
        mt_prod = len(data) - mp_prod

        mp_cons = random.randrange(0, cons_cnt)
        mt_cons = cons_cnt - mp_cons

        result = type(self).target_fn(pipe_r, pipe_w, *data, mp_prod = mp_prod, mt_prod = mt_prod, mp_cons = mp_cons, mt_cons = mt_cons, end_Data = end_Data, ctx = ctx)
        spend_time[type(ctx).__name__] += time.time()
        return result
    @classmethod
    def test_all(cls, length, data_size_range, prod_cnt, cons_cnt, buf_size, seed, utc):
        TC = cls(length, buf_size, seed)

        utc.spend_time['data gen'] -= time.time()
        random.seed(seed)
        data = [dataset(length, random.randrange(*data_size_range)) for _ in range(prod_cnt)]
        utc.spend_time['data gen'] += time.time()

        utc.spend_time['search unused'] -= time.time()
        data_hashes = set(hashlib.sha256(I).digest() for I in itertools.chain.from_iterable(data))
        end_Data = random_bytes(length)
        while hashlib.sha256(end_Data).digest() in data_hashes:
            end_Data = random_bytes(length)
        utc.spend_time['search unused'] += time.time()

        utc.assertEqual(TC.run_test(data, end_Data = end_Data, cons_cnt = cons_cnt, ctx = multiprocessing.get_context("fork"), spend_time = utc.spend_time), True)
        utc.assertEqual(TC.run_test(data, end_Data = end_Data, cons_cnt = cons_cnt, ctx = multiprocessing.get_context("spawn"), spend_time = utc.spend_time), True)
        utc.assertEqual(TC.run_test(data, end_Data = end_Data, cons_cnt = cons_cnt, ctx = multiprocessing.get_context("forkserver"), spend_time = utc.spend_time), True)

def random_ordered_cycle(S, E):
    local_random = random.Random(123)
    while True:
        X = list(range(S, E))
        local_random.shuffle(X)
        yield from X

class Test_suite_base:
    # target_class = TestCase_MPMC
    # prod_cnt_ord = random_ordered_cycle(1, 4)
    # cons_cnt_ord = random_ordered_cycle(1, 4)
    def test_small1(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (1, 10), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 1, seed, self)
    def test_small2(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (1, 100), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)
    def test_small3(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (10, 100), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)
    def test_small4(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(128, (1, 50), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)
    def test_small5(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(128, (0, 10), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 1024, seed, self)
    def test_small6(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(1024, (0, 50), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)

class Test_suite_large:
    # target_class = TestCase_MPMC
    # prod_cnt_ord = random_ordered_cycle(1, 4)
    # cons_cnt_ord = random_ordered_cycle(1, 4)
    def test_small1(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (1, 1000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 1, seed, self)
    def test_small2(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (1, 10000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)
    def test_small3(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(4, (100, 10000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 4, seed, self)
    def test_small4(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(128, (1, 50000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 32, seed, self)
    def test_small5(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(128, (0, 100000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 1024, seed, self)
    def test_small6(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            type(self).target_class.test_all(1024, (0, 50000), next(type(self).prod_cnt_ord), next(type(self).cons_cnt_ord), 32, seed, self)

class TestCase_MPSC(TestCase_MPMC_base):
    mtmc_seed = 0
    target_fn = type20_tester
class Type_1(unittest.TestCase, Test_suite_base):
    target_class = TestCase_MPSC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = random_ordered_cycle(1, 5)
    cons_cnt_ord = itertools.cycle([1])
class Type_1L(unittest.TestCase, Test_suite_large):
    target_class = TestCase_MPSC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = random_ordered_cycle(1, 20)
    cons_cnt_ord = itertools.cycle([1])

class TestCase_SPMC(TestCase_MPMC_base):
    mtmc_seed = 0
    target_fn = type02_tester
class Type_2(unittest.TestCase, Test_suite_base):
    target_class = TestCase_SPMC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = itertools.cycle([1])
    cons_cnt_ord = random_ordered_cycle(1, 5)
class Type_2L(unittest.TestCase, Test_suite_large):
    target_class = TestCase_SPMC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = itertools.cycle([1])
    cons_cnt_ord = random_ordered_cycle(1, 20)
    
class TestCase_MPMC(TestCase_MPMC_base):
    mtmc_seed = 0
    target_fn = type22_tester
class Type_3(unittest.TestCase, Test_suite_base):
    target_class = TestCase_MPMC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = random_ordered_cycle(1, 5)
    cons_cnt_ord = random_ordered_cycle(1, 5)
class Type_3L(unittest.TestCase, Test_suite_large):
    target_class = TestCase_MPMC
    spend_time = collections.defaultdict(float)
    prod_cnt_ord = random_ordered_cycle(1, 20)
    cons_cnt_ord = random_ordered_cycle(1, 20)

if __name__ == '__main__':
    try:
        unittest.main(exit=False)
    finally:
        for K, V in TestCase_SPSC.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("====================")
        for K, V in Type_1.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("--------------------")
        for K, V in Type_1L.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("====================")
        for K, V in Type_2.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("--------------------")
        for K, V in Type_2L.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("====================")
        for K, V in Type_3.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("--------------------")
        for K, V in Type_3L.spend_time.items():
            print(f"{K} : {V: 4.2f}")