import collections
import copy
import time
import unittest
import random 
import multiprocessing
import threading
import itertools
import hashlib

from buffered_pipe import Static_Pipe as Pipe

def dataset(length, count):
    return [random.randbytes(length) for _ in range(count)]

def producer(pipe, data):
    for I in data:
        pipe.send(I)

def mpmt_producer(pipe, data:list, barrier:multiprocessing.Barrier):
    print(f"prod reach barrier")
    barrier.wait()
    print(f"prod pass barrier")
    for I in data:
        pipe.send(I)
    # print(f"prod finish with send {len(data)} elements")

def mpmt_consumer(pipe, RTQ:multiprocessing.Queue, finished:bytes, barrier:multiprocessing.Barrier):
    print(f"cons reach barrier")
    barrier.wait()
    print(f"cons pass barrier")
    items = []
    while True:
        data = pipe.recv()
        if data == finished:
            break
        items.append(data)
    # print(f"cons finish with recv {len(items)} elements")
    RTQ.put(items)

def type_tester(pipe, data):
    # single prod, single cons
    for idx, exp in enumerate(data):
        ret = pipe.recv()
        if exp != ret:
            raise Exception(f"{exp} !=  {ret} at {idx}")
    return True

def joinable_test(result, *data):
    lookup = set([0] * len(data))
    for step, ret in enumerate(result):
        lookup, prev_lookup = set(), lookup
        for I in prev_lookup:
            for i, I in enumerate(I):
                if len(data[i]) < I and data[i][I] == ret:
                    lup = copy.copy(I)
                    lup[i] = I + 1
                    lookup.add(lup)
        if len(lookup) == 0:
            raise Exception(f"len(lookup) = {len(lookup)} at {step}")
    assert step + 1 == sum(map(len, data))
    return len(lookup) > 0

def joinable_test(Idatas, Odatas):
    lookup = set([([0] * len(Idatas), [0] * len(Odatas))])
    assert sum(map(len, Idatas)) == sum(map(len, Odatas))
    for step in range(sum(map(len, Idatas))):
        lookup, prev_lookup = set(), lookup
        for I_up, O_up in prev_lookup:
            for i, I in enumerate(I_up):
                if len(Idatas[i]) < I:
                    I_nxt = copy.copy(I_up)
                    I_nxt[i] += 1
                    for j, J in enumerate(O_up):
                        if len(Odatas[j]) < J and Idatas[i][I] == Odatas[j][J]:
                            O_nxt = copy.copy(O_up)
                            O_nxt[i] += 1
                            lookup.add((I_nxt, O_nxt))
        if len(lookup) == 0:
            raise Exception(f"len(lookup) = {len(lookup)} at {step}")
    return len(lookup) > 0

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
    if collections.Counter(itertools.chain.from_iterable(Idatas)) != collections.Counter(itertools.chain.from_iterable(Odatas)):
        return -123
    return all(_sub(I, Odatas) for I in Idatas)    


def type20_tester(pipe, *data, num_mp = 0):
    # multi prod, main cons
    ...
    return joinable_test((pipe.recv() for _ in range(sum(map(len, data)))), *data)

def type02_tester(pipe, data, num_mp = 0):
    # main prod, multi cons
    ...
    return joinable_test((pipe.recv() for _ in range(sum(map(len, data)))), *data)

def type22_tester(pipe_r, pipe_w, *data, mp_prod = 0, mt_prod = 0, mp_cons = 0, mt_cons = 0, end_Data = None, ctx = None):
    # multi prod, multi cons
    print(f"mp_prod = {mp_prod} , mt_prod = {mt_prod}, mp_cons = {mp_cons}, mt_cons = {mt_cons}, end_Data = {end_Data} ")
    assert len(data) == mp_prod + mt_prod

    ctx = ctx or multiprocessing.get_context()
    RTQ = ctx.Queue()
    prod_barrier = ctx.Barrier(mp_prod + mt_prod)
    cons_barrier = ctx.Barrier(mp_cons + mt_cons)

    mp_prods = [ctx.Process(target = mpmt_producer, args = (pipe_w, data[idx], prod_barrier)) for idx in range(mp_prod)]
    mt_prods = [threading.Thread(target = mpmt_producer, args = (pipe_w, data[mp_prod + idx], prod_barrier)) for idx in range(mt_prod)]
    
    mp_conss = [ctx.Process(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mp_cons)]
    mt_conss = [threading.Thread(target = mpmt_consumer, args = (pipe_r, RTQ, end_Data, cons_barrier)) for _ in range(mt_cons)]


    print(f"MPMT prod ({len(mp_prods + mt_prods)}) / cons ({len(mp_conss + mt_conss)}) start with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")
    for MPMT in itertools.chain(mp_prods, mt_prods, mp_conss, mt_conss):
        MPMT.start()
    # time.sleep(0.02)
    print("MPMT prod join")
    for MPMT in itertools.chain(mp_prods, mt_prods):
        MPMT.join()
    print("cons finish final data send")        
    for MPMT in itertools.chain(mp_conss, mt_conss):
        pipe_w.send(end_Data)
    print("cons join and collect")   
    results = []
    for MPMT in itertools.chain(mp_conss, mt_conss):
        MPMT.join()
        results.append(RTQ.get())
    print("joinable result")  
    # print('\n----------------\n', data, '\n', results, '\n=============\n')
    print(f"\n----------------\n {[len(I) for I in data]} \n {[len(I) for I in results]} \n=============\n")
    R = joinable_test(data, results)
    if R is not True:
        print(f"\n----------------\n {data} \n {results} \n=============\n")

    print(f"MPMT prod ({len(mp_prods + mt_prods)}) / cons ({len(mp_conss + mt_conss)}) end with {threading.activeCount()} mt {multiprocessing.active_children()} mp ")

    return R

class TestCase:
    spend_time = collections.defaultdict(float)
    def __init__(self, length, count, buf_size, seed):
        self.length = length
        self.count = count
        self.buf_size = buf_size
        self.seed = seed
    def mp_test(self, data, ctx = multiprocessing.get_context()):
        TestCase.spend_time[type(ctx).__name__] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        P = ctx.Process(target = producer, args = (pipe_w, data))
        P.start()
        result = type_tester(pipe_r, data)
        TestCase.spend_time[type(ctx).__name__] += time.time()
        return result
    def mt_test(self, data):
        TestCase.spend_time['threading'] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        P = threading.Thread(target = producer, args = (pipe_w, data))
        P.start()
        TestCase.spend_time['threading'] += time.time()
        return type_tester(pipe_r, data)
    @classmethod
    def test_all(cls, length, count, buf_size, seed, utc):
        TC = cls(length, count, buf_size, seed)

        TestCase.spend_time['data gen'] -= time.time()
        random.seed(seed)
        data = dataset(length, count)
        TestCase.spend_time['data gen'] += time.time()

        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("fork")), True)
        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("spawn")), True)
        utc.assertEqual(TC.mp_test(data, ctx = multiprocessing.get_context("forkserver")), True)
        utc.assertEqual(TC.mt_test(data), True)

class Type1(unittest.TestCase):
    def test_small1(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(4, 10000, 1, seed, self)
    def test_small2(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(4, 10000, 2, seed, self)
    def test_small3(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(4, 10000, 4, seed, self)
    def test_small4(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(128, 10000, 4, seed, self)
    def test_small5(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(128, 10000, 1024, seed, self)
    def test_small6(self):
        for seed in [123,1251,523,12,3535,167,945,933]:
            TestCase.test_all(1024, 10000, 4, seed, self)

class MPMC_TestCase:
    mtmc_seed = 0
    spend_time = collections.defaultdict(float)
    def __init__(self, length, buf_size, seed):
        self.length = length
        self.buf_size = buf_size
        self.seed = seed
    def run_test(self, data, end_Data, ctx = multiprocessing.get_context()):
        MPMC_TestCase.spend_time[type(ctx).__name__] -= time.time()
        pipe_r, pipe_w = Pipe(self.length, self.buf_size)
        random.seed(MPMC_TestCase.mtmc_seed)
        MPMC_TestCase.mtmc_seed += 1
        mp_prod = random.randrange(0, len(data))
        mt_prod = len(data) - mp_prod

        mp_cons = random.choice([0, 1, 2])
        mt_cons = random.choice([0, 1, 2][(mp_cons == 0):])

        result = type22_tester(pipe_r, pipe_w, *data, mp_prod = mp_prod, mt_prod = mt_prod, mp_cons = mp_cons, mt_cons = mt_cons, end_Data = end_Data, ctx = ctx)
        MPMC_TestCase.spend_time[type(ctx).__name__] += time.time()
        return result
    @classmethod
    def test_all(cls, length, data_size_range, data_count, buf_size, seed, utc):
        TC = cls(length, buf_size, seed)

        MPMC_TestCase.spend_time['data gen'] -= time.time()
        random.seed(seed)
        data = [dataset(length, random.randrange(*data_size_range)) for _ in range(data_count)]
        MPMC_TestCase.spend_time['data gen'] += time.time()

        MPMC_TestCase.spend_time['search unused'] -= time.time()
        data_hashes = set(hashlib.sha256(I).digest() for I in itertools.chain.from_iterable(data))
        end_Data = random.randbytes(length)
        while hashlib.sha256(end_Data).digest() in data_hashes:
            end_Data = random.randbytes(length)
        MPMC_TestCase.spend_time['search unused'] += time.time()

        utc.assertEqual(TC.run_test(data, end_Data = end_Data, ctx = multiprocessing.get_context("fork")), True)
        utc.assertEqual(TC.run_test(data, end_Data = end_Data, ctx = multiprocessing.get_context("spawn")), True)
        utc.assertEqual(TC.run_test(data, end_Data = end_Data, ctx = multiprocessing.get_context("forkserver")), True)

class Type_MPMC(unittest.TestCase):
    def test_small1(self):
        for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
            MPMC_TestCase.test_all(4, (1, 10), chunk_count % 4 + 1, 1, seed, self)
    def test_small2(self):
        for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
            MPMC_TestCase.test_all(4, (10, 100), chunk_count % 4 + 1, 4, seed, self)
    # def test_small3(self):
    #     for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
    #         MPMC_TestCase.test_all(4, (1, 100), chunk_count % 4 + 1, 4, seed, self)
    # def test_small4(self):
    #     for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
    #         MPMC_TestCase.test_all(128, (1, 50), chunk_count % 4 + 1, 4, seed, self)
    # def test_small5(self):
    #     for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
    #         MPMC_TestCase.test_all(128, (0, 50), chunk_count % 4 + 1, 1024, seed, self)
    # def test_small6(self):
    #     for chunk_count, seed in enumerate([123,1251,523,12,3535,167,945,933]):
    #         MPMC_TestCase.test_all(1024, (0, 50), chunk_count % 4 + 1, 4, seed, self)

if __name__ == '__main__':
    try:
        unittest.main(exit=False)
    finally:
        for K, V in TestCase.spend_time.items():
            print(f"{K} : {V: 4.2f}")
        print("====================")
        for K, V in MPMC_TestCase.spend_time.items():
            print(f"{K} : {V: 4.2f}")