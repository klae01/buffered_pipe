import pandas
import time
import multiprocessing
import os
from buffered_pipe import Pipe as Generic_Pipe

def SB(Q, pipe, D):
    Q.put(time.time())
    list(map(pipe.send_bytes, D))
    Q.put(time.time())

def test(cnt, size, Datas, ctx, method, parameters):
    Q = ctx.Queue()
    if method == 'Generic_Pipe':
        pipe_r, pipe_w = Generic_Pipe(*parameters)
        P = ctx.Process(target=SB, args=(Q, pipe_w, Datas))
    if method == 'Pipe':
        pipe_r, pipe_w = ctx.Pipe(*parameters)
        P = ctx.Process(target=SB, args=(Q, pipe_w, Datas))
    P.start()

    Ts = []
    Ts += [time.time()]
    error_cnt = sum( exp != pipe_r.recv_bytes() for exp in Datas)
    Ts += [time.time()]
    P.join()
    
    Ps = [Q.get() for _ in range(Q.qsize())]

    return { 
          "error": error_cnt / cnt
        , "execute delay": Ps[0] - Ts[0]
        , "send time": Ps[1] - Ps[0]
        , "recv time": Ts[1] - Ps[0]
        , "last packet delay": Ts[1] - Ps[1]
        , "ctx" : type(ctx).__name__
        , "method": method
        , "parameters": parameters
    }

if __name__ == '__main__':
    path_info = __file__.split('/')
    path_info = '/'.join(path_info[path_info.index('benchmarks'):])
    print(path_info)
    cnt = 1000000
    size = 128
    Datas = [os.urandom(size) for _ in range(cnt)]
    results = []
    for ctx_method in ['fork', 'spawn', 'forkserver']:
        ctx = multiprocessing.get_context(ctx_method)

        results += [
          test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**18, 1])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**18, 4])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**18, 16])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**14, 1])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**14, 4])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [64, 2**14, 16])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**14, 1])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**14, 4])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**14, 16])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**10, 1])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**10, 4])
        , test(cnt, size, Datas, ctx, method = "Generic_Pipe", parameters = [16, 2**10, 16])
        , test(cnt, size, Datas, ctx, method = "Pipe", parameters = [True])
        , test(cnt, size, Datas, ctx, method = "Pipe", parameters = [False])
        ]
    df = pandas.DataFrame(results)
    print(df.to_string())
