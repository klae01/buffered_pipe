import pandas
import time
import multiprocessing
import os
from buffered_pipe import Static_Pipe

def SB(Q, pipe, D):
    Q.put(time.time())
    list(map(pipe.send_bytes, D))
    Q.put(time.time())

def test(cnt, size, Datas, ctx, method, parameters):
    Q = ctx.Queue()
    if method == 'Static_Pipe':
        pipe_r, pipe_w = Static_Pipe(*parameters)
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
    # pipe_r, pipe_w = multiprocessing.Pipe() #379780.12it/s
    # pipe_r, pipe_w = Static_Pipe(size, 512, 1, 0.01) #1194458.16it/s
    # pipe_r, pipe_w = Static_Pipe(size, 512, 4, 0.01) #956994.65it/s
    results = []
    for ctx_method in ['fork', 'spawn', 'forkserver']:
        ctx = multiprocessing.get_context(ctx_method)

        results += [
          test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 512, 8, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 512, 4, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 512, 2, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 512, 1, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 64, 8, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 64, 4, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 64, 2, 0.01])
        , test(cnt, size, Datas, ctx, method = "Static_Pipe", parameters = [size, 64, 1, 0.01])
        , test(cnt, size, Datas, ctx, method = "Pipe", parameters = [True])
        , test(cnt, size, Datas, ctx, method = "Pipe", parameters = [False])
        ]
    df = pandas.DataFrame(results)
    print(df.to_string())
