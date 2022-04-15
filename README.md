# buffered_pipe
Buffered pipe through shared memory.

## core features
- `Pipe`: alias for `Generic_Pipe`
- `Generic_Pipe`: free-length data, bytes or picklable
- `Static_Pipe`: fixed length data, only bytes supported

## install
--------------------
```shell
pip install buffered-pipe
```

## usage
--------------------

- Generic_Pipe
```python
import multiprocessing
import threading

from buffered_pipe import Generic_Pipe


def foo(barrier, pipe_send):
    pipe_send.register()
    barrier.wait()
    pipe_send.send("Hello, world!")


def bar(barrier, pipe_recv):
    pipe_recv.register()
    barrier.wait()
    print(pipe_recv.recv())


if __name__ == "__main__":
    barrier = multiprocessing.Barrier(3)
    pipe_recv, pipe_send = Generic_Pipe(
        buffer_size=2048, duplex=False
    )
    P = multiprocessing.Process(target=foo, args=(barrier, pipe_send))
    T = threading.Thread(target=bar, args=(barrier, pipe_recv))
    P.start()
    T.start()
    barrier.wait()
    P.join()
    T.join()
```

- Static_Pipe
```python
import multiprocessing
import threading

from buffered_pipe import Static_Pipe


def foo(barrier, pipe_send):
    pipe_send.register()
    barrier.wait()
    ret_string = "Hello, world!"
    ret_string = ret_string + " " * (32 - len(ret_string))
    ret_string = ret_string.encode()
    pipe_send.send(ret_string)


def bar(barrier, pipe_recv):
    pipe_recv.register()
    barrier.wait()
    ret_string = pipe_recv.recv()
    print(ret_string.decode())


if __name__ == "__main__":
    barrier = multiprocessing.Barrier(3)
    pipe_recv, pipe_send = Static_Pipe(object_size=32, object_count=16, duplex=False)
    P = multiprocessing.Process(target=foo, args=(barrier, pipe_send))
    T = threading.Thread(target=bar, args=(barrier, pipe_recv))
    P.start()
    T.start()
    barrier.wait()
    P.join()
    T.join()
```

- When you create a `process` or `thread`, call `register` of `Pipe`
- The `buffer_size` of `Generic_Pipe` is in `bytes`.
- The `object_size` of `Static_Pipe` is in `bytes`.
- `object_count` in `Static_Pipe` is the maximum number of objects placed in the buffer.
- `duplex` is equivalent to `multiprocessing.Pipe`.


## tests
--------------
```shell
> benchmark_result.txt
for f in benchmarks/*.py; do python3 "$f" &>> benchmark_result.txt; done
> test_result.txt
for f in tests/*.py; do python3 "$f" &>> test_result.txt; done
```