# buffered_pipe
Buffered pipe through shared memory.

## core features
- Pipe: free-length data, bytes or picklable
- Static_Pipe: fixed length data, support bytes only

## tests
--------------
```shell
> test_result.txt
for f in tests/*.py; do python3 "$f" &>> test_result.txt; done
```