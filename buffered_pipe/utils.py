import copy

class Duplex_Pipe:
    def __init__(self, create_fn):
        self.pipe_1 = create_fn()
        self.pipe_2 = create_fn()
        self.mode = None

    def set_mode(self, mode):
        assert self.mode is None
        assert mode in ["RW", "WR"]
        self.mode = mode

    def recv_bytes(self):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv_bytes()

    def send_bytes(self, data):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "R"]
        pipe.send_bytes(data)

    def recv(self):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.recv()

    def send(self, item):
        pipe = [self.pipe_1, self.pipe_2][self.mode[0] == "W"]
        return pipe.send(item)

def get_duplex_Pipe(create_fn):
    pipe_1 = Duplex_Pipe(create_fn)
    pipe_2 = copy.copy(pipe_1)
    pipe_1.set_mode("RW")
    pipe_2.set_mode("WR")
    return pipe_1, pipe_2