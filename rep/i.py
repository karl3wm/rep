import mmap, os

class fI:
    idsize = memoryview(bytes(0)).cast('Q').itemsize
    allocsize = mmap.PAGESIZE - idsize
    def __init__(self, n='fI.d'):
        self.n = n
        self.d = os.open(self.n, os.O_RDWR | os.O_CREAT)
        y = os.lseek(self.d, 0, os.SEEK_END)
        os.lseek(self.d, 0, os.SEEK_CUR)
        if y > 0:
            self.w = memoryview(mmap.mmap(self.d, y))
            self.q = self.w.cast('Q')
            y = len(self.w)
        if y < self.idsize:
            os.truncate(self.n, mmap.PAGESIZE)
            self.w = memoryview(mmap.mmap(self.d, mmap.PAGESIZE))
            self.q = self.w.cast('Q')
            self.q[0] = 1
        self.yq = len(self.q)
    def alloc(self, data):
        addr8 = self.q[0]
        id = self.w[0:self.q.itemsize].tobytes()
        l8 = (len(data)+self.q.itemsize-1)//self.q.itemsize + 1
        if addr8 + l8 > self.yq:
            y2 = self.yq *2*self.q.itemsize
            if (addr8+l8)*self.q.itemsize > y2:
                y2 = ((addr+l8*2-1)*self.q.itemsize // mmap.PAGESIZE + 1) * mmap.PAGESIZE
            os.truncate(self.n, y2)
            self.w = memoryview(mmap.mmap(self.d, y2))
            self.q = self.w.cast('Q')
            self.yq = len(self.q)
        self.q[addr8] = len(data)
        addr = (addr8 + 1) * self.q.itemsize
        self.w[addr:addr+len(data)] = data
        assert self.fetch(id) == data
        self.q[0] = addr8 + l8
        return id
    def fetch(self, id):
        addr8 = memoryview(id).cast('Q')[0]
        l = self.q[addr8]
        addr = (addr8+1) * self.q.itemsize
        return self.w[addr:addr+l].tobytes()
    def fetch_size(self, id):
        addr8 = memoryview(id).cast('Q')[0]
        return self.q[addr8]
