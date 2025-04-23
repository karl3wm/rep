import mmap, os

# note: this uses a linked list of deallocated regions,
#       but does not merge them, potentially causing fragmentation

class fI:
    idsize = memoryview(bytes(0)).cast('Q').itemsize
    allocsize = mmap.PAGESIZE - idsize
    def __init__(self, n='fI.d'):
        import atexit
        self.n = n
        self.d = os.open(self.n, os.O_RDWR | os.O_CREAT)
        y = os.lseek(self.d, 0, os.SEEK_END)
        os.lseek(self.d, 0, os.SEEK_CUR)
        if y > 0:
            # - assert that the structure is correct
            self.w = memoryview(mmap.mmap(self.d, y))
            self.q = self.w.cast('Q')
            y = len(self.w)
        if y < self.idsize:
            os.truncate(self.n, mmap.PAGESIZE)
            self.w = memoryview(mmap.mmap(self.d, mmap.PAGESIZE))
            self.q = self.w.cast('Q')
            self.q[0] = 1
            self.q[1] = 0
            self.q[2] = len(self.q) - 1
        self.yq = len(self.q)
        atexit.register(self.shrink)
    def dealloc(self, id):
        self.fsck()
        addr8 = memoryview(id).cast('Q')[0]
        l8 = max((self.q[addr8]+self.idsize-1)//self.idsize,1) + 1
        self.q[addr8] = self.q[0]
        self.q[addr8+1] = l8
        self.q[0] = addr8
        self.fsck()
    def alloc(self, data, replacing=[]):
        self.fsck()
        l8 = max((len(data)+self.idsize-1)//self.idsize,1) + 1
        addr8 = 0
        assert self.q[addr8] != 0
        dbg_seen = {}
        while self.q[addr8] != 0:
            assert addr8 not in dbg_seen
            prev8 = addr8
            addr8 = self.q[prev8]
            assert prev8 != addr8
            dbg_seen[prev8] = addr8
            assert self.q[addr8+1] + addr8 <= self.yq
            if (self.q[addr8+1] == l8 and self.q[addr8] != 0) or self.q[addr8+1] >= l8+2:
                # found region
                break
        else:
            # no regions were found that data fits in. expand the last one and use it.
            assert self.q[addr8+1] == self.yq - addr8
            y2 = self.yq *2*self.idsize
            if (addr8+l8+2)*self.idsize > y2: # two extra to ensure there is always an unallocated region to use
                y2 = ((addr8+(l8+2)*2-1)*self.idsize // mmap.PAGESIZE + 1) * mmap.PAGESIZE
            os.truncate(self.n, y2)
            self.w = memoryview(mmap.mmap(self.d, y2))
            self.q = self.w.cast('Q')
            self.yq = len(self.q)
            self.q[addr8+1] = self.yq - addr8
            self.fsck()
        # set id from its address
        id = self.q[prev8:prev8+1].tobytes()
        # remove region from linked list
        if self.q[addr8+1] > l8:
            assert prev8 != 0 or addr8 + l8 != 0
            assert self.q[addr8+1] >= l8 + 2
            self.q[prev8] = addr8 + l8
            self.q[addr8+l8] = self.q[addr8]
            self.q[addr8+l8+1] = self.q[addr8+1] - l8
        else:
            assert prev8 != 0 or self.q[addr8] != 0
            self.q[prev8] = self.q[addr8]
        addr0 = addr8 * self.idsize
        addr1 = addr0 + self.idsize
        self.q[addr8] = len(data)
        self.w[addr1:addr1+len(data)] = data
        #for replaced in replacing:
        #    self._dealloc(replaced)
        assert self.fetch(id) == data
        self.fsck()
        return id
    def fetch(self, id):
        addr8 = memoryview(id).cast('Q')[0]
        l = self.q[addr8]
        addr = (addr8+1) * self.idsize
        return self.w[addr:addr+l].tobytes()
    def fetch_size(self, id):
        addr8 = memoryview(id).cast('Q')[0]
        return self.q[addr8]
    def fsck(self):
        regions = []
        prev8 = 0; addr8 = self.q[prev8]
        while addr8 != 0:
            regions.append([addr8, self.q[addr8+1]])
            prev8 = addr8
            addr8 = self.q[prev8]
            assert prev8 != addr8
        regions.sort(reverse=True)
        passed_regions = []
        addr8 = 1
        while addr8 < self.yq:
            assert regions[-1][0] >= addr8
            if regions[-1][0] == addr8:
                region = regions.pop()
                passed_regions.append(region)
                assert region[1] <= self.yq - addr8
                addr8 += region[1]
            else:
                l8 = max((self.q[addr8]+self.idsize-1)//self.idsize,1) + 1
                assert l8 <= self.yq - addr8
                addr8 += l8
        assert addr8 == self.yq
    def shrink(self):
        self.fsck()
        unused = 0
        regions = []
        prev8 = 0; addr8 = self.q[prev8]
        while addr8 != 0:
            regions.append(prev8)#[addr8, prev8])
            prev8 = addr8
            addr8 = self.q[prev8]
            assert prev8 != addr8
            unused += self.q[prev8+1]
        # merge regions
        regions.sort(key=lambda prev8:self.q[prev8])
        # idx   head    prev
        # 0     1       5
        # 1     5       0 <-
        # 2     11      1
        # note:
        #   q[5] == 11
        #   q[0] == 1
        #   q[1] == 11
        for idx in range(1, len(regions)):
            self.fsck()
            prev0, prev1 = regions[idx-1:idx+1]
            #head0, prev0 = regions[idx-1]
            head0 = self.q[prev0]
            tail0 = self.q[head0+1] + head0
            #assert self.q[prev0] == head0 # <-
            #head1, prev1 = regions[idx]
            head1 = self.q[prev1]
            tail1 = self.q[head1+1] + head1
            #assert self.q[prev1] == head1
            self.fsck()
            if tail0 == head1:
                # remove region0
                assert self._calc_unused() == unused
                if prev1 == head0:
                    assert head1 != prev0
                    self.q[prev0] = head1
                    prev1 = prev0
                else:
                    assert self.q[head0] != prev0
                    self.q[prev0] = self.q[head0]
                assert self._calc_unused() == unused - (tail0 - head0)

                assert self.q[head1] != head0
                self.q[head0] = self.q[head1] # set region0's next to region1's next
                self.q[head0+1] = tail1 - head0 # set region0's length to region0's + region1's
                assert head0 != prev1
                self.q[prev1] = head0 # <- reseat region1 to be region0 + region1
                #regions[idx][0] = head0 # <-
                assert self._calc_unused() == unused
                self.fsck()
            else:
                assert tail0 < head1
            assert self._calc_unused() == unused
        # remove tail region
        self.fsck()
        regions.sort(key=lambda prev8: self.q[prev8])
        prev8 = regions[-1]
        addr8 = self.q[prev8]
        unused += 2 - self.q[addr8+1]
        y2 = (addr8+2)*self.idsize
        os.truncate(self.n, y2)
        self.w = memoryview(mmap.mmap(self.d, y2))
        self.q = self.w.cast('Q')
        self.yq = len(self.q)
        self.q[addr8+1] = self.yq - addr8
        assert self.q[addr8+1] == 2
        self.fsck()
        print('used:  ', (self.yq - unused) * self.idsize)
        print('unused:', unused * self.idsize)
        print('total: ', self.yq * self.idsize)
    def _calc_unused(self):
        unused = 0
        prev8 = 0; addr8 = self.q[prev8]
        while addr8 != 0:
            assert self.q[addr8+1] + addr8 <= self.yq
            prev8 = addr8
            addr8 = self.q[prev8]
            assert addr8 != prev8
            unused += self.q[prev8+1]
        return unused
