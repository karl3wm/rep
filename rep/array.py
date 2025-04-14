from .rep import ResizeableDocument, Rep, IterableToBytes, IterableWithLength
import collections

class FixedArray(collections.abc.MutableSequence):
    def __init__(self, itemsize, id=b'', rep=Rep()):
        self.doc = ResizeableDocument(id, rep)
        self._itemsize = itemsize
    def __len__(self):
        return len(self.doc) // self._itemsize
    def __getitem__(self, slice):
        sz = self._itemsize
        if type(slice) is int:
            if slice < 0 or slice >= len(self):
                raise IndexError('index out of range')
            return self.doc[slice * sz : (slice + 1) * sz]
        else:
            start, stop, step = slice.indices(len(self))
            data = self.doc[start * sz : stop * sz]
            return [data[off:off+sz] for off in range(0,len(data),sz)][::step]
    def __iter__(self):
        sz = self._itemsize
        buf = bytearray()
        szminusbuflen = sz
        for region in self.doc:
            regionlen = len(region)
            if regionlen < szminusbuflen:
                buf += region
                szminusbuflen -= regionlen
                continue
            yield buf + region[:szminusbuflen]
            buflen = (regionlen - szminusbuflen) % sz
            tailoff = regionlen - buflen
            for off in range(szminusbuflen, tailoff, sz):
                yield region[off:off+sz]
            buf[:] = region[tailoff:]
            szminusbuflen = sz - buflen
        assert szminusbuflen == sz
    def __setitem__(self, slice, values):
        if type(slice) is int:
            start, stop, step = [slice, slice + 1, 1]
            if start < 0 or start >= len(self):
                raise IndexError('index out of range')
            values = [values]
        else:
            start, stop, step = slice.indices(len(self))
        sz = self._itemsize
        dbg_startlen = len(self)
        data = IterableToBytes(len(values) * sz, values)
        self.doc[start * sz : stop * sz] = data
        assert len(self) == dbg_startlen + len(values) - (stop - start)
    def __delitem__(self, slice):
        self[slice] = []
    def insert(self, idx, value):
        self[idx:idx] = [value]
    @property
    def itemsize(self):
        return self._itemsize
    def mutate_all(self, mutator):
        length = len(self)
        self.doc[:] = b''.join([
            mutator(value) for value in self
        ])
        self._itemsize = len(self.doc) // length
        assert self._itemsize * length == len(self.doc)

class Array(FixedArray):
    def __init__(self, id=b'', rep=Rep()):
        super().__init__(rep.manager.idsize, id, rep)
        self._alloc = rep.manager.alloc
        self._fetch = rep.manager.fetch
    def __getitem__(self, slice):
        fetch = self._fetch
        if type(slice) is int:
            return fetch(super().__getitem__(slice))
        else:
            return [fetch(id) for id in super().__getitem__(slice)]
    def __iter__(self):
        fetch = self._fetch
        for id in super().__iter__():
            yield fetch(id)
    def __setitem__(self, slice, values):
        alloc = self._alloc
        if type(slice) is int:
            super().__setitem__(slice, alloc(values))
        else:
            super().__setitem__(slice, IterableWithLength((alloc(value) for value in values), len(values)))

if __name__ == '__main__':
    import random, tqdm
    text = b'The quick brown fox jumped over the lazy dog.'
    doc = Array()
    cmp = []
    assert doc[:] == cmp
    with tqdm.tqdm(range(32), desc=b','.join(cmp).decode(), leave=False) as pbar:
        for iter in pbar:
            dest = [random.randint(0,len(doc)) for x in range(2)]
            dest.sort()
            srcs = []
            srcslen = random.randint(0,3)
            for idx in range(srcslen):
                src = [random.randint(0,len(text)) for x in range(2)]
                src.sort()
                srcs.append(text[src[0]:src[1]])
            assert doc[dest[0]:dest[1]] == cmp[dest[0]:dest[1]]
            doc[dest[0]:dest[1]] = srcs
            cmp[dest[0]:dest[1]] = srcs
            assert cmp == doc[:]
            assert cmp == list(doc)
            pbar.desc = b','.join(cmp).decode()
