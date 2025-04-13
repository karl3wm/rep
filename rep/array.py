from .rep import ResizeableDocument, Rep, IterableToBytes, IterableWithLength
import collections

# often i want to fill an array with a lot of data at once
# or process a lot of items from an array
# implementing __iter__ well could help with processing many.
# what about filling? .extend() and __setitem__(slice) can leave a trailing end
# maybe a context for filling a lot at once
# or maybe we'd just store partial data while writing ...
# what if i could pass an iterable, and generate the data :) this might work

# might mean letting doc accept some kind of bytes-stream

# when passing bytes from FixedArray say
# it iterates over a sequence of items and generates bytes as it iterates.
# it could provide a generator in theory -- an iterable of bytes

class FixedArray(collections.abc.MutableSequence):
    def __init__(self, itemsize, id=b'', rep=Rep()):
        self.doc = ResizeableDocument(id, rep)
        self._itemsize = itemsize
    def __len__(self):
        return len(self.doc) // self._itemsize
    def __getitem__(self, slice):
        start, stop, step = slice.indices(len(self))
        sz = self._itemsize
        data = self.doc[start * sz : stop * sz]
        return [data[off:off+sz] for off in range(0,len(data),sz)][::step]
    def __setitem__(self, slice, values):
        start, stop, step = slice.indices(len(self))
        sz = self._itemsize
        dbg_startlen = len(self)
        data = IterableToBytes(len(values) * sz, values)
        #assert len(data) == len(values) * sz
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
        return [fetch(id) for id in super().__getitem__(slice)]
    def __setitem__(self, slice, values):
        alloc = self._alloc
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
            pbar.desc = b','.join(cmp).decode()
