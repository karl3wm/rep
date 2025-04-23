from .rep import ResizeableDocument, Rep, IterableToBytes, IterableWithLength
import collections

class FixedArray(collections.abc.MutableSequence):
    def __init__(self, itemsize, id=b'', rep=None):
        self.doc = ResizeableDocument(id, rep)
        self._itemsize = itemsize
    @property
    def id(self):
        return self.doc.id
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
    def index_to_id(self, index):
        id = self.doc.offset_to_id(index * sz)
        assert self.doc.offset_to_id(index * sz + sz - 1) == id
        return id
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
    #def update(self, idxvaluesdict, **idxvalueskws):
    #        # this is kind of a lot of implementations
    #    #self.doc.update([
    #    #    [start * sz, stop * sz, data]
    #    #])
    #    idxvalues = list(dict(idxvaluesdict).items()) + list(idxvalueskws.items())
    #    idxvalues.sort()
    #    last_idx = 0
    #    for idx, value in idxvalues:
            
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
    def __init__(self, id=b'', rep=None):
        if rep is None:
            rep = Rep()
        super().__init__(rep.manager.idsize, id, rep)
        self._alloc = rep.manager.alloc
        self._fetch = rep.manager.fetch
        self._dealloc = rep.manager.dealloc
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
        dealloc = self._dealloc
        if type(slice) is int:
            old_ids = [super().__getitem__(slice)]
            super().__setitem__(slice, alloc(values, replacing=old_ids))
        else:
            old_ids = super().__getitem__(slice)
            super().__setitem__(slice, IterableWithLength((alloc(value, replacing=old_ids) for value in values), len(values)))
        for old_id in old_ids:
            dealloc(old_id)

if __name__ == '__main__':
    import random, time, tqdm
    for seed in [1745318607,int(time.time())]:
        print(f'random.seed({seed})')
        random.seed(seed)
        text = b'The quick brown fox jumped over the lazy dog.'
        doc = Array()
        cmp = []
        assert doc[:] == cmp
        with tqdm.tqdm(range(1), desc=b','.join(cmp).decode(), leave=False) as pbar:
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
        doc[:] = b''
        if hasattr(doc.doc.rep.manager, 'shrink'):
            doc.doc.rep.manager.shrink()
