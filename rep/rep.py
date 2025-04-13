from .r import aR as manager

class Rep:
    def __init__(self, manager=manager()):
        self.manager = manager
    def alloc(self, data):
        sz = self.manager.allocsize
        return b''.join([
            self.manager.alloc(data[off:off+sz])
            for off in range(0,len(data),sz)
        ])
    def fetch(self, id):
        sz = self.manager.idsize
        return b''.join([
            self.manager.fetch(id[off:off+sz])
            for off in range(0,len(id),sz)
        ])

class Document:
    def __init__(self, id=b'', rep=Rep()):
        self.rep = rep
        self._idsize = self.rep.manager.idsize
        self._allocsize = self.rep.manager.allocsize
        sz = self._idsize
        self._ids = [id[off:off+sz] for off in range(0, len(id), sz)]
    @property
    def id(self):
        return b''.join(self._ids)

    def __getitem__(self, slice):
        start, stop, stride = slice.indices(self._allocsize*len(self._ids))
        sz = self._idsize
        start_id = start // sz
        stop_id = ((stop-1) // sz) + 1
        base = start - (start % sz)
        #sz, start_id, stop_id = self._sz_startid_stopid(start, stop)
        #base, start_id, outer, stop_id = self._id_base_start_stop(start, stop)
        data = b''.join([self.rep.manager.fetch(id) for id in self._ids[start_id:stop_id]])
        return data[start - base : stop - base : stride]

    def __setitem__(self, slice, data):
        _dbg_old_data = bytearray(self[:])
        start, stop, stride = slice.indices(self._allocsize*len(self._ids))
        assert len(data) == (stop - start) // stride
        sz = self._idsize
        start_id = start // sz
        base_off = start % sz
        outer_off = stop % sz

        if base_off != 0:
            first = self.rep.manager.fetch(self._ids[start_id])
            data_off = sz - base_off
            first[:base_off] + data[:data_off]
            data_array = [first] + [data[off:off+sz] for off in range(data_off, None, sz)]
        else:
            data_array = [data[off:off+siz] for off in range(0, None, sz)]
        assert len(data[-1]) == outer_off
        if outer_off != 0:
            stop_id = stop // sz
            last = self.manager.fetch(self._ids[stop_id])
            data[-1] = data[-1] + last[outer_off:]

        new_ids = [self.manager.alloc(data_item) for data_item in data]
        self._ids[start_id:len(new_ids)] = new_ids

        _dbg_old_data[slice] = data
        assert _dbg_old_data == self[:]

import bisect, itertools
class ResizeableDocument:
    def __init__(self, id=b'', rep=Rep()):
        self.rep = rep
        self._idsize = self.rep.manager.idsize
        self._allocsize = self.rep.manager.allocsize
        sz = self._idsize
        self._ids = [id[off:off+sz] for off in range(0, len(id), sz)]
        self._sizes = [self.rep.manager.fetch_size(id) for id in self._ids]
        self._offs = list(itertools.accumulate(self._sizes, initial=0))
    def _idx2off(self, idx):
        return sum(self._sizes[:idx])
    def _off2idxoff(self, off, lo=0, hi=None):
        idx = bisect.bisect_right(self._offs, off, lo, hi) - 1
        assert off >= self._offs[idx]
        return [idx, off - self._offs[idx]]
    def fsck(self):
        assert 0 not in self._sizes
        assert self._sizes == [self.rep.manager.fetch_size(id) for id in self._ids]
        assert self._offs == list(itertools.accumulate(self._sizes, initial=0))
    def __len__(self):
        return self._offs[-1]
    def __getitem__(self, slice):
        start, stop, step = slice.indices(len(self))
        start_idx, start_off = self._off2idxoff(start)
        stop_idx, stop_off = self._off2idxoff(stop, start_idx)
        if stop_idx > start_idx:
            if stop_off == 0:
                stop_off = None
                last_idx = stop_idx - 1
            else:
                stop_off -= self._sizes[stop_idx]
                last_idx = stop_idx
                stop_idx += 1
        elif stop_off > start_off:
            stop_off -= self._sizes[stop_idx]
            last_idx = stop_idx
            stop_idx += 1
        else:
            assert start_idx == stop_idx
            assert start_off == stop_off
            return b''
        datas = [self.rep.manager.fetch(id) for id in self._ids[start_idx:stop_idx]]
        datas[0] = datas[0][start_off:]
        datas[-1] = datas[-1][:stop_off]
        data = b''.join(datas)[::step]
        assert len(data) == (stop - start) // step
        return data
    def __setitem__(self, slice, data):
        start, stop, step = slice.indices(len(self))
        assert step == 1
        start_idx, start_off = self._off2idxoff(start)
        stop_idx, stop_off = self._off2idxoff(stop, start_idx)

        # note: additional prefix and suffix material could be added to defrag the content so long as idx count did not increase
        if start_off > 0:
            prefix = self.rep.manager.fetch(self._ids[start_idx])[:start_off]
            prefixlen = start_off
        else:
            prefix = b''
            prefixlen = 0
        if stop_off > 0:
            suffix = self.rep.manager.fetch(self._ids[stop_idx])[stop_off:]
            suffixlen = len(suffix)
            stop_idx += 1
        else:
            suffix = b''
            suffixlen = 0
        datalen = len(data)
        sz = self._allocsize

        if prefixlen + datalen < sz:
            suffixoff = sz - prefixlen - datalen
            data_array = [prefix + data + suffix[:suffixoff]] if suffixlen + prefixlen + datalen > 0 else []
        else:
            off = sz - prefixlen
            data_array = [prefix + data[:off]] + [
                data[off:off+sz]
                for off in range(off, datalen, sz)
            ]
            suffixoff = sz - len(data_array[-1])
            data_array[-1] += suffix[:suffixoff]
        if suffixoff < suffixlen:
            data_array.append(suffix[suffixoff:])
        self._ids[start_idx:stop_idx] = [self.rep.manager.alloc(data_item) for data_item in data_array]
        self._sizes[start_idx:stop_idx] = [len(data_item) for data_item in data_array]
        self._offs[start_idx:] = itertools.accumulate(self._sizes[start_idx:], initial=self._offs[start_idx])
        self.fsck()
    def __iadd__(self, data):
        # reusing setitem for coverage
        self[len(self):] = data
        return self

if __name__ == '__main__':
    import random, tqdm
    text = b'The quick brown fox jumped over the lazy dog.'
    doc = ResizeableDocument()
    cmp = bytearray()
    assert doc[:] == cmp
    doc += text
    cmp += text
    assert doc[:] == cmp
    with tqdm.tqdm(range(24), desc=cmp.decode(), leave=False) as pbar:
        for iter in pbar:
            dest = [random.randint(0,len(doc)) for x in range(2)]
            dest.sort()
            src = [random.randint(0,len(text)) for x in range(2)]
            src.sort()
            assert doc[dest[0]:dest[1]] == cmp[dest[0]:dest[1]]
            doc[dest[0]:dest[1]] = text[src[0]:src[1]]
            cmp[dest[0]:dest[1]] = text[src[0]:src[1]]
            assert cmp == doc[:]
            pbar.desc = cmp.decode()

    doc += text
    cmp += text
    assert doc[:] == cmp
