from .r import aR as manager
#from .i import fI as manager

import tqdm

class Rep:
    def __init__(self, manager=None):
        if manager is None:
            manager = globals()['manager']()
        self.manager = manager
    def alloc(self, data, replacing=b''):
        sz = self.manager.allocsize
        return b''.join([
            self.manager.alloc(data[off:off+sz], replacing=[replacing[idx:idx+sz] for idx in range(0,len(replacing),sz)])
            for off in range(0,len(data),sz)
        ])
    def fetch(self, id):
        sz = self.manager.idsize
        return b''.join([
            self.manager.fetch(id[off:off+sz])
            for off in range(0,len(id),sz)
        ])

class Document:
    def __init__(self, id=b'', rep=None):
        if rep is None:
            rep = Rep()
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

    def __iter__(self):
        for id in self._ids:
            yield self.rep.manager.fetch(id)

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

        old_ids = self._ids[start_id:start_id+len(new_ids)]
        new_ids = [self.manager.alloc(data_item, replacing=old_ids) for data_item in data]
        self._ids[start_id:start_id+len(new_ids)] = new_ids

        _dbg_old_data[slice] = data
        assert _dbg_old_data == self[:]

        if old_id in old_ids:
            self.rep.manager.dealloc(old_id)

import bisect, itertools
class ResizeableDocument:
    def __init__(self, id=b'', rep=None):
        if rep is None:
            rep = Rep()
        self.rep = rep
        self._idsize = self.rep.manager.idsize
        self._allocsize = self.rep.manager.allocsize
        sz = self._idsize
        self._ids = [id[off:off+sz] for off in range(0, len(id), sz)]
        self._sizes = [self.rep.manager.fetch_size(id) for id in self._ids]
        self._offs = list(itertools.accumulate(self._sizes, initial=0))
    @property
    def id(self):
        return b''.join(self._ids)
    def _idx2off(self, idx):
        return sum(self._sizes[:idx])
    def _off2idxoff(self, off, lo=0, hi=None):
        idx = bisect.bisect_right(self._offs, off, lo, hi) - 1
        assert off >= self._offs[idx]
        return [idx, off - self._offs[idx]]
    def offset_to_id(self, offset):
        idx, off = self._off2idxoff(offset)
        return self._ids[idx]
    def fsck(self):
        assert 0 not in self._sizes
        assert self._sizes == [self.rep.manager.fetch_size(id) for id in tqdm.tqdm(self._ids, desc='fsck sizes', leave=False)]
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
    def __iter__(self):
        for id in self._ids:
            yield self.rep.manager.fetch(id)
    def __setitem__(self, slice, data):
        start, stop, step = slice.indices(len(self))
        assert step == 1
        start_idx, start_off = self._off2idxoff(start)
        stop_idx, stop_off = self._off2idxoff(stop, start_idx)
        old_ids = self._ids[start_idx:stop_idx]

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

        new_ids = []
        new_sizes = []
        alloc = self.rep.manager.alloc
        if prefixlen + datalen < sz:
            suffixoff = sz - prefixlen - datalen
            if suffixlen + prefixlen + datalen > 0:
                piece = prefix + data[:] + suffix[:suffixoff]
                new_sizes.append(len(piece))
                new_ids.append(alloc(piece, replacing=old_ids))
        else:
            off = sz - prefixlen
            piece = prefix + data[:off]
            new_sizes.append(len(piece))
            new_ids.append(alloc(piece, replacing=old_ids))
            offs = list(range(off, datalen, sz))
            for off in offs[:-1]:
                piece = data[off:off+sz]
                new_sizes.append(len(piece))
                new_ids.append(alloc(piece, replacing=old_ids))
            tail = data[offs[-1]:]
            suffixoff = sz - len(tail)
            piece = tail + suffix[:suffixoff]
            new_sizes.append(len(piece))
            new_ids.append(alloc(piece, replacing=old_ids))
        if suffixoff < suffixlen:
            piece = suffix[suffixoff:]
            new_sizes.append(len(piece))
            new_ids.append(alloc(piece, replacing=old_ids))
        self._ids[start_idx:stop_idx] = new_ids#[self.rep.manager.alloc(data_item) for data_item in data_array]
        self._sizes[start_idx:stop_idx] = new_sizes#[len(data_item) for data_item in data_array]
        self._offs[start_idx:] = itertools.accumulate(self._sizes[start_idx:], initial=self._offs[start_idx])
        self.fsck()
    def update(self, *start_stop_data):
        start_stop_data = list(start_stop_data)
        start_stop_data.sort()
        # group them by id
        # i'm not sure i have the capacity rn to implement this algorithm reasonably. some parts expressing a lot of hopelessness/etc repeatedly/persistently/aggressively
            # irritating. we're looking for a simpler way to make the structures usable.
            # basically, dicts are sparse. we'd like to perform a number of updates at once.
            # i guess that means fetching each id, and placing new data into it at once.
    def __iadd__(self, data):
        # reusing setitem for coverage
        self[len(self):] = data
        return self

class IterableToBytes:
    def __init__(self, length, iterable):
        self.length = length
        self.iteration = iter(iterable)
        self.offset = 0
        self.buffer = bytearray()
    def __len__(self):
        return self.length
    def __getitem__(self, slice):
        start, stop, step = slice.indices(self.length)
        assert start == self.offset
        buf = self.buffer
        length = stop - start
        while len(buf) < length:
            buf += next(self.iteration)
        self.offset += length
        if self.offset == self.length:
            try:
                extra_value = next(self.iteration)
                assert not 'length shorter than data'
            except StopIteration:
                pass
        result = buf[:length][::step]
        buf[:length] = b''
        return result

class IterableWithLength:
    def __init__(self, iter, length):
        self.iter = iter
        self.length = length
    def __len__(self):
        return self.length
    def __iter__(self):
        return iter(self.iter)

if __name__ == '__main__':
    import random, tqdm, time
    for seed in [1745156337, int(time.time())]:
        print(f'random.seed({seed})')
        random.seed(seed)
        text = b'The quick brown fox jumped over the lazy dog.'
        doc = ResizeableDocument()
        cmp = bytearray()
        assert doc[:] == cmp
        doc += text
        cmp += text
        assert doc[:] == cmp
        with tqdm.tqdm(range(24), desc=cmp.decode(), leave=False) as pbar:
            for it in pbar:
                dest = [random.randint(0,len(doc)) for x in range(2)]
                dest.sort()
                src = [random.randint(0,len(text)) for x in range(2)]
                src.sort()
                assert doc[dest[0]:dest[1]] == cmp[dest[0]:dest[1]]
                doc[dest[0]:dest[1]] = IterableToBytes(src[1]-src[0], [text[src[0]:(src[0]+src[1])//2],text[(src[0]+src[1])//2:src[1]]])
                cmp[dest[0]:dest[1]] = text[src[0]:src[1]]
                assert cmp == doc[:]
                pbar.desc = cmp.decode()
        doc += text
        cmp += text
        assert doc[:] == cmp
