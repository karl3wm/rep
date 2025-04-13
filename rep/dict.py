from .rep import Rep, IterableWithLength
from .array import FixedArray
import collections
import tqdm

from cryptography.hazmat.primitives.hashes import Hash as H, SHA256 as DIGEST
DIGEST = DIGEST()
def hash(bytes):
    hash = H(DIGEST)
    hash.update(bytes)
    return hash.finalize()

# the current dict approach grows the size first to 256 then 64k
# but since the hash is never written it would be fine to use a bitcount rather than a bytecount for more reasonable storage usage for small dicts

# approaches to dicts:
#   - header free
#           if data is the only thing held, then either sparsity is held with a sentinel or bisect search is used to find items (both can work too)
#   - nested
#           large pages can make a compromise between trees and lists to provide for very huge stores with log(n)/log(pagesize) access time
#   - sequential
#           this can provide for direct lookup but may involve unhashed lists of indices. current approach
#   - per-table metadata
#           this can help with information like number of items, or depth, or itemsize
#   - per-entry metadata
#           this is maybe normal helpful, provides for things like bucket usage
#   - holding key/keyhash
#           makes resizing much easier, can reindex

class FixedDict(collections.abc.MutableMapping):
    def __init__(self, itemsize, key, id=b'', rep=Rep()):
        self._itemsize = itemsize
        self._key = key
        self._rep = rep
        self.array = FixedArray(self._itemsize, id, rep)
        self._capacity = len(self.array)
        self._sentinel = bytes(self._itemsize)
        if self._capacity > 0:
            self._hashbytes = ((self._capacity-1).bit_length()-1) // 8 + 1
            assert 1<<(self._hashbytes<<3) == self._capacity
        else:
            self._hashbytes = 0
    def __len__(self):
        raise NotImplementedError('len(FixedDict)')
    def __getitem__(self, keyhash):
        # some interest in an approach that expands on collisions.
        # this means every fetch either finds zeros or a single value, because collisions always make sparsity
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big')
        return self.array[idx]
    def __setitem__(self, keyhash, item):
        assert self._key(item) == keyhash # interestingly this is the same as a set
        if self._capacity == 0:
            self.array[:] = [self._sentinel] * 256
            self._capacity = 256
            self._hashbytes = 1
        assert item != self._sentinel
        while True:
            idx = int.from_bytes(keyhash[:self._hashbytes], 'big')
            place = self.array[idx]
            if place == self._sentinel or self._key(place) == keyhash:
                break
            def content_generator():
                for superidx, item in enumerate(tqdm.tqdm(self.array, desc='growing hashtable', leave=False)):
                    if item == self._sentinel:
                        yield from [self._sentinel] * 256
                    else:
                        keyhash = self._key(item)
                        assert int.from_bytes(keyhash[:self._hashbytes], 'big') == superidx
                        subidx = keyhash[self._hashbytes]
                        yield from [self._sentinel] * subidx
                        yield item
                        yield from [self._sentinel] * (255 - subidx)

            self.array[:] = IterableWithLength(content_generator(), self._capacity * 256)
            self._capacity *= 256
            self._hashbytes += 1
        self.array[idx] = item
    def __iter__(self):
        for item in self.array:
            if item != self._sentinel:
                yield [self._key(item), item]
    def __delitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big')
        assert self.array[idx] != self._sentinel
        self.array[idx] = self._sentinel

class Dict(FixedDict):
    def __init__(self, id=b'', rep=Rep()):
        super().__init__(rep.manager.idsize*2, self._key, id, rep)
        self._alloc = rep.manager.alloc
        self._fetch = rep.manager.fetch
        self._idsize = rep.manager.idsize
        self._keycache = {}
    def _key(self, keyval):
        return hash(self._fetch(keyval[:self._idsize]))
    def __getitem__(self, key):
        keyhash = hash(key)
        storedkeyval = super().__getitem__(keyhash)
        if storedkeyval != self._sentinel:
            sz = self._idsize
            storedkey = self._fetch(storedkeyval[:sz])
            if storedkey == key:
                return self._fetch(storedkeyval[sz:])
        raise KeyError(key)
    def __setitem__(self, key, val):
        # if the item is present already, then only the second half of the key need be updated
        keyhash = hash(key)
        storedkeyval = super().__getitem__(keyhash)
        alloc = self._alloc
        if storedkeyval != self._sentinel:
            storedkeyid = storedkeyval[:self._idsize]
            storedkey = self._fetch(storedkeyid)
            if storedkey == key:
                return super().__setitem__(keyhash, storedkeyid + alloc(val))
        super().__setitem__(keyhash, alloc(key) + alloc(val))
    def keys(self):
        sz = self._idsize
        fetch = self._fetch
        sentinel = self._sentinel
        for idx, storedkeyval in enumerate(self.array):
            if storedkeyval != sentinel:
                key = fetch(storedkeyval[:sz])
                assert int.from_bytes(hash(key)[:self._hashbytes], 'big') == idx
                yield key
    def values(self):
        raise NotImplementedError() # values implies not checking keyhashes
    def items(self):
        sz = self._idsize
        fetch = self._fetch
        sentinel = self._sentinel
        for idx, storedkeyval in enumerate(self.array):
            if storedkeyval != sentinel:
                key = fetch(storedkeyval[:sz])
                assert int.from_bytes(hash(key)[:self._hashbytes], 'big') == idx
                yield [key, fetch(storedkeyval[sz:])]
    def __iter__(self):
        return self.keys()
    def __delitem__(self, key):
        keyhash = hash(key)
        storedkeyval = super().__getitem__(keyhash)
        if storedkeyval != self._sentinel:
            storedkeyid = storedkeyval[:self._idsize]
            storedkey = self._fetch(storedkeyid)
            if storedkey == key:
                return super().__delitem__(keyhash)
        raise KeyError(key)

if __name__ == '__main__':
    doc = Dict()
    cmp = {}
    for x in tqdm.tqdm(range(257)):
        val = str(x).encode()
        doc[val] = val
        cmp[val] = val
        assert dict(doc.items()) == cmp
