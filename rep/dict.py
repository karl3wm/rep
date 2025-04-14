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

# the following ideas were encountered:
# - the loop where the growth amount is measured could be simplified into a bitwise check between the hashes if such a check exists.
#   it could at least have other content pulled out of the loop.
# - by using little-endian instead of big-endian keyhash expansion, growth could be zero-allocation by imaging pages.
#   this implies either not tracking presence or using a different method than a sentinel to do so.

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
#   - cloning tables to grow
#           efficient constant allocation, leaves data in unallocated areas, might use little-endian hashing

class FixedDict(collections.abc.MutableMapping):
    # this approach expands on collisions.
    # so every fetch encounters at most one value, because collisions always make sparsity
    def __init__(self, itemsize, key, id=b'', rep=Rep()):
        self._itemsize = itemsize
        self._key = key
        self._rep = rep
        self.array = FixedArray(self._itemsize, id, rep)
        self._capacity = len(self.array)
        self._sentinel = bytes(self._itemsize)
        self._hashbits = self._capacity.bit_length()
        if self._capacity > 0:
            self._hashbits = (self._capacity-1).bit_length()
            assert 1<<(self._hashbits) == self._capacity
        else:
            self._hashbits = 0
        self._hashbytes = (self._hashbits+7) >> 3
        self._hashshift = (self._hashbytes << 3) - self._hashbits
    def __len__(self):
        raise NotImplementedError('len(FixedDict)')
    def __getitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
        return self.array[idx]
    def __setitem__(self, keyhash, item):
        assert self._key(item) == keyhash # interestingly this is the same as a set
        if self._capacity == 0:
            self._capacity = 2
            self._hashbytes = 1
            self._hashbits = 1
            self._hashshift = 7
            self.array[:] = [self._sentinel, self._sentinel]
        assert item != self._sentinel
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
        place = self.array[idx]
        if place != self._sentinel:
            collision = self._key(place)
            if collision != keyhash:
                assert idx == int.from_bytes(collision[:self._hashbytes], 'big') >> self._hashshift
                spread = 0
                while True:
                    spread += 1
                    hashbits = self._hashbits + spread
                    expansion = 1 << spread
                    hashbytes = (hashbits+7) >> 3
                    hashshift = (hashbytes << 3) - hashbits
                    idx = int.from_bytes(keyhash[:hashbytes], 'big') >> hashshift
                    if idx != int.from_bytes(collision[:hashbytes], 'big') >> hashshift:
                        break
                capacity = self._capacity * expansion
                assert 1 << hashbits == capacity
                expansionmask = expansion - 1
                def content_generator():
                    for superidx, item in enumerate(tqdm.tqdm(self.array, desc='growing hashtable', leave=False)):
                        if item == self._sentinel:
                            yield from [self._sentinel] * expansion
                        else:
                            keyhash = self._key(item)
                            wholeidx = int.from_bytes(keyhash[:hashbytes], 'big')
                            assert superidx == wholeidx >> (hashbytes * 8 - self._hashbits)
                            subidx = (wholeidx >> hashshift) & expansionmask
                            assert superidx * expansion + subidx == wholeidx >> hashshift
                            yield from [self._sentinel] * subidx
                            yield item
                            yield from [self._sentinel] * (expansion - subidx - 1)
                self.array[:] = IterableWithLength(content_generator(), self._capacity * expansion)
                self._capacity = capacity
                self._hashbits = hashbits
                self._hashbytes = hashbytes
                self._hashshift = hashshift
                dict(self.items()) # fsckish
        self.array[idx] = item
    def __iter__(self):
        for item in self.array:
            if item != self._sentinel:
                yield [self._key(item), item]
    def __delitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
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
        alloc = self._alloc
        if self._capacity > 0:
            storedkeyval = super().__getitem__(keyhash)
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
                assert idx == int.from_bytes(hash(key)[:self._hashbytes], 'big') >> self._hashshift
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
                assert idx == int.from_bytes(hash(key)[:self._hashbytes], 'big') >> self._hashshift
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
    for x in tqdm.tqdm(range(17)):
        val = str(x).encode()
        doc[val] = val
        cmp[val] = val
        assert dict(doc.items()) == cmp
