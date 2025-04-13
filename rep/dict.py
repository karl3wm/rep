from .rep import Rep, ResizeableDocument, IterableWithLength
from .array import FixedArray
import collections
import tqdm

from cryptography.hazmat.primitives.hashes import Hash as H, SHA256 as DIGEST
DIGEST = DIGEST()
def hash(bytes):
    hash = H(DIGEST)
    hash.update(bytes)
    return hash.finalize()

# DRAFT nearly works, has logic bugs


# approaches to dicts:
#   - header free
#           if data is the only thing held, then either sparsity is held with a sentinel or bisect search is used to find items (both can work too)
#   - nested
#           large pages can make a compromise between trees and lists to provide for very huge stores with log(n/pagesize) access time
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
        self._hashbytes = ((self._capacity-1).bit_length()-1) // 8 + 1
        self._sentinel = bytes(self._itemsize)
        assert 1<<(self._hashbytes<<3) == self._capacity
    def __len__(self):
        raise NotImplementedError('len(FixedDict)')
    def __getitem__(self, keyhash):
        # some interest in an approach that expands on collisions.
        # this means every fetch either finds zeros or a single value, because collisions always make sparsity
        idx = int.from_bytes(keyhash[:self.hash_bytes], 'big')
        return self.array[idx]
    def __setitem__(self, keyhash, item):
        assert self._key(item) == keyhash # interestingly this is the same as a set
        idx = int.from_bytes(keyhash[:self.hash_bytes], 'big')
        assert item != self._sentinel
        while True:
            place = self.array[idx]
            if place == self._sentinel or self._key(place) == keyhash:
                break
            def content_generator():
                for item in tqdm.tqdm(self.array, desc='growing hashtable', leave=False):
                    subidx = self._key(item)[self.hash_bytes]
                    yield from [self._sentinel] * subidx
                    yield item
                    yield from [self._sentinel] * (255 - subidx)
            self.array[:] = IterableWithLength(content_generator(), self._capacity * 256)
            self._capacity *= 256
            self._hash_bytes += 1
        self.array[idx] = item
    def __iter__(self):
        for item in self.array:
            if item != self._sentinel:
                yield [self._key(item), item]
    def __delitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self.hash_bytes], 'big')
        assert self.array[idx] != self._sentinel
        self.array[idx] = self._sentinel
