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
    def __init__(self, itemsize, key, id=b'', rep=None):
        self._itemsize = itemsize
        self._key = key
        self.array = FixedArray(self._itemsize, id, rep)
        self._rep = self.array.doc.rep
        self._capacity = len(self.array)
        self._sentinel = bytes(self._itemsize)
        if self._capacity > 0:
            self._hashbits = (self._capacity-1).bit_length()
            assert 1<<(self._hashbits) == self._capacity
        else:
            self._hashbits = 0
        self._hashbytes = (self._hashbits+7) >> 3
        self._hashshift = (self._hashbytes << 3) - self._hashbits
    @property
    def id(self):
        return self.array.id
    def __len__(self):
        raise NotImplementedError('len(FixedDict)')
    def __getitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
        return self.array[idx]
    def __setitem__(self, keyhash, item):
        self.update({keyhash:item})
    #def __setitem__(self, keyhash, item):
    #    assert self._key(item) == keyhash # interestingly this is the same as a set
    #    if self._capacity == 0:
    #        self._capacity = 2
    #        self._hashbytes = 1
    #        self._hashbits = 1
    #        self._hashshift = 7
    #        self.array[:] = [self._sentinel, self._sentinel]
    #    assert item != self._sentinel
    #    idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
    #    place = self.array[idx]
    #    if place != self._sentinel:
    #        collision = self._key(place)
    #        if collision != keyhash:
    #            assert idx == int.from_bytes(collision[:self._hashbytes], 'big') >> self._hashshift
    #            spread = 0
    #            while True:
    #                spread += 1
    #                hashbits = self._hashbits + spread
    #                expansion = 1 << spread
    #                hashbytes = (hashbits+7) >> 3
    #                hashshift = (hashbytes << 3) - hashbits
    #                idx = int.from_bytes(keyhash[:hashbytes], 'big') >> hashshift
    #                if idx != int.from_bytes(collision[:hashbytes], 'big') >> hashshift:
    #                    break
    #            capacity = self._capacity * expansion
    #            assert 1 << hashbits == capacity
    #            expansionmask = expansion - 1
    #            def content_generator():
    #                for superidx, item in enumerate(tqdm.tqdm(self.array, desc='growing hashtable', leave=False)):
    #                    if item == self._sentinel:
    #                        yield from [self._sentinel] * expansion
    #                    else:
    #                        keyhash = self._key(item)
    #                        wholeidx = int.from_bytes(keyhash[:hashbytes], 'big')
    #                        assert superidx == wholeidx >> (hashbytes * 8 - self._hashbits)
    #                        subidx = (wholeidx >> hashshift) & expansionmask
    #                        assert superidx * expansion + subidx == wholeidx >> hashshift
    #                        yield from [self._sentinel] * subidx
    #                        yield item
    #                        yield from [self._sentinel] * (expansion - subidx - 1)
    #            self.array[:] = IterableWithLength(content_generator(), self._capacity * expansion)
    #            self._capacity = capacity
    #            self._hashbits = hashbits
    #            self._hashbytes = hashbytes
    #            self._hashshift = hashshift
    #            dict(self.items()) # fsckish
    #    self.array[idx] = item
    def update(self, keyhashitems):
        updates = {}
        expansion = 1
        hashshift = self._hashshift
        hashbytes = self._hashbytes
        hashbits = self._hashbits
        hashshift = self._hashshift
        capacity = max(self._capacity, 1)

        # i believe some of the storage representation here could be optimized away if that helps anything
        # was thinking i hadnt fully thought about storage updates as dicts vs list and its members
            # it looks like a more optimized data structure could be a dict of superidx (original idx)
            # containing sorted lists of either final idx==newidx or subidx

        for keyhash, item in keyhashitems:
            assert item != self._sentinel
            byteidx = int.from_bytes(keyhash[:hashbytes], 'big')
            newidx = byteidx >> hashshift
            if self._capacity > 0:
                # this block checks for collision with previous stored values
                if capacity > self._capacity:
                    superidx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
                else:
                    superidx = newidx
                place = self.array[superidx]
                if place != self._sentinel:
                    collision = self._key(place)
                    if collision != keyhash:
                        assert superidx == int.from_bytes(collision[:self._hashbytes], 'big') >> self._hashshift
                        updates[newidx] = [collision, place, False]
            # this separated approach to checking for collisions allows for accepting
            # batched data that ends up containing hash collisions solely within itself
            placing = updates.get(newidx)
            if placing is not None:
                collision, place, is_new = placing
                while newidx == int.from_bytes(collision[:hashbytes], 'big') >> hashshift:
                    capacity <<= 1
                    expansion <<= 1
                    #spread += 1
                    #hashbits = self._hashbits + spread
                    hashbits += 1
                    hashbytes = (hashbits+7) >> 3
                    hashshift = (hashbytes << 3) - hashbits
                    byteidx = int.from_bytes(keyhash[:hashbytes], 'big')
                    newidx = byteidx >> hashshift
                    assert capacity == (1 << hashbits)
                new_updates = {}
                for _keyhash, _item, _is_new in updates.values():
                    if _is_new:
                        newnewidx = int.from_bytes(_keyhash[:hashbytes], 'big') >> hashshift
                        assert newnewidx not in new_updates
                        new_updates[newnewidx] = [_keyhash, _item, True]
                updates = new_updates
            assert newidx not in updates
            assert int.from_bytes(keyhash[:hashbytes], 'big') >> hashshift == newidx
            updates[newidx] = [keyhash, item, True]
        updates = [[newidx, keyhash, item] for newidx, [keyhash, item, is_new] in updates.items() if is_new]
        updates.sort(reverse=True)

        if capacity == self._capacity: # no expansion, newidx == idx == superidx
            allocsz = self._rep.manager.allocsize
            itemsz = self._itemsize
            update_chunks = [[updates.pop()]]
            while len(updates):
                update = updates.pop()
                if (update[0] + 1 - update_chunks[-1][-1][0]) * itemsz >= allocsize:
                    update_chunks.append([update])
                else:
                    update_chunks[-1].append(update)
            for update_chunk in update_chunks:
                if len(update_chunk) == 1:
                    idx, keyhash, item = update_chunk[0]
                    self.array[idx] = item
                else:
                    min_idx, min_keyhash, min_item = update_chunk[0]
                    max_idx, max_keyhash, max_item = update_chunk[-1]
                    content = [min_item] + self.array[min_idx+1:max_idx] + [max_item]
                    for idx, item in update_chunk[1:-1]:
                        content[idx-min_idx] = item
                    self.array[min_idx:max_idx+1] = content
                update_chunk[:] = []
        else:
            # big-endian expand with sentinels, write entire array larger to spread zeros between items
            def content_generator():
                next_newidx, next_keyhash, next_item = updates.pop() if len(updates) else [1<<hashbits,None,None]
                next_superidx = next_newidx >> (hashbits - self._hashbits)
                expansionmask = expansion - 1
                def newidx2subidx(newidx):
                    assert superidx == newidx >> (hashbits - self._hashbits)
                    #subidx = (newidx >> hashshift) & expansionmask
                    subidx = newidx & expansionmask
                    assert superidx * expansion + subidx == newidx# >> hashshift
                    return subidx
                for superidx, item in enumerate(tqdm.tqdm(self.array if self._capacity else [self._sentinel], desc='growing sentinel hashtable', leave=False)):
                    update_chunk = [self._sentinel] * expansion
                    if item != self._sentinel:
                        keyhash = self._key(item)
                        newidx = int.from_bytes(keyhash[:hashbytes], 'big') >> hashshift
                        update_chunk[newidx2subidx(newidx)] = item
                    dbg_additions = []
                    while next_superidx == superidx:
                        item = next_item
                        newidx = next_newidx
                        dbg_additions.append([next_newidx, next_keyhash, next_item])
                        assert int.from_bytes(self._key(next_item)[:hashbytes], 'big') >> hashshift == newidx
                        update_chunk[newidx2subidx(newidx)] = item
                        next_newidx, next_keyhash, next_item = updates.pop() if len(updates) else [1<<hashbits,None,None]
                        next_superidx = next_newidx >> (hashbits - self._hashbits)
                    for subidx, item in enumerate(update_chunk):
                        if item != self._sentinel:
                            dbg_keyhash = self._key(item)
                            assert superidx * expansion + subidx == int.from_bytes(dbg_keyhash[:hashbytes], 'big') >> hashshift
                    yield from update_chunk
                    del update_chunk

            assert capacity == 1 << hashbits
            assert hashbytes * 8 >= hashbits
            assert hashshift == hashbytes * 8 - hashbits
            self.array[:] = IterableWithLength(content_generator(), capacity)
            self._capacity = capacity
            self._hashbits = hashbits
            self._hashbytes = hashbytes
            self._hashshift = hashshift
            dict(self.items()) # fsckish

#        # below legacy draft
#
#            # this is sparse placement
#            # whether it's implemented here or in a multiupdate function
#            # at a lower level, basically what will happen is fetching the
#            # record, changing the values that are in it, and placing it back
#
#            # now, the current implementation places an update for each id.
#            # but actually we only need an update for each allocsize !
#                # a maximal update could be allocsize/itemsize large,
#                # extending from either end of an existing id.
#                # but mostly is only useful if it covers ids fully.
#
#            # another option is to rewrite the entire range of updates.
#            
#            # a reasonable simplification would be to rewrite in sequences that have gaps no larger than allocsize/itemsize.
#
#            def content_generator(sorted_id_updates):
#                for idx in range(len(sorted_id_updates)-1):
#                    yield sorted_id_updates[idx]
#                    yield array
#            for id, id_updates in updates.items():
#                if len(id_updates) == 1:
#                    idx, item = id_updates[0]
#                    self.array[idx] = item
#                else:
#                    id_updates.sort()
#                    min_idx, min_item = id_updates[0]
#                    max_idx, max_item = id_updates[-1]
#                    content = [min_item] + self.array[min_idx+1:max_idx] + [max_item]
#                    for idx, item in id_updates[1:-1]:
#                        content[idx-min_idx] = item
#                    self.array[min_idx:max_idx+1] = content
#                
#                content = FixedArray(self._itemsize, id, rep=self._rep)
#                items = [*content]
#                for idx, item in id_updates:
#                    items[idx] = item
#
#                # i've shrunk the task to make easier and more completable.
#
#        # the plan is to calculate all the keyhashes in advance
#        # sending the changes well is helped if there's a way to group indices into their underlying records
#        total_assignments = len(dict) + len(kwvalues)
#        if self._capacity == 0:
#            self._capacity = 2
#            self._hashbytes = 1
#            self._hashbits = 1
#            self._hashshift = 7
#            self.array[:] = [self._sentinel, self._sentinel]
    def __iter__(self):
        for item in self.array:
            if item != self._sentinel:
                yield [self._key(item), item]
    def __delitem__(self, keyhash):
        idx = int.from_bytes(keyhash[:self._hashbytes], 'big') >> self._hashshift
        assert self.array[idx] != self._sentinel
        self.array[idx] = self._sentinel

class Dict(FixedDict):
    def __init__(self, id=b'', rep=None):
        super().__init__(rep.manager.idsize*2, self._key, id, rep)
        self._alloc = self._rep.manager.alloc
        self._fetch = self._rep.manager.fetch
        self._idsize = self._rep.manager.idsize
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
        self.update([key, val])
    def update(self, keyitemseq = {}, **keyitemkws):
        # when/if parallel allocs are used, this function would need to be reorganized to use them.
        alloc = self._alloc
        try:
            keyitemseq = keyitemseq.items()
        except AttributeError:
            pass
        keyitemkws = ([key.encode(), item] for key, item in keyitemkws.items())
        keyitems = (keyitem for keyitems in [keyitemseq, keyitemkws] for keyitem in keyitems)
        if self._capacity > 0:
            super_ = super()
            def keyhashitems():
                for key, item in keyitems:
                    keyhash = hash(key)
                    # if the item is present already, then only the second half of the key need be updated
                    storedkeyval = super_.__getitem__(keyhash)
                    if storedkeyval != self._sentinel:
                        storedkeyid = storedkeyval[:self._idsize]
                        storedkey = self._fetch(storedkeyid)
                        if storedkey == key:
                            yield [keyhash, storedkeyid + alloc(val)]
                            continue
                        assert keyhash != hash(storedkey)
                    yield [keyhash, alloc(key) + alloc(val)]
            super().update(keyhashitems())
        else:
            super().update([
                [hash(key), alloc(key) + alloc(val)]
                for key, item in keyitems
            ])
            
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
    from .i import fI as manager
    doc = Dict(rep=Rep(manager=manager()))
    cmp = {}
    for x in tqdm.tqdm(range(17)):
        val = str(x).encode()
        doc.update([[val,val]])
        #doc[val] = val
        cmp[val] = val
        assert dict(doc.items()) == cmp
