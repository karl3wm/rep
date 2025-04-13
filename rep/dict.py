from .rep import Rep
from .array import FixedArray
import collections

from cryptography.hazmat.primitives.hashes import Hash as H, SHA256 as DIGEST
DIGEST = DIGEST()

class FixedDict(collections.abc.MutableMapping):
    def __init__(self, itemsize, id=b'', rep=Rep()):
        self._itemsize = itemsize
        self.array = FixedArray(self._itemsize, id, rep)
        self._capacity = len(self.array)
    def __len__(self):
        raise NotImplementedError('len(FixedDict)')
    def __getitem__(self, keyhash):
        # each has maps to a line to find the key in.
            # given challengeish,
            # let's sort out hash collisions ...
            # we have only len() bits of hash material so collisions are likely i suspect
            # like if you only have 2 entries, there is a high chance they both land in same spot.
            # then let's sort out impact of workaround.
                # i guess we'd use a bisection
                # using the hash location as a guess
                # and insert it
                # such that everything is sorted based on hash
                # thinking of improvements a little
                # header data would help a lot, then length and bucket count could be separated
                # we could also use a normalized bucket count making the length calculable from the bucket count ... maybe
                # given the backend supports inserts it could be reasonable
                    # inserts seem supported. this could be made more efficient by providing for partial references in the ids.
            # if there were a 'normalized bucket count' ... then how still would collisions be handled
            # but i see with guaranteed sparsity that a bisect search is much faster.
            # if we had 4x sparsity then basically each bucket would have 4 entries

            # if there were not sparsity, though, then density would accumulate randomly and it would basically be a tree rather than a table
                    # i think in one of the prior implementations empty spots used their data to link to relevent information ...
            # it helps to have metadata stored.

                        # we could maintain metadata locally.
                        # if it stores a sorted list, then we could store the bisection information to do lookups quickly.
                        # we could use a local dict or such ... or information to a given depth ...
            # the nice thing about a real hashmap is you only need 1 lookup
            # i think past hashmaps used sparsity to provide this -- for example the fixed table size with nested tables
            # the allocator does provide its allocation size. one could make a fixed-size table.
                # given the allocation size is known it could make sense to make a table that fills it.
                # it leaves the size uncertain though
                # but maybe the size is not that important
                        # we could also use an id as a key hash
        # this structure should take the hashed key

            # so another approach was to use one large table that filled what is basically a page
            # and have different entries depending on whether a further table is needed, or a value is available.
            # this used likely a 1-byte marker indicating that state
            # it could even be 1-bit if there's a way to squeeze it in with something else
            # it could say unoccupied, immediate data, deferred data, or subtable
            # here it might say uhh just unoccupied or immediate data or subtable
            # another option more in line with other files would be a list of tables.
            # keeping them full table size.
                    # that might be doable with a fixedarray where the itemsize is the allocsize
            # if we did a table list with sparsity it would be analogous to earlier thoughts, basically the same
            # one of the concerns is how to represent an empty spot without a marker, and it could be all 0s maybe unsure
            # or the nature of the dictish could be to return None for every key
            # FixedDict should be a useful structure to use to implement a full-like dict i suppose
                # so on hash collision one would grow the buckets, in the super-simple view
                # and the length would be unavailable. it would be an estimate, like bucketsize // 2
                # collisions could decide when the table is grown.

                # now this approach, unlike the treeish approach, has an upper bound on the amount of data it can store.
                # basically all the keyhashes fit in ram (or on-disk)
                # maybe it can work, unsure
                    # it's bigger than one thinks but yeah

        # so some interest in an approach that expands on collisions.
        # this means every fetch either finds zeros or a single value, because collisions always make sparsity

        hash = H(DIGEST); hash.update(key); hash = hash.finalize()[:self._hashsize]

        hash = int.from_bytes(hash,'little') % 
        

