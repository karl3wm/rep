from base64 import _urlsafe_encode_translation
from binascii import b2a_base64
from threading import current_thread
import requests

class aR:
    def __init__(self, *params, **kwparams):
        from ar import Peer as R, Wallet as T, DataItem as M, PUBLIC_GATEWAYS as C
        from bundlr.node import DEFAULT_API_URL as L, DEFAULT_SUBSIDY_MAX_BYTES as S, Node as E
        from toys.accelerated_ditem_signing import AcceleratedSigner as D, AR_DIGEST as _
        from cryptography.hazmat.primitives.hashes import Hash as H

        self._e = E()
        #self._r = R()
        self._c = C
        self.__ = requests.Session()

        try:
            t = T('aR.w')
        except:
            t = T.generate(4096,'aR.w')
        _d = D(M(data=b'', *params, **kwparams), t.rsa)
        self._d = _d
        self._ds = { current_thread(): _d }

        s0, s1 = _d.signature_range()
        def id(data):
            h = H(_)
            h.update(data[s0:s1])
            return h.finalize()
        self._id = id

        _dh = _d.header(b'')
        self.allocsize = S - len(_dh)
        self.idsize = len(id(_dh))

    def alloc(self, data):
        th = current_thread()
        _ds = self._ds
        _d = _ds.get(th)
        if _d is None:
            _d = self._d.clone()
            _ds[th] = _d
        encoded = _d.header(data) + data
        res = self._e.send_tx(encoded)
        return self._id(encoded)

    def fetch(self, id):
        id_str = b2a_base64(id, newline=False).rstrip(b'=').translate(_urlsafe_encode_translation)
        return self.__.get(self._c[0] + '/raw/' + id_str.decode()).content
        #return self._r._request('raw', id_str.decode(), method='GET').content

    def fetch_size(self, id):
        id_str = b2a_base64(id, newline=False).rstrip(b'=').translate(_urlsafe_encode_translation)
        return int(self.__.head(self._c[0] + '/raw/' + id_str.decode()).headers['Content-Length'])
        #return int(self._r._request('raw', id_str.decode(), method='HEAD').headers['Content-Length'])
