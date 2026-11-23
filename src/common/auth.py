"""Token xác thực peer: "<peer_id>.<expiry>.<hmac-sha256>" do tracker ký bằng TRACKER_SECRET.

Peer không biết TRACKER_SECRET nên không thể tự tạo token cho Peer ID khác.
"""
import hashlib
import hmac
import re
import time

PEER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def valid_peer_id(peer_id):
    return isinstance(peer_id, str) and bool(PEER_ID_RE.match(peer_id))


def _sign(secret, msg):
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


def issue_token(secret, peer_id, ttl):
    msg = f"{peer_id}.{int(time.time()) + int(ttl)}"
    return f"{msg}.{_sign(secret, msg)}"


def verify_token(secret, token):
    """Trả về peer_id nếu token hợp lệ và còn hạn, ngược lại None."""
    if not isinstance(token, str) or token.count(".") != 2:
        return None
    peer_id, exp, sig = token.split(".")
    if not valid_peer_id(peer_id) or not exp.isdigit():
        return None
    if not hmac.compare_digest(sig, _sign(secret, f"{peer_id}.{exp}")):
        return None
    if int(exp) < time.time():
        return None
    return peer_id


def hash_secret(value):
    return hashlib.sha256(value.encode()).digest()


def check_secret(value, expected_hash):
    if not isinstance(value, str):
        return False
    return hmac.compare_digest(hash_secret(value), expected_hash)
