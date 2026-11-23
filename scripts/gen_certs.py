"""Tạo CA riêng của nhóm và chứng chỉ cho các node (dùng cho TLS_ENABLED=1).

python scripts/gen_certs.py              # tạo certs/ca.crt, certs/node.crt, certs/node.key
python scripts/gen_certs.py --out other  # thư mục khác

Cần lệnh `openssl` (có sẵn trong Git for Windows, Linux, macOS).
Chép cả thư mục certs/ sang mọi máy chạy tracker/peer. KHÔNG commit certs/ lên GitHub.
"""
import argparse
import os
import shutil
import subprocess
import tempfile

GIT_OPENSSL = [
    r"C:\Program Files\Git\usr\bin\openssl.exe",
    r"C:\Program Files\Git\mingw64\bin\openssl.exe",
]

LEAF_EXT = """basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid
subjectAltName=DNS:localhost,IP:127.0.0.1
"""


def find_openssl():
    found = shutil.which("openssl")
    if found:
        return found
    for path in GIT_OPENSSL:
        if os.path.isfile(path):
            return path
    raise SystemExit("openssl not found: install Git for Windows or OpenSSL")


def run(openssl, *args):
    subprocess.run([openssl, *args], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def generate(out_dir, days=365, cn="DS02 Group 7 CA"):
    openssl = find_openssl()
    os.makedirs(out_dir, exist_ok=True)
    ca_key, ca_crt = os.path.join(out_dir, "ca.key"), os.path.join(out_dir, "ca.crt")
    key, crt = os.path.join(out_dir, "node.key"), os.path.join(out_dir, "node.crt")
    run(openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key, "-out", ca_crt,
        "-days", str(days), "-subj", f"/CN={cn}",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    with tempfile.TemporaryDirectory() as tmp:
        csr, ext = os.path.join(tmp, "node.csr"), os.path.join(tmp, "ext.cnf")
        with open(ext, "w") as f:
            f.write(LEAF_EXT)
        run(openssl, "req", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", csr, "-subj", "/CN=ds02-node")
        run(openssl, "x509", "-req", "-in", csr, "-CA", ca_crt, "-CAkey", ca_key, "-CAcreateserial",
            "-out", crt, "-days", str(days), "-extfile", ext)
    return ca_crt, crt, key


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="certs")
    p.add_argument("--days", type=int, default=365)
    args = p.parse_args()
    ca, crt, key = generate(args.out, args.days)
    print(f"CA: {ca}\ncert: {crt}\nkey: {key}\n"
          f"Set TLS_ENABLED=1 and use https:// in TRACKER_URL. Copy {args.out}/ to every machine.")


if __name__ == "__main__":
    main()
