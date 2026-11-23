"""Tạo file ngẫu nhiên để thử nghiệm.

python scripts/gen_file.py samples/test-20mb.bin --mb 20
"""
import argparse
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--mb", type=float, default=10)
    p.add_argument("--seed", type=int, default=None, help="cố định nội dung (để các seeder có file giống nhau)")
    args = p.parse_args()
    size = int(args.mb * 1024 * 1024)
    os.makedirs(os.path.dirname(os.path.abspath(args.path)), exist_ok=True)
    if args.seed is not None:
        import random
        rnd = random.Random(args.seed)
        data = rnd.randbytes(size)
    else:
        data = os.urandom(size)
    with open(args.path, "wb") as f:
        f.write(data)
    print(f"wrote {size} bytes -> {args.path}")


if __name__ == "__main__":
    main()
