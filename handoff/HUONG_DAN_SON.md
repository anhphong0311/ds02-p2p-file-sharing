# Phần việc của Nguyễn Thanh Sơn

Phụ trách uploader, metadata, CLI, Docker, script vận hành/benchmark và mutual TLS.

## File trong gói

- `src/peer/uploader.py`
- `src/peer/main.py`
- `src/common/metadata.py`
- `src/common/tls.py`
- `deploy/`
- `scripts/`
- `tests/test_metadata.py`
- `tests/test_tls.py`

## Cách commit

```powershell
git switch main
git pull
git switch -c feature/son-uploader-deploy
git config user.name "Nguyễn Thanh Sơn"
git config user.email "EMAIL_GITHUB_CUA_SON"
git add src/peer/uploader.py src/peer/main.py src/common/metadata.py src/common/tls.py deploy scripts tests/test_metadata.py tests/test_tls.py
git commit -m "Thêm uploader, metadata, CLI, Docker và TLS"
git push -u origin feature/son-uploader-deploy
```

Sau đó tạo Pull Request vào `main`. Không commit thư mục `certs/`, khóa riêng hoặc file `.env`. Trước khi commit, cần đọc, chạy test và xác nhận đây đúng là phần mình phụ trách.
