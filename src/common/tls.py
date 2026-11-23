"""Mã hóa đường truyền bằng TLS hai chiều (mutual TLS).

- Nhóm tự tạo một CA riêng (scripts/gen_certs.py); mọi node dùng chứng chỉ do CA này ký.
- Cả hai phía đều phải trình chứng chỉ hợp lệ của CA nhóm -> máy lạ không bắt tay được,
  kẻ nghe lén trên mạng không đọc được chunk, token hay JOIN_KEY.
- Không kiểm tra hostname vì peer được gọi bằng IP thay đổi; danh tính từng peer vẫn được
  xác thực ở tầng ứng dụng bằng token do tracker ký.
"""
import ssl


def _check_files(settings):
    for path in (settings.tls_ca, settings.tls_cert, settings.tls_key):
        try:
            open(path, "rb").close()
        except OSError:
            raise FileNotFoundError(f"TLS file not found: {path} (run: python scripts/gen_certs.py)") from None


def server_context(settings):
    """Context cho phía nhận kết nối (uploader, tracker). None nếu tắt TLS."""
    if not settings.tls_enabled:
        return None
    _check_files(settings)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(settings.tls_cert, settings.tls_key)
    ctx.load_verify_locations(settings.tls_ca)
    ctx.verify_mode = ssl.CERT_REQUIRED  # bắt buộc phía kia cũng có chứng chỉ của CA nhóm
    return ctx


def client_context(settings):
    """Context cho phía chủ động kết nối (downloader, tracker client). None nếu tắt TLS."""
    if not settings.tls_enabled:
        return None
    _check_files(settings)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(settings.tls_ca)
    ctx.load_cert_chain(settings.tls_cert, settings.tls_key)
    return ctx
