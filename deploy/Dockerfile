FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY src ./src
COPY scripts ./scripts
# Không dùng thư viện ngoài: chỉ cần Python chuẩn
RUN useradd -m app && mkdir -p /data /logs /samples && chown -R app /data /logs /samples
USER app
