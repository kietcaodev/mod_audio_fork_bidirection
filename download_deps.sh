#!/bin/bash
# Chạy script này trên máy CÓ INTERNET (Debian 12)
# Kết quả: file deps.tar.gz chứa toàn bộ .deb cần thiết

set -e

echo "=== Cập nhật apt cache ==="
apt-get update

echo "=== Tạo thư mục chứa packages ==="
DEST_DIR="/tmp/mod_audio_fork_deps"
rm -rf "$DEST_DIR"
mkdir -p "$DEST_DIR"
cd "$DEST_DIR"

echo "=== Download tất cả .deb packages ==="
# Không cần libfreeswitch-dev vì FreeSWITCH đã build từ source
apt-get download $(apt-cache depends --recurse --no-recommends --no-suggests \
  --no-conflicts --no-breaks --no-replaces --no-enhances \
  libssl-dev zlib1g-dev libspeexdsp-dev libwebsockets-dev \
  cmake make g++ pkg-config | grep "^\w" | sort -u) 2>/dev/null || true

# Download trực tiếp các package chính để đảm bảo có đủ
apt-get download \
  libssl-dev \
  zlib1g-dev \
  libspeexdsp-dev \
  libwebsockets-dev \
  cmake \
  make \
  g++ \
  pkg-config

echo "=== Đóng gói ==="
cd /tmp
tar czf mod_audio_fork_deps.tar.gz mod_audio_fork_deps/

echo ""
echo "✅ Xong! Copy file sau sang máy offline:"
echo "   /tmp/mod_audio_fork_deps.tar.gz"
echo ""
ls -lh /tmp/mod_audio_fork_deps.tar.gz
