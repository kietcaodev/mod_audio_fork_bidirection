#!/bin/bash
# Chạy script này trên máy OFFLINE (Debian 12)
# Yêu cầu: file mod_audio_fork_deps.tar.gz phải ở cùng thư mục với script này

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPS_FILE="$SCRIPT_DIR/mod_audio_fork_deps.tar.gz"

echo "=== Kiểm tra file deps ==="
if [ ! -f "$DEPS_FILE" ]; then
  echo "❌ Không tìm thấy file: $DEPS_FILE"
  echo "   Hãy copy mod_audio_fork_deps.tar.gz vào cùng thư mục với script này."
  exit 1
fi

echo "=== Giải nén packages ==="
cd /tmp
tar xzf "$DEPS_FILE"

echo "=== Cài đặt packages ==="
cd /tmp/mod_audio_fork_deps

# Cài lần 1
dpkg -i *.deb 2>/dev/null || true
# Cài lần 2 để xử lý dependency thứ tự
dpkg -i *.deb 2>/dev/null || true

echo "=== Kiểm tra freeswitch.pc ==="
# Tìm freeswitch.pc
FS_PC=$(find /usr -name "freeswitch.pc" 2>/dev/null | head -1)

if [ -z "$FS_PC" ]; then
  echo "❌ Không tìm thấy freeswitch.pc"
  echo "   FreeSWITCH chưa được build hoặc file .pc chưa được tạo."
  echo "   Tìm thủ công: find / -name 'freeswitch.pc' 2>/dev/null"
  exit 1
fi

echo "✅ Tìm thấy: $FS_PC"

# Kiểm tra modulesdir trong .pc file
MOD_DIR=$(grep "^modulesdir" "$FS_PC" | cut -d= -f2)
echo "   modulesdir hiện tại: $MOD_DIR"

# Nếu modulesdir không phải /usr/lib/freeswitch/mod thì sửa
if [ "$MOD_DIR" != "/usr/lib/freeswitch/mod" ]; then
  echo "⚠️  Sửa modulesdir → /usr/lib/freeswitch/mod"
  sed -i 's|^modulesdir=.*|modulesdir=/usr/lib/freeswitch/mod|' "$FS_PC"
fi

# Set PKG_CONFIG_PATH
FS_PC_DIR="$(dirname "$FS_PC")"
export PKG_CONFIG_PATH="$FS_PC_DIR:$PKG_CONFIG_PATH"
echo "   PKG_CONFIG_PATH=$PKG_CONFIG_PATH"

echo "=== Build module ==="
cd "$SCRIPT_DIR"
rm -rf build
mkdir -p build && cd build

cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
make install

echo ""
echo "=== Kiểm tra kết quả ==="
ls -la /usr/lib/freeswitch/mod/mod_audio_fork.so && echo "✅ Build thành công!" || echo "❌ Không tìm thấy file .so"
