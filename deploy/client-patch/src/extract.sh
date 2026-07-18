#!/usr/bin/env bash

set -e

BASE_DIR=$(pwd)
WORK_DIR=$BASE_DIR/temp

VERSION=$(cat "$BASE_DIR/assets/.version")
FIRMWARE_PATH=$(find "$BASE_DIR/assets" -maxdepth 1 -type f -name "mico_all_*_${VERSION}.bin" -print -quit)
FIRMWARE=$(basename "$FIRMWARE_PATH" .bin)

if [ -z "$FIRMWARE_PATH" ] || [ ! -f "$FIRMWARE_PATH" ]; then
    echo "❌ 固件文件不存在，请先下载固件到：$BASE_DIR/assets/"
    exit 1
fi

rm -rf "$WORK_DIR" && mkdir -pv "$WORK_DIR" && cd $WORK_DIR

python3 $BASE_DIR/src/extract.py -e "$FIRMWARE_PATH" -d "$WORK_DIR/$FIRMWARE"

ln -sf $WORK_DIR/$FIRMWARE/root.squashfs $WORK_DIR/root.squashfs 

unsquashfs $WORK_DIR/root.squashfs
