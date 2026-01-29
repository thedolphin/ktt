#!/bin/bash

set -e
set -x

WORKDIR="${PWD}"
SRCDIR="${WORKDIR}/src"
DESTDIR="${WORKDIR}/dist"

mkdir -p "${DESTDIR}" "${SRCDIR}"
export DESTDIR
export MACOSX_DEPLOYMENT_TARGET=12.0

git clone https://github.com/ibireme/yyjson.git "${SRCDIR}/yyjson"
mkdir -p "${SRCDIR}/yyjson.build"
cmake -B "${SRCDIR}/yyjson.build" -S "${SRCDIR}/yyjson"
make -C "${SRCDIR}/yyjson.build" install

#git clone https://luajit.org/git/luajit.git "${SRCDIR}/luajit"
git clone https://github.com/LuaJIT/LuaJIT.git "${SRCDIR}/luajit"
make -C "${SRCDIR}/luajit" install

export CGO_CFLAGS="-I${DESTDIR}/usr/local/include -I${DESTDIR}/usr/local/include/luajit-2.1"
export CGO_LDFLAGS="${DESTDIR}/usr/local/lib/libyyjson.a ${DESTDIR}/usr/local/lib/libluajit-5.1.a"
go build -trimpath -ldflags="-s -w -extldflags='-Wl,-dead_strip'" -o ${DESTDIR}/ktt${1} ./cmd/ktt
