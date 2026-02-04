#!/bin/bash

set -e
set -x

OS_VER=($(uname -sm))

case "${OS_VER[0]}" in
    Linux)
        OS_FAMILY=linux
        . /etc/os-release
        ID_LIKE="${ID_LIKE:-${ID}}"
        OS_NAME="${ID}-${VERSION_ID}"
        ;;
    Darwin)
        OS_FAMILY=darwin
        MACOSX_DEPLOYMENT_TARGET=12.0
        OS_NAME="macos-${MACOSX_DEPLOYMENT_TARGET}"
        export MACOSX_DEPLOYMENT_TARGET
        ;;
    *)
        echo "Unsupported OS: ${OS_VER[0]}"
        exit 1
        ;;
esac

case "${OS_VER[1]}" in
    x86_64)
        OS_ARCH=amd64
        ;;
    aarch64)
        OS_ARCH=arm64
        ;;
    arm64)
        OS_ARCH=arm64
        ;;
    *)
        echo "Unsupported arch: ${OS_VER[1]}"
        exit 1
        ;;
esac

case $ID_LIKE in
    *debian*)
        apt-get update && apt-get -y install curl git gcc make cmake
        ;;
    *fedora*)
        yum install -y curl git gcc make cmake
        ;;    
esac

APP_SUFFIX=".${OS_NAME}-${OS_ARCH}"

APP_VERSION=${APP_VERSION:-${GITHUB_REF_NAME}}
APP_VERSION=${APP_VERSION:-${CI_COMMIT_REF_NAME}}
APP_VERSION=${APP_VERSION:-$(git describe --tags||:)}
APP_VERSION=${APP_VERSION:-dev}

ROOTDIR="${PWD}"
SRCDIR="${ROOTDIR}/src"
DESTDIR="${ROOTDIR}/build"
DISTDIR="${ROOTDIR}/dist"


mkdir -p "${SRCDIR}" "${DESTDIR}" "${DISTDIR}"
export DESTDIR

if ! which go; then
    GO_DEV_VER=($(curl -fs https://go.dev/VERSION?m=text))
    curl -fsLJ "https://go.dev/dl/${GO_DEV_VER[0]}.${OS_FAMILY}-${OS_ARCH}.tar.gz" | tar -xzC "${DESTDIR}"
    PATH="${DESTDIR}/go/bin:${PATH}"
    export PATH
fi

git clone --depth 1 https://github.com/ibireme/yyjson.git "${SRCDIR}/yyjson"
mkdir -p "${SRCDIR}/yyjson.build"
cmake -B "${SRCDIR}/yyjson.build" -S "${SRCDIR}/yyjson"
make -C "${SRCDIR}/yyjson.build" install

#git clone https://luajit.org/git/luajit.git "${SRCDIR}/luajit"
git clone --depth 1 https://github.com/LuaJIT/LuaJIT.git "${SRCDIR}/luajit"
make -C "${SRCDIR}/luajit" install

export CGO_CFLAGS="-I${DESTDIR}/usr/local/include -I${DESTDIR}/usr/local/include/luajit-2.1"
export CGO_LDFLAGS="${DESTDIR}/usr/local/lib/libyyjson.a ${DESTDIR}/usr/local/lib/libluajit-5.1.a -lm"
go build -trimpath -ldflags="-s -w -X 'main.Version=${APP_VERSION}'" -o ${DISTDIR}/ktt${APP_SUFFIX} ./cmd/ktt
