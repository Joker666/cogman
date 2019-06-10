#!/bin/sh
set -e

PROJ="cogman"
ORG_PATH="github.com/Tapfury"
REPO_PATH="${ORG_PATH}/${PROJ}"

if ! [ -x "$(command -v go)" ]; then
    echo "go is not installed"
    exit 1
fi
if ! [ -x "$(command -v git)" ]; then
    echo "git is not installed"
    exit 1
fi

if [ -z "${VERSION}" ]; then
    COMMIT=`git rev-parse --short HEAD`
    TAG=$(git describe --exact-match --abbrev=0 --tags ${COMMIT} 2> /dev/null || true)

    if [ -z "${TAG}" ]; then
        VERSION=${COMMIT}
    else
        VERSION=${TAG}
    fi
    if [ -n "$(git diff --shortstat 2> /dev/null | tail -n1)" ]; then
        VERSION="${VERSION}-dirty"
    fi
fi

go mod vendor
go install -v -ldflags="-X ${REPO_PATH}/version.Version=${VERSION}" ./cmd/...
