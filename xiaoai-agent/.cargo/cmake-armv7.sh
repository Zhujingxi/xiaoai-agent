#!/bin/sh
set -eu

case "${1:-}" in
    --build|--install|--version|-E)
        exec cmake "$@"
        ;;
    *)
        exec cmake "$@" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DOPUS_DISABLE_INTRINSICS=ON
        ;;
esac
