#!/bin/bash

SEASTAR_DIR="~/fun/cpp_projects/seastar"

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=gold \
      -DCMAKE_MODULE_LINKER_FLAGS=-fuse-ld=gold \
      -DCMAKE_SHARED_LINKER_FLAGS=-fuse-ld=gold \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
      -DCMAKE_PREFIX_PATH="$SEASTAR_DIR/build/release/_cooking/installed" \
      -DCMAKE_MODULE_PATH="$SEASTAR_DIR/cmake" \
      ..

      # -DCMAKE_C_COMPILER=clang \
      # -DCMAKE_CXX_COMPILER=clang++ \
      # -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld \
      # -DCMAKE_MODULE_LINKER_FLAGS=-fuse-ld=lld \
      # -DCMAKE_SHARED_LINKER_FLAGS=-fuse-ld=lld \
