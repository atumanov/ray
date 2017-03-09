#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/atumanov/arrow.git "$TP_DIR/arrow"
fi
cd "$TP_DIR/arrow"
git checkout parallel-arrow-memcpy
#git checkout c88bd70c13cf16c07b840623cb466aa98d535be0
