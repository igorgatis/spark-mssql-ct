#!/bin/bash

set -e

pushd "$( dirname "${BASH_SOURCE[0]}" )"

DEFAULT_CMD="sbt ~package"
CMD="$@"

sudo docker run \
  -it --rm \
  -v $HOME/.cache/coursier:/root/.cache/coursier \
  -v $PWD:/app \
  -w /app \
  mozilla/sbt \
  ${CMD:-$DEFAULT_CMD}
