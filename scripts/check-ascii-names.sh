#!/usr/bin/env bash
# Product names are written in ASCII (Mimir, Tyr, Vor). The original spelling may appear only in the
# "Mimir (Mímir)" form that marks a name's first use in a document. Fixtures keep their bytes and are not checked.
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(dirname "$0")/.."
if git grep -n -P '(?<!Mimir \()M\x{ed}mir|(?<!Tyr \()T\x{fd}r|(?<!Vor \()V\x{f6}r|(?<!Mjolnir \()Mj\x{f6}lnir' -- . ':(exclude)fixtures/'; then
  echo "Write product names in ASCII: Mimir, Tyr, Vor, Mjolnir (see the lines above)." >&2
  exit 1
fi
echo "product names: ASCII only"
