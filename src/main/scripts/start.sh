#!/bin/bash

source /etc/profile

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
$SCRIPT_DIR/bin/gremlin-cer &>> $GREMLIN_LOG_FOLDER/indexer.log &
