#!/usr/bin/env bash

## Script information
# author: Florian Hucke, florian(dot)hucke(at)hhu(dot)de
# date: 02. Feb 2018
# version: 0.1
# license: GPL V3

LIB_DIR=$PWD/lib
BIN_DIR=$PWD/bin
CONF_DIR=$PWD/config


BINARY=$BIN_DIR/dxram-memory-develop.jar
if [ ! -e $BINARY ]; then
    echo "No jar found at $BINARY"
    echo "run the script: \"build_dxram_memory\""
    exit 1
fi

LIBS=$LIB_DIR/slf4j-api-1.6.1.jar:$LIB_DIR/gson-2.7.jar:$LIB_DIR/log4j-api-2.7.jar:$LIB_DIR/log4j-core-2.7.jar:$LIB_DIR/jline-2.15.jar

CLASSPATH=$LIBS:$BINARY

MAIN_CLASS=de.hhu.bsinfo.DXRAMMemory

java \
    -Dlog4j.configurationFile=$CONF_DIR/log4j.xml \
    -Ddxram.config=$CONF_DIR/dxram.json \
    -cp $CLASSPATH $MAIN_CLASS \
    -ea \
    -jar $BINARY #> /dev/null 2>&1

