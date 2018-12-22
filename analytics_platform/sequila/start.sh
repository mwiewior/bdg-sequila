#!/usr/bin/env bash -x
D_UID=${UID} D_GID=$(id -g) D_SEQUILA_VERSION=$(grep version ../../build.sbt | cut -f2 -d'=' | sed 's/ //g' | sed 's/"//g') D_METASTORE_VERSION=1.2.x D_SEQUILA_MASTER=$1 D_SEQUILA_DRIVER_MEM=$2 D_DATA=$3 docker-compose up -d
