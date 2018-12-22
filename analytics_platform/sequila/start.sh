#!/usr/bin/env bash -x
D_UID=${UID} D_GID=${GROUPS} D_SEQUILA_VERSION=0.5.2 D_METASTORE_VERSION=1.2.x D_SEQUILA_MASTER=local[2] D_SEQUILA_DRIVER_MEM=2g  docker-compose up -d
