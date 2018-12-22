#!/usr/bin/env bash -x
D_UID=${UID} D_GID=${GROUPS} D_SEQUILA_VERSION=0.5.2 D_METASTORE_VERSION=1.2.x docker-compose up -d
