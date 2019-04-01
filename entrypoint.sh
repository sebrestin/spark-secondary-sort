#!/usr/bin/env bash

export SPARK_NO_DAEMONIZE=true

${SPARK_HOME}/sbin/start-history-server.sh
