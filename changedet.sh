#!/bin/bash

export SPARK_APPLICATION_ARGS="${APP_ARGS_HDFSDIR} ${APP_ARGS_MASTERIMG} ${APP_ARGS_SLAVEIMG} ${APP_ARGS_RESULT}"

sh /template.sh
