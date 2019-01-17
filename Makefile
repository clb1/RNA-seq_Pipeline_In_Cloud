SHELL = /bin/bash

# This is the Makefile to use for running pipeline stages in a fault tolerant way on Google Cloud.

ROOT_DIR = /projects/GCE
SCRIPTS = ${ROOT_DIR}/scripts

CONFIG_FILE = GCE.config

.PHONY: run_%


# % is a pipeline stage (e.g. "trim-fastq")
run_%:
    ${SCRIPTS}/gceRunPipelineStage.py $* ${CONFIG_FILE}
