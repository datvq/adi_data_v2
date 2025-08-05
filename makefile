SHELL := /bin/bash

include ./.env

include ./docker/${AIRFLOW_OS_SYSTEM}/${AIRFLOW_EXECUTOR}/makefile

PHONY: run
run::
	@python ./app/main.py model_sales
