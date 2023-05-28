#!/bin/bash

sudo apt-get update
sudo apt-get install -y git python3-pip

curl -sSL https://install.python-poetry.org | python3 -

gsutil cp -r gs://storage_intermediate/sql-query-engine /home/

cd /home/sql-query-engine

cp -r ./data /data
hadoop fs -mkdir /data/
hadoop fs -put /data/* /data/

/root/.local/bin/poetry config virtualenvs.in-project true
/root/.local/bin/poetry install
