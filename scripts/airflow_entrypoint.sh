#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py

airflow connections add 'postgres_ops' \
    --conn-type 'postgres' \
    --conn-login $POSTGRES_USER \
    --conn-password $POSTGRES_PASSWORD \
    --conn-host 'host.docker.internal' \
    --conn-port $POSTGRES_PORT \
    --conn-schema $POSTGRES_DB
airflow connections add 'postgres_dw' \
    --conn-type 'postgres' \
    --conn-login $DW_POSTGRES_USER \
    --conn-password $DW_POSTGRES_PASSWORD \
    --conn-host 'host.docker.internal' \
    --conn-port $DW_POSTGRES_PORT \
    --conn-schema $DW_POSTGRES_DB
airflow webserver