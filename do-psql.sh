#!/bin/bash

get_config() {
    awk -F '=' "/$1/ {print \$2}" master.local.cfg | tr -d ' '
}

HOST=$(get_config 'host')
DB=$(get_config 'db')
USER=$(get_config 'user')
PASSWORD=$(get_config 'password')

echo "Connecting to database '$DB' as $USER@$HOST..."

PGPASSWORD="$PASSWORD" exec psql -U "$USER" -h "$HOST" -d "$DB"
