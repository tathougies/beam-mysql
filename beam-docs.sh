#!/bin/sh

set -e

CHINOOK_MYSQL_URL="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql"
EXPECTED_SHA256="23e0822a57bfe70a2538f78cd197f73e7f977116f5e838f8397598fd188fb5df"

HOST=$1
PORT=$2
USER=$3
PASSWORD=$4
DATABASE=$5

status "MYSQL HOST=${HOST} PORT=${PORT} USER=${USER}"

run_mysql () {
    if [[ ! -z $PASSWORD ]]; then
        PASSWORD_ARG="--password $PASSWORD"
    else
        PASSWORD_ARG=""
    fi

    mysql -h $HOST -P $PORT --user=$USER $PASSWORD_ARG --protocol=TCP -N "$@"
}

db_exists() {
    echo "SHOW DATABASES" | run_mysql | grep -Fx "$1" >/dev/null
}

print_open_statement() {
    echo "chinook <- connect defaultConnectInfo {"
    echo "              connectHost = \"$HOST\", connectPort = $PORT,"
    echo "              connectUser = \"$USER\", connectPassword = \"$PASSWORD\","
    echo "              connectDatabase = \"$DATABASE\","
    echo "              connectOptions = [ Base.CharsetName \"utf8\", Base.Protocol Base.TCP ] }"
    echo "autocommit chinook False -- Do not auto commit DDL statements"
}

if db_exists "$DATABASE"; then
    print_open_statement
    exit 0
fi

if [ ! -f chinook-data/Chinook_MySql.sql ]; then
    status "Downloading MySql chinook data ..."
    download "chinook-data/Chinook_MySql.sql.tmp" "$CHINOOK_MYSQL_URL" "$EXPECTED_SHA256"

    status "Converting file"
    cat chinook-data/Chinook_MySql.sql.tmp | tail -c +4 | sed '/CREATE TABLE/,$!d'  > chinook-data/Chinook_MySql.sql.conv

    status "Finished conversion"
    mv chinook-data/Chinook_MySql.sql.conv chinook-data/Chinook_MySql.sql
    rm chinook-data/Chinook_MySql.sql.tmp
fi

status "Creating temporary MySql database ${DATABASE}..."

echo "CREATE DATABASE ${DATABASE}" | run_mysql

(echo "START TRANSACTION; SET autocommit=0;"; pv chinook-data/Chinook_MySql.sql; echo "COMMIT;") | run_mysql "${DATABASE}"

status "Success"
print_open_statement
