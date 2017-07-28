#!/bin/sh
# netcat-traditional is installed via apt-get in the Dockerfile

POSTGRES_TCP_ADDR=${POSTGRES_SOS_HOST:-"postgres"}
POSTGRES_TCP_PORT=${POSTGRES_SOS_PORT:-5432}

TESTING_POSTGRES_URL="$POSTGRES_TCP_ADDR:$POSTGRES_TCP_PORT"

echo "checking postgres availability at $TESTING_POSTGRES_URL"
sh -c "nc -v -z $POSTGRES_TCP_ADDR $POSTGRES_TCP_PORT" >/dev/null 2>&1
while [ $? -ne 0 ]; do
  echo "$(date) - still trying to connect to postgres at $TESTING_POSTGRES_URL"
  sleep 5
  sh -c "nc -v -z $POSTGRES_TCP_ADDR $POSTGRES_TCP_PORT" >/dev/null 2>&1
done
echo "postgres available at $TESTING_POSTGRES_URL !"

$@
