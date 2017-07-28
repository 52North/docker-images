FROM mdillon/postgis:9.5

#ENV POSTGRES_USER=postgres
#ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=sos

ADD $POSTGRES_DB.sql.gz /docker-entrypoint-initdb.d/zzz-$POSTGRES_DB.sql.gz
