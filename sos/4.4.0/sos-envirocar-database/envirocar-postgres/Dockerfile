FROM mdillon/postgis:9.5
MAINTAINER Arne de Wall <a.dewall@52north.org>

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=sos

ADD sos_db_create.sql /docker-entrypoint-initdb.d/
RUN mkdir /data \
    && chown -R postgres /data

ENV PGDATA /data
