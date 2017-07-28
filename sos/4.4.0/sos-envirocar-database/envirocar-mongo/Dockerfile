FROM mongo
MAINTAINER Arne de Wall <a.dewall@52north.org>

RUN apt-get update \
    && apt-get -y install curl \
    && apt-get -y install xz-utils

# Download and unpack the envirocar dump
WORKDIR /tmp
RUN curl -L -o envirocar-dump.tar.xz https://52north.org/delivery/enviroCar-db-dump/enviroCar-dump_2016-02-03.tar.xz \
    && unxz envirocar-dump.tar.xz \
    && tar xf envirocar-dump.tar 

# Set the default mongodb config (not necessary anymore)
RUN mv /etc/mongod.conf.orig /etc/mongod.conf

# Start mongodb in order to restore the envirocar dump
RUN mongod --dbpath /data --fork --logpath /var/log/mongod.log \
    && mongorestore dump \
    && mongod --dbpath /data --shutdown

# start mongodb on startup
WORKDIR /
CMD mongod --dbpath /data
