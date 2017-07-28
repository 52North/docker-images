FROM node
MAINTAINER Arne de Wall <a.dewall@52north.org>

# create folder stuff
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# copy package.json and install node dependencies
COPY package.json /usr/src/app
RUN npm install

# copy sources
COPY app/ /usr/src/app/app

# set environment variables
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=sos

# run the database import
CMD node app/track-pgsos-importer.js
