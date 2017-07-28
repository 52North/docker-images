FROM tomcat:9
MAINTAINER Matthes Rieke <m.rieke@52north.org>

# Project: https://github.com/52North/sos

WORKDIR /tmp
#COPY tomcat-conf/* $CATALINA_HOME/conf/
#COPY rest-api-config/* sos-webapp/WEB-INF/classes/
COPY tomcat-conf/ /tmp/tomcat-conf/
COPY rest-api-config/ /tmp/rest-api-config/
COPY helgoland-config/ /tmp/helgoland-config/
RUN curl -L -o sos-bundle.zip https://github.com/52North/sos/releases/download/v4.4.0/52n-sensorweb-sos-bundle-4.4.0.zip \
    && unzip -d sos-bundle sos-bundle.zip \
    && unzip -d sos-webapp sos-bundle/*/bin/target/*.war \
    && cp -R /tmp/tomcat-conf/* $CATALINA_HOME/conf/ \
    && cp -R /tmp/rest-api-config/* sos-webapp/WEB-INF/ \
    && curl -sL https://deb.nodesource.com/setup_6.x | bash - \
    && apt-get -y install nodejs git g++ python pkg-config libcairo2-dev libjpeg62-turbo-dev libpango1.0-dev libgif-dev build-essential \
    && npm install -g grunt grunt-cli node-gyp \
    && curl -L -o helgoland.zip https://github.com/52North/helgoland/archive/v1.0.0.zip \
    && unzip -d helgoland helgoland.zip \
    && cd helgoland/helgoland-1.0.0 \
    && npm install && grunt \
    && cp /tmp/helgoland-config/* dist/ \
    && cp -R dist/ ../../sos-webapp/static/client/helgoland \
    && cd ../../ \
    && cp -R sos-webapp/ $CATALINA_HOME/webapps/52n-sos-webapp \
    && npm uninstall -g grunt grunt-cli node-gyp \
    && apt-get -y purge nodejs git python g++ python python3.5 python2.7 python3 pkg-config libcairo2-dev libjpeg62-turbo-dev libpango1.0-dev libgif-dev build-essential \
    && apt-get -y autoremove --purge \
    && apt-get clean \
    && rm -rf /tmp/* && rm -rf /usr/lib/node_modules && rm -rf /root/.npm
ENV JAVA_OPTS="-XX:PermSize=4096m -XX:MaxPermSize=3072m"
