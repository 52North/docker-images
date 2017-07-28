FROM 52north/sos:4.4.0
MAINTAINER Matthes Rieke <m.rieke@52north.org>

# Project: https://github.com/52North/sos

RUN apt-get -y install netcat && apt-get clean

COPY sos-config/configuration.db $CATALINA_HOME/webapps/52n-sos-webapp/
COPY sos-config/datasource.properties $CATALINA_HOME/webapps/52n-sos-webapp/WEB-INF
COPY tomcat-bin/* $CATALINA_HOME/bin

RUN chmod +x $CATALINA_HOME/bin/*.sh
