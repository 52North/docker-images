# ARCHIVED

This project is no longer maintained and will not receive any further updates. If you plan to continue using it, please be aware that future security issues will not be addressed.

# 52°North Docker Images

Meta project, providing Dockerfiles and docker-compose files for
52°North software projects.

## Structure

A top-level 52°North is represented by a directory. Images for a
version of the project are located in a subdirectory. The Structure
of that subdirectory is to be defined by the project maintainer.

Example for SOS:

```
<repository>
|-- sos
|   |-- 4.4.0
|       |-- sos
|       |   |-- Dockerfile
|       |   +-- docker-compose.yml
|       |-- sos-configured
|       |   |-- ..
|       +-- sos-weather-postgres
+-- helgoland
```
