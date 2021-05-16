#!/bin/sh -ex
./mvnw -DskipTests=true clean package && target/awscat-0.0.1-SNAPSHOT.jar "$@"
