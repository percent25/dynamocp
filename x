#!/bin/sh -ex
./mvnw -DskipTests=true package && target/awscat-0.0.1-SNAPSHOT.jar "$@"
