#!/bin/sh -ex
./mvnw package -DskipTests=true && target/awscat-0.0.1-SNAPSHOT.jar "$@"
