#!/bin/sh -ex
./mvnw clean package -DskipTests=true && target/awscat-0.0.1-SNAPSHOT.jar "$@"
