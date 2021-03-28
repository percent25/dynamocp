#!/bin/sh -ex
./mvnw package && target/awscat-0.0.1-SNAPSHOT.jar "$@"
