#!/bin/sh -ex
# ./gradlew run
# mvn spring-boot:run -Dspring-boot.run.arguments="$*"
./mvnw clean package && target/awscat-0.0.1-SNAPSHOT.jar "$@"
