#!/bin/sh -ex
# ./gradlew run
mvn spring-boot:run -Dspring-boot.run.arguments="$*"
