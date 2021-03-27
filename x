#!/bin/sh -ex
# ./gradlew run
REVISION=$(git describe --always --dirty)
# mvn spring-boot:run -Dspring-boot.run.arguments="$*"
./mvnw -Drevision=${REVISION?} package && target/awscat-${REVISION?}.jar "$@"
# ./mvnw -Drevision=${REVISION?} clean package && target/awscat-${REVISION?}.jar "$@"
