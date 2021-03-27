#!/bin/sh -ex

REVISION=${1?}

./mvnw -Drevision=${REVISION?} package
ln -fs target/awscat-${REVISION?}.jar awscat.jar

cat > setup.py << EOF
import setuptools
setuptools.setup(name="awscat", version="${REVISION?}", scripts=["awscat.jar"])
EOF

zip awscat-${REVISION?}.zip setup.py awscat.jar
ln -fs awscat-${REVISION?}.zip awscat.zip
