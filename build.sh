#!/bin/sh -ex

REVISION=${1?}

./mvnw -B -Drevision=${REVISION?} verify
ln -fs target/awscat-${REVISION?}.jar awscat.jar

cat > setup.py << EOF
import setuptools
setuptools.setup(name="awscat", version="${REVISION?}", scripts=["awscat.jar"])
EOF

zip awscat.zip awscat.jar setup.py
# zip awscat-${REVISION?}.zip awscat.jar setup.py
# ln -fs awscat-${REVISION?}.zip awscat.zip
