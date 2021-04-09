#!/bin/sh -ex

REVISION=$(git describe --always --dirty)

./mvnw -Drevision=${REVISION?} package
ln -fs target/awscat-${REVISION?}.jar awscat.jar

cat > setup.py << EOF
import setuptools
setuptools.setup(name="awscat", version="${REVISION?}", scripts=["awscat.jar"])
EOF

zip awscat.zip awscat.jar setup.py
