#!/bin/sh -ex

# https://maven.apache.org/maven-ci-friendly.html
REVISION=$(date +%Y.%m).${${GITHUB_SHA?}:0:7}

./mvnw verify -B -Drevision=${REVISION?} -Plocalstack

ln -fs target/awscat-${REVISION?}.jar awscat.jar

cat > setup.py << EOF
import setuptools
setuptools.setup(name="awscat", version="${REVISION?}", scripts=["awscat.jar"])
EOF

zip awscat.zip awscat.jar setup.py

response_code=$(curl -sv -o output -w "%{response_code}" -H"Authorization: Token ${GITHUB_TOKEN?}" https://api.github.com/repos/percent25/awscat/releases -d @-) << EOF
{
  "tag_name": "${REVISION?}",
  "target_commitish": "${GITHUB_SHA?}"
}
EOF
cat output

if [ $response_code -lt 400 ]; then
  release_id=$(jq .id output)
  response_code=$(curl -sv -o output -w "%{response_code}" -H"Authorization: Token ${GITHUB_TOKEN?}" -H"Content-Type: application/octet-stream" "https://uploads.github.com/repos/percent25/awscat/releases/${release_id?}/assets?name=awscat.zip" --data-binary @awscat.zip)
  cat output
fi
