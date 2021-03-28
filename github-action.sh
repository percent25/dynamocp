#!/bin/sh -ex

echo ${GITHUB_RUN_ID?}
echo ${GITHUB_SHA?}

REVISION=v0.9.${GITHUB_RUN_ID}-${GITHUB_SHA:0:7}

./build.sh ${REVISION?}

# TODO
# TODO
# TODO
exit 0
# TODO
# TODO
# TODO

response_code=$(curl -sv -o output -w "%{response_code}" -H"Authorization: Token ${GITHUB_TOKEN?}" https://api.github.com/repos/percent25/awscat/releases -d @-) << EOF
{
  "tag_name": "${REVISION}",
  "target_commitish": "${GITHUB_SHA}"
}
EOF
cat output

if [ $response_code -lt 400 ]; then
  release_id=$(jq .id output)
  response_code=$(curl -sv -o output -w "%{response_code}" -H"Authorization: token ${GITHUB_TOKEN?}" -H"Content-Type: application/octet-stream" "https://uploads.github.com/repos/percent25/awscat/releases/$release_id/assets?name=awscat.zip" --data-binary @awscat.zip)
  cat output
fi
