#!/usr/bin/env bash

set -ev

echo "TRAVIS_PULL_REQUEST is ${TRAVIS_PULL_REQUEST}"
echo "TRAVIS_BRANCH is ${TRAVIS_BRANCH}"
echo "TRAVIS_TAG is ${TRAVIS_TAG}"

test "${TRAVIS_PULL_REQUEST}" == "false"
if [ -z "${TRAVIS_TAG}" ]
then
    echo "TRAVIS_TAG tag is not set, skip deploying"
    exit 0
fi

# Pushing yqlplus_engine
cd yqlplus_engine
mvn deploy --settings ../travis/settings.xml
cd ..

# Pushing yqlplus_stdlib
cd yqlplus_stdlib
mvn deploy --settings ../travis/settings.xml
cd ..

# Pushing yqlplus_language
cd yqlplus_language
mvn deploy --settings ../travis/settings.xml
cd ..

# Pushing yqlplus_source_api
cd yqlplus_source_api
mvn deploy --settings ../travis/settings.xml
cd ..


