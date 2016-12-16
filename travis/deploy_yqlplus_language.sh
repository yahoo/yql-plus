#!/usr/bin/env bash

set -e

echo "TRAVIS_TAG is ${TRAVIS_TAG}" 

test "${TRAVIS_PULL_REQUEST}" == "false"
test "${TRAVIS_BRANCH}" == "master"
test "${TRAVIS_TAG}" != ""
cd yqlplus_language
echo "inside yqlplus_language"
pwd
echo $?
mvn deploy --settings ../travis/settings.xml
