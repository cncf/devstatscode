#!/bin/bash
if [ -z "$1" ]
then
  echo "Argument required: repo path"
  exit 1
fi
git -C "${1}" tag -l --format="%(refname:short)♂♀%(creatordate:unix)♂♀%(subject)"
