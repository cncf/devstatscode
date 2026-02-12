#!/bin/bash
if [ -z "$1" ]
then
  echo "Arguments required: path sha, none given"
  exit 1
fi
if [ -z "$2" ]
then
  echo "Arguments required: path sha, only path given"
  exit 2
fi

if ! output=`git -C "${1}" show "$2" --shortstat --oneline 2>/dev/null`
then
  exit 4
fi
output=`echo "$output" | tail -1 | sed 's/^[[:space:]]*//'`
echo "$output"
