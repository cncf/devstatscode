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

git -C "$1" show -s --format=%ct "$2" || exit 4

git -C "$1" diff-tree --no-commit-id --name-only -M7 -r -z "$2" 2>/dev/null | \
while IFS= read -r -d '' file
do
    file_and_size=`git -C "$1" ls-tree -r -l "$2" -- "$file" | awk '{print $5 "♂♀" $4}'`
    if [ -z "$file_and_size" ]
    then
      echo "$file♂♀-1"
    else
      echo "$file_and_size"
    fi
done || exit 5

