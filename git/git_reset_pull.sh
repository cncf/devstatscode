#!/bin/bash
if [ -z "$1" ]
then
  echo "Argument required: path to call git-reset and the git-pull"
  exit 1
fi

cd "$1" || exit 2
git fetch origin || exit 3
DEFAULT_REF="$(git symbolic-ref -q refs/remotes/origin/HEAD || true)"
if [ -n "$DEFAULT_REF" ]
then
  git reset --hard "$DEFAULT_REF" || exit 4
else
  DEFAULT_BRANCH="$(git remote show origin 2>/dev/null | sed -n 's/^[[:space:]]*HEAD branch: //p' | head -n1)"
  if [ -n "$DEFAULT_BRANCH" ]
  then
    git reset --hard "origin/$DEFAULT_BRANCH" || exit 5
  else
    git reset --hard origin/master || git reset --hard origin/main || exit 5
  fi
fi
git pull || exit 6
