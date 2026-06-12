  select
    count(distinct (number, dup_repo_name)) as prs
  from (
    select
      number, dup_repo_name
    from
      gha_issues
    where
      is_pull_request = true
      and (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_user_login) = 'mesutoezdil')
    union select
      number, dup_repo_name
    from
      gha_pull_requests
    where
      (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_user_login) = 'mesutoezdil' or lower(dupn_merged_by_login) = 'mesutoezdil')
    ) s
    ;

  select
    count(distinct (number, dup_repo_name)) as issues
  from
    gha_issues
  where
    is_pull_request = false
    and (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_user_login) = 'mesutoezdil')
    ;

  select
    count(distinct s.event_id) as contributions
  from (
    select
      event_id
    from
      gha_commits
    where
      (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_author_login) = 'mesutoezdil' or lower(dup_committer_login) = 'mesutoezdil')
    union select
      event_id
    from
      gha_issues
    where
      (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_user_login) = 'mesutoezdil')
    union select
      event_id
    from
      gha_pull_requests
    where
      (lower(dup_actor_login) = 'mesutoezdil' or lower(dup_user_login) = 'mesutoezdil' or lower(dupn_merged_by_login) = 'mesutoezdil')
    union select
      event_id
    from
      gha_commits_roles
    where
      lower(actor_login) = 'mesutoezdil'
    union select
      id as event_id
    from
      gha_events
    where
      lower(dup_actor_login) = 'mesutoezdil'
      and type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    ) s
    ;
