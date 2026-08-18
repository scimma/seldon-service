# Project: SCiMMA SELDON Service

## Git workflow
- **Fork-based repo.** `origin` is the developer's fork; `upstream` is canonical
  and is what pull requests target. Confirm with `git remote -v`.
- **Never commit to `main`.** Create a branch first and commit there, so the work can be
  pushed to `origin` and opened as a pull request against `upstream`. If a commit lands on
  `main` by mistake, move it onto a branch: `git branch <name> && git reset --hard HEAD~1`.
- **`GH_TOKEN` gates writes to `origin`.** Before the first push or `gh` command, check that
  the variable is set *without printing its value*: `test -n "$GH_TOKEN" && echo set`.
  - **Set:** push branches to `origin` and open pull requests on `origin` without asking
    again. This overrides the global "never push to remote" default, for `origin` only.
  - **Unset:** do not attempt the push. Stop and say so, so I can export it or push myself.
  - Never echo, log, or paste the token value anywhere — not in commit messages, PR bodies,
    command output, or a file. Test for presence, never print.
- **Writes go to the fork only.** Before any push, confirm the target is `origin` and that
  `git remote get-url origin` resolves to the fork, not the canonical repo. Push the branch,
  then `gh pr create` against `origin` with an explicit base and repo so `gh` cannot default
  to the parent: `gh pr create --repo <origin-owner>/<repo> --base main`.
- **Still off-limits for Claude:** pushing to `upstream`, pushing to (or force-pushing) `main`
  on any remote, the PR against `upstream`, and every merge — those stay with the maintainer.
  `GH_TOKEN` being set never authorizes any of these. A push is off-limits whether `upstream`
  is named as the remote, given as a URL, or reached through a branch whose tracking ref
  points there.

## Don't
- Push to `upstream`, push/force-push `main`, open a PR against `upstream`, or merge anything —
  those stay with the maintainer, with or without `GH_TOKEN`. (Pushing branches and opening PRs
  on `origin` is fine when `GH_TOKEN` is set; see Git workflow.)
- Print, echo, or copy the value of `GH_TOKEN`.
