# Project: SCiMMA SELDON Service

## Git workflow
- **Machine-account repo.** This repository is set up for the `skoranda-agent` machine
  account, which is the condition the global CLAUDE.md's "Machine account" section requires
  before its rules apply. Remotes (confirm with `git remote -v`; all HTTPS):
  - `bot` -> `https://github.com/skoranda-agent/seldon-service.git` -- the machine account's
    fork of `scimma/seldon-service`. Claude's only push target.
  - `upstream` -> `https://github.com/scimma/seldon-service.git` -- canonical, and what pull
    requests target. Never pushed by Claude.
  - `origin` -> `https://github.com/skoranda/seldon-service.git` -- the maintainer's personal
    fork. **Read-only for Claude:** `skoranda-agent` has no write access there. Kept for
    reading and history.
- **Credentials.** There is no `GH_TOKEN` here and none is needed. `gh` is logged in as
  `skoranda-agent` and supplies git credentials through `credential.helper=store`, so a plain
  `git push bot <branch>` works.
- **Verify before any push or PR.** `gh api user --jq .login` must print `skoranda-agent`, and
  `git remote get-url bot` must be owned by `skoranda-agent`. If either differs, stop and say so.
- **Never commit to `main`.** Create a branch first and commit there. If a commit lands on
  `main` by mistake, move it onto a branch: `git branch <name> && git reset --hard HEAD~1`.
- **Shipping flow:** push the feature branch to `bot`, then open a ready-for-review (not draft)
  PR on `upstream`:
  `gh pr create --repo scimma/seldon-service --base main --head skoranda-agent:<branch>`.
  The maintainer reviews and merges there. There is no `origin` PR step.
- **Allowed without asking each time:** push a feature branch to `bot`; open the PR above; push
  follow-up commits to that branch; edit the PR title and body; reply to review comments; read
  CI results.
- **Ask first:** force-pushing a `bot` branch (for example after a rebase), closing a PR, or
  deleting a `bot` branch.
- **Landing record.** The PR opened from a `bot` branch is on `upstream` and **is** the landing
  record -- cite it owner-qualified (`scimma/seldon-service#N`). While it is unmerged, name the
  branch and say the merge is pending. (Earlier PRs in this repo's history came through the old
  `origin` fork flow; their merge subjects carry the upstream number.)
- **Upstream CI on bot PRs:** fork PRs may wait for maintainer approval before Actions run, and
  they get no repository secrets. `gh pr checks` printing "no checks reported" is a red result,
  not a neutral one -- look for an `action_required` run in `gh run list`.
- **Still off-limits for Claude:** pushing to `upstream` or `origin`, pushing to (or
  force-pushing) `main` on any remote, and approving or merging any PR -- those stay with the
  maintainer. A push is off-limits whether `upstream` is named as the remote, given as a URL,
  or reached through a branch whose tracking ref points there.

## Don't
- Push to `upstream` or `origin`, push/force-push `main`, or approve/merge anything -- those
  stay with the maintainer. (Pushing feature branches to `bot` and opening/managing PRs on
  `upstream` is fine; see Git workflow.)
- Print, echo, or copy the value of any token or credential.
