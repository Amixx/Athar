# Git Integration Agent Notes

`athar_git/` is presentation and Git workflow integration. It may depend on the
core engine, but `athar/` must never depend on `athar_git/`.

## Responsibilities

- `render.py` produces deterministic terminal/Markdown summaries from existing
  JSON delta reports. It should not alter engine semantics.
- `cache.py` stores persistent `SignatureBundle` objects for Git diffs, keyed
  by Git blob oid when available and by content sha256 otherwise. Cache entries
  are canon-version scoped and invalidated when pickle metadata or bundle canon
  version differs. V1 has no eviction policy; document cleanup rather than
  implying bounded storage.
- Git LFS pointer files are resolved transparently in `load_or_build_bundle`:
  the pointer's `oid sha256:` becomes the cache key (a cached diff never
  downloads the blob), and a miss materializes content via `git lfs smudge`
  with a hash check. Modern Git usually smudges blobs before invoking the
  external driver, so this path is the safety net for `GIT_LFS_SKIP_SMUDGE=1`
  checkouts, partial fetches, and direct invocations on pointer files.
- `cli.py` implements `athar git diff` and `athar git install`. The external
  diff driver accepts Git's seven-argument shape:
  `path old-file old-hex old-mode new-file new-hex new-mode`.
- `athar git install` configures `diff.athar.command=athar git diff --external`
  and writes `.gitattributes` guidance: `*.ifc diff=athar -merge`.
- The `-merge` marker is intentional. Git text merge can silently corrupt IFC
  STEP exports; semantic merge is out of scope.
- `pr_bot.py` discovers changed `.ifc` files from
  `git diff --name-status -z base...head`, diffs modified/renamed pairs through
  the cached path, renders Markdown, and creates or updates one stable GitHub
  issue comment identified by an HTML sentinel.

## Boundaries

- File-level add/delete entries are reported as semantic pair-diff skips
  because the engine requires both sides.
- `--policy-result` may surface an existing `athar check` result in a PR
  comment, but policy enforcement remains in `athar/check.py`.
