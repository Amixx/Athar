# Git Integration Agent Notes

`athar_git/` is presentation and Git workflow integration. It may depend on the
core engine, but `athar/` must never depend on `athar_git/`.

## Responsibilities

- `render.py` produces deterministic terminal/Markdown summaries from existing
  JSON delta reports. It should not alter engine semantics.
- `cache.py` stores persistent `SignatureBundle` objects for Git diffs, keyed
  by Git blob oid when available and by content sha256 otherwise. Cache entries
  are canon-version scoped and invalidated when pickle metadata, the
  `athar-ifc` distribution version (`importlib.metadata`, through
  `athar.engine.package_version`, the same source as the report's
  `audit.producer.version` and `athar --version`), or bundle canon version
  differs. V1 has no eviction policy; document cleanup rather than implying
  bounded storage.
- The cache directory (`ATHAR_CACHE_DIR`, `$XDG_CACHE_HOME/athar/signatures`,
  or `~/.cache/athar/signatures`) is trusted local state: entries are pickles,
  and unpickling runs code, so never point it at a shared, downloaded, or
  CI-restored directory you do not control. `cache.py` creates a missing cache
  root and canon-version subdirectory with mode 0700 and, on POSIX, refuses
  either directory (a `PermissionError` naming it, before any entry is
  unpickled) when it is not owned by the current user or is group- or
  other-writable. Existing directories are checked, never chmod-ed.
- Git LFS pointer files are resolved transparently in `load_or_build_bundle`:
  the pointer's `oid sha256:` becomes the cache key (a cached diff never
  downloads the blob), and a miss materializes content via `git lfs smudge`
  with a hash check. Modern Git usually smudges blobs before invoking the
  external driver, so this path is the safety net for `GIT_LFS_SKIP_SMUDGE=1`
  checkouts, partial fetches, and direct invocations on pointer files.
- `cli.py` implements `athar git diff` and `athar git install`. The external
  diff driver accepts Git's seven-argument shape:
  `path old-file old-hex old-mode new-file new-hex new-mode`.
- `athar git install` configures `diff.athar.command` as
  `<athar> git diff --external` (shell-quoted), where `<athar>` is resolved at
  install time: the absolute console script listed in the running `athar-ifc`
  distribution's own record (a `bin` or `Scripts` entry) when it exists on
  disk, otherwise `<sys.executable> -c <bootstrap>`, which removes
  the current-directory entry from `sys.path` before importing `athar`.
  `--difftool` uses the same prefix for `difftool.athar.cmd`. Git runs drivers
  from the worktree top level, so the command must never be `python -m`: that
  puts the cwd first on `sys.path`, and a diffed repository's own `athar.py`
  or `json.py` would run. `-P` needs Python 3.11 and `-I` drops the user
  site-packages a `pip install --user` uses. The command works from a venv
  that is not activated and without `athar` on `PATH`. Locally it appends `*.ifc diff=athar -merge` to the
  `.gitattributes` at `git rev-parse --show-toplevel`, and fails before
  touching any config when run outside a repository (`--global` writes no
  attributes file). Reinstall after moving or recreating the venv.
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
