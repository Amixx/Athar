# Git Integration Agent Notes

`athar_git/` is presentation and Git workflow integration. It may depend on the
core engine, but `athar/` must never depend on `athar_git/`.

## Responsibilities

- `render.py` produces deterministic terminal/Markdown summaries from existing
  JSON delta reports. It should not alter engine semantics. IFC-derived text
  (schema, class, entity, property set and property names, values, file paths,
  policy messages) appears only inside code spans or fenced blocks, so links,
  images, mentions and HTML in it never render. Code spans fold newlines to
  spaces; fences exceed every backtick run in the text they hold.
- Entity lists print deleted, added, then modified, each with its own cap and
  a "not shown" remainder line. Under a modified entity, `property_deltas`
  render as `Pset.Name: old -> new` first. Raw `data_delta` rows for
  property and quantity facts are dropped only when every value those facts
  lost and gained pairs one to one with a property delta (names by equality;
  strings, integers and booleans by equality with a fact of a compatible IFC
  value type; quantized reals, quantities and enumeration lists by count);
  otherwise every raw row is shown, since the
  engine skips unnamed and null properties and collapses duplicate
  (pset, name, inherited) keys. Other raw rows are always shown. `stats.placement_cohorts` of two or more
  elements print as one "N elements moved by (x, y, z) m" line, largest
  first and capped like the entity lists (`max_cohorts`, default `max_items`,
  then an "and M more placement cohorts" line), and drop those elements' own
  placement rows. Every IFC-derived name, value, path and GlobalId is cut to
  200 characters with an explicit "...[N more characters]" marker, and inside
  the terminal text (and so inside the Markdown fenced block) escapes
  backslashes, carriage returns and newlines, so model text cannot start a
  report row of its own. The match line appears only for entities not
  matched by GlobalId. Numbers never use scientific notation. Counts and
  values shown with a unit (placement moves and vectors) carry thousands
  separators; other model numbers, which may be identifiers or years, print
  their exact digits without separators. Placement moves of a metre or more
  are shown in metres.
  Text the renderers author never contains middle dots; model text is printed
  as the report carries it, middle dots included.
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
