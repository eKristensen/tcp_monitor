# Vendored agent skills

Third-party skills that Claude Code loads in this repository. They are committed here
(not installed per developer) so every session, local or cloud, uses the same version.
`CLAUDE.md` says when to use them.

| Skill | Source | Version | License |
|-------|--------|---------|---------|
| `rust-best-practices/` | [apollographql/skills](https://github.com/apollographql/skills), `skills/rust-best-practices` | 1.1.2, commit `f9843dce86b1a561e3bfbb024e65c28a6cbbe38c` | MIT (`rust-best-practices/LICENSE`) |
| `pragmatic-rust-guidelines/` | [microsoft/rust-guidelines](https://github.com/microsoft/rust-guidelines), `src/agents/all.txt` | commit `19723b30aadf1854fef6a56c2da2395dc03255c1` | MIT (`pragmatic-rust-guidelines/LICENSE.md`) |

## Verbatim excerpts, not full copies

Only the parts that apply to a small application crate like tcp-monitor are kept. Every
kept line is upstream text, unchanged. Lines are only removed, never edited.

- **Apollo** (`rust-best-practices/`): `SKILL.md` and chapters 1, 2, 4, 5, 8 (idioms,
  linting, errors, testing, docs).
  - Removed: chapters 3, 6, 7, 9 (performance, generics and dispatch, type state, pointers),
    plus their four entries in `SKILL.md`'s chapter list. That is the only edit to `SKILL.md`.
  - Chapter 1 still contains one upstream "More on Chapter 3" link, which now leads nowhere.
- **Microsoft** (`pragmatic-rust-guidelines/`): `references/excerpt.txt` is `all.txt`
  filtered by `excerpt.awk`.
  - Kept: the header; the AI, Application, Correctness, Documentation, Project, and
    Universal sections in full; and M-INTEGRATION-TESTS, M-LOG-NOT-PRINT, and
    M-MOCKABLE-SYSCALLS from Libraries / Resilience.
  - Removed: FFI, Macros, Performance, Safety, and the rest of the Libraries sections.
  - `SKILL.md` is a local wrapper; Microsoft publishes the guidelines as one agent-oriented
    text file, not as a skill.

## Updating

```bash
tmp=$(mktemp -d)
skills=.claude/skills

# Apollo: copy the kept files from the skill directory.
git clone --depth 1 https://github.com/apollographql/skills.git "$tmp/apollo"
src="$tmp/apollo/skills/rust-best-practices"
cp "$src/SKILL.md" "$skills/rust-best-practices/SKILL.md"
for n in 01 02 04 05 08; do
  cp "$src/references/chapter_$n.md" "$skills/rust-best-practices/references/"
done
cp "$tmp/apollo/LICENSE" "$skills/rust-best-practices/LICENSE"
# Then delete the chapter 3, 6, 7 and 9 entries from SKILL.md's chapter list again.

# Microsoft: all.txt is stored in Git LFS, so rebuild it with their own script,
# then filter it. The script output is canonical; the published/LFS copy can
# differ by a few bytes from the script output at the same commit.
git clone --depth 1 https://github.com/microsoft/rust-guidelines.git "$tmp/ms"
bash "$tmp/ms/scripts/agents_summary.sh"
awk -f "$skills/pragmatic-rust-guidelines/excerpt.awk" "$tmp/ms/src/agents/all.txt" \
  > "$skills/pragmatic-rust-guidelines/references/excerpt.txt"
```

Running the Microsoft steps at the commit above must reproduce `references/excerpt.txt`
exactly: sha256 `597d2f67b39a97c09d43f5d1d6f40be5cddc50819e434709322da2c47c3a7d8a`. The
unfiltered `all.txt` at that commit has sha256
`e69533b0541a5d185ad9d8304f5eb5cf4b033135b7046f89c499b6126dced3b6`.

After an update:

1. Update the commit hashes and checksums above.
2. Check that every guideline ID named in `pragmatic-rust-guidelines/SKILL.md` and
   `CLAUDE.md` is still present in `excerpt.txt`.
3. Review the diff before committing.
