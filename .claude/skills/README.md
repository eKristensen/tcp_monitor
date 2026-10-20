# Vendored agent skills

Third-party skills that Claude Code loads in this repository. They are committed here
(not installed per developer) so every session, local or cloud, uses the same version.
`CLAUDE.md` says when to use them.

| Skill | Source | Version | License |
|-------|--------|---------|---------|
| `rust-best-practices/` | [apollographql/skills](https://github.com/apollographql/skills), `skills/rust-best-practices` | 1.1.2, commit `f9843dce86b1a561e3bfbb024e65c28a6cbbe38c` | MIT (`rust-best-practices/LICENSE`) |
| `pragmatic-rust-guidelines/` | [microsoft/rust-guidelines](https://github.com/microsoft/rust-guidelines), `src/agents/all.txt` | commit `19723b30aadf1854fef6a56c2da2395dc03255c1` | MIT (`pragmatic-rust-guidelines/LICENSE.md`) |

Both are copied unmodified, except that `pragmatic-rust-guidelines/SKILL.md` is a local
wrapper. Microsoft publishes the guidelines as one agent-oriented text file, not as a skill.

## Updating

```bash
tmp=$(mktemp -d)

# Apollo: copy the skill directory as is.
git clone --depth 1 https://github.com/apollographql/skills.git "$tmp/apollo"
rm -rf .claude/skills/rust-best-practices
cp -r "$tmp/apollo/skills/rust-best-practices" .claude/skills/
cp "$tmp/apollo/LICENSE" .claude/skills/rust-best-practices/LICENSE

# Microsoft: all.txt is stored in Git LFS, so rebuild it with their own script
# (or download https://microsoft.github.io/rust-guidelines/agents/all.txt).
git clone --depth 1 https://github.com/microsoft/rust-guidelines.git "$tmp/ms"
bash "$tmp/ms/scripts/agents_summary.sh"
cp "$tmp/ms/src/agents/all.txt" .claude/skills/pragmatic-rust-guidelines/references/all.txt
```

Afterwards, update the commit hashes above, check that the guideline IDs listed in
`pragmatic-rust-guidelines/SKILL.md` still exist, and review the diff before committing.
