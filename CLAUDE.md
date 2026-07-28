@AGENTS.md
@HANDOFF.md

Everything from `AGENTS.md` above is shared across AI coding tools (the
open `AGENTS.md` standard) and kept there so it stays portable. The
`@HANDOFF.md` import is Claude-Code-specific syntax (other tools reading
`AGENTS.md`'s prose pointer to `HANDOFF.md` would need to open it manually)
— it's imported here rather than referenced in `AGENTS.md` so its content
is *guaranteed* loaded every session, not just suggested. `HANDOFF.md`
stays small on purpose (currently ~60 lines) specifically so this is cheap.

This repo has no other Claude-Code-specific mechanism yet (no
`.claude/skills`/`.claude/agents`/hooks), unlike the sibling `cryptoTracker`
repo. Add Claude-specific scaffolding here if it earns its keep, matching
`cryptoTracker/CLAUDE.md`'s pattern.
