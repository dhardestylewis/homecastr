> **Synced from**: https://github.com/dhardestylewis/GUIDELINES/main/GUIDELINES.md (filtered for relevance)
> **Updated**: 2026-01-14T20:25

---

## CRITICAL Rules (Adapted for Web Project)

> **CRITICAL**: Always prioritize by TODO priority order. New user prompts should be triaged into the TODO list by priority, not necessarily addressed immediately. (UNREVIEWED) [Added: 2026-01-14 20:25]

> **CRITICAL**: Commit at every turn. After each response that modifies files, run `git add -A && git commit -m "..."`. [Adapted: using `&&` for PowerShell compatibility]

> **CRITICAL**: NEVER perform `git rebase` or `git reset` without explicit permission.

> **CRITICAL**: Maintain TODO.md for incomplete work. When user sends prompts before prior work is done, add items to TODO.

> **CRITICAL**: Update PROMPTS-LOG.md at every turn with new user prompts verbatim. Timestamp each entry `[YYYY-MM-DD HH:MM]`.

> **CRITICAL**: Do not interrupt the user's view with walkthrough artifacts. Maintain silently.

> **CRITICAL**: At each prompt turn, evaluate whether the user's instruction should become a guideline. If yes, add immediately.

> **META**: All new guidelines must be marked `(UNREVIEWED)` with timestamp `[Added: YYYY-MM-DD HH:MM]`.

---

## P1 - Version Control (Every Session)

### 1.1 Commit Hygiene
- **Atomic Commits**: Commit logically grouped changes with descriptive messages.
- **No Dangling Work**: Never leave uncommitted changes at end of session.
- **Push periodically**: Run `git push` after logical batches of commits.

### 1.2 Strict Prohibitions
- **NO REBASE/RESET**: Never without explicit, written user permission.
- **NO SWITCH IF DETACHED**: If in detached HEAD state, do NOT switch branches without permission.

---

## P2 - Documentation

### 2.1 Prompt Logging
- Append prompts verbatim to `PROMPTS-LOG.md`.
- New sessions use header `## YYYY-MM-DDTHH:MM - Session Description`.
- Never truncate or use ellipses. Log exact full text.
- Append ONLY - do not revise existing logs.

### 2.2 TODO Management
- Assess priority (P1, P2, P3) and insert accordingly.
- Append `[Added: YYYY-MM-DD HH:MM]` to new items.
- Move completed items from `TODO.md` to `TODO-COMPLETED.md`.

---

## P3 - Housekeeping

### 3.1 Safe Deletion
- Delete files only after verifying content is captured elsewhere.
- Track deferred work in `TODO.md`.

---

## Project-Specific: Properlytic UI

### Target Audience: Homeowners (UNREVIEWED) [Added: 2026-01-14 20:12]

Based on competitive research (Zillow shows Zestimate as dollar value only; Redfin shows estimate with confidence range; neither uses numeric scores for homeowners):

- Show **dollar values and growth %**, not abstract scores
- Avoid σ-notation and technical metrics (hide for homeowner view)
- Use plain English: "High/Medium/Low" instead of percentages
- Hide investor-only metrics (DSCR, Cap Rate, Breakeven, Risk Factor)
- **Note**: Users may have retail investing experience (Robinhood, Kalshi) so some familiarity with basic concepts [Added: 2026-01-14 20:33]

### Code Practices (UNREVIEWED) [Added: 2026-01-14 20:14]

- **Hide vs Remove**: When deprecating features, comment out (with `{false && ...}`) rather than delete
- **Small edits**: Make targeted, atomic edits rather than large rewrites

---

## Not Applicable to This Project

The following master guidelines are **excluded** as not relevant to a Next.js/TypeScript web application:

- Citation Integrity (IEEE format) - for academic papers
- LaTeX Compilation - not applicable
- Bibliography Management (BibTeX) - not applicable
