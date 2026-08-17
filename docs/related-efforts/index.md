# Related efforts

Assessments of other projects that solve part of what MeDIC solves, or that MeDIC could borrow
from. Each one answers the same three questions: **what does it actually do**, **where does it
genuinely overlap with MeDIC**, and **what should we take, ignore, or deliberately do differently**.

These are analyses, not proposals. They record a judgement made at a point in time, with the
evidence behind it, so a later decision does not have to start from scratch — and so that
"we looked at X" is a thing the repository can show rather than something someone remembers.

## Contents

| | |
|---|---|
| [Open Targets](open-targets.md) | The target–disease association platform. Where MeDIC's indication layer sits relative to OT's ~50 evidence streams, and what its disease grounding (OnToma/EFO), evidence scoring and release engineering are worth borrowing. |

## Adding one

Name the file after the project, not after the comparison (`open-targets.md`, not
`open-targets-gap-analysis.md`) — the directory already says it is a comparison. Keep the
status/author/date header, cite sources inline, and mark anything you could not pin down rather
than smoothing over it. A confident sentence nobody checked is worse than a flagged gap.

## Related, but filed elsewhere

[`docs/sepio-sieve-alignment.md`](../sepio-sieve-alignment.md) compares MeDIC's provenance model
with SEPIO and the sibling [`sieve`](https://github.com/monarch-initiative/sieve) project, and is
the same kind of document. It lives outside this directory only because
`specs/2026-08-09-source-scoped-association-provenance-design.md` links to it at that path; move
it here and fix that link if you want the set together.
