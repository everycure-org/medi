# Peripheral Resources

External standards, tools, and datasets that are adjacent to MeDIC — worth knowing about, referenced in discussions, but not core pipeline dependencies. See core sources in `docs/sources/` and architecture in `docs/architecture.md`.

## Standards & schemas

- **Biolink Model — `treats` predicate** — https://biolink.github.io/biolink-model/treats/
  - Maintained by the Biolink Model project (LinkML-based; https://github.com/biolink/biolink-model). MeDIC's KGX export is already Biolink-compliant (`docs/architecture.md`).
  - `treats` holds between an intervention and a medical condition, and per Biolink should only be asserted with strong evidence (FDA approval, Phase 3+ trials, established practice); weaker evidence should use alternative predicates or be marked as prediction. Two more-specific mixins hang off it: `ameliorates_condition` and `preventative_for_condition`.
  - **Why it matters:** directly relevant to the indication predicate-mapping priority — choosing between `treats` / `indicated for` / `ameliorates` / `preventative for` for extracted drug→disease associations. Surfaced in the [2026-07-06 Kevin/Nico meeting](../background/meetings/2026.07.06_kevin_nico.md).

## Databases

- **DrugMechDB** (discontinued) — example entry: https://sulab.github.io/DrugMechDB/db05018-mesh-d000795-1.html
  - Maintained by the Su Lab. A manually curated database of drug mechanism paths: for each drug–disease indication, a mechanistic chain from drug through proteins/processes/chemicals to the disease, using Biolink relationship terminology (e.g. `DECREASES ACTIVITY OF`, `POSITIVELY REGULATES`, `INCREASES ABUNDANCE OF`, `POSITIVELY CORRELATED WITH`).
  - **Why it's peripheral:** discontinued, and MeDIC does not model the mechanistic/pathophysiology path (that is closer to Dismech's scope). Discussed as background in the [2026-07-06 Kevin/Nico meeting](../background/meetings/2026.07.06_kevin_nico.md), with an open question about whether its entries are indications broken down into mechanisms or predictions based on mechanistic chains.
