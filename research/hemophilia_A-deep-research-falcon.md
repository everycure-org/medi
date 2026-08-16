---
provider: falcon
model: Edison Scientific Literature
cached: false
start_time: '2026-04-03T20:47:19.946699'
end_time: '2026-04-03T21:18:44.861956'
duration_seconds: 1884.92
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: hemophilia A
  mondo_id: MONDO:0010602
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    temperature: 0.1
citation_count: 30
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** hemophilia A
- **MONDO ID:** MONDO:0010602

## Research Objectives

Please provide a comprehensive research report on **drug therapies for hemophilia A**.
Focus on approved treatments, investigational drugs, repurposing candidates, and off-label uses.

### Required Information

#### 1. Approved Drug Therapies
- What drugs are currently approved for this disease?
- Which regulatory agencies have approved them (FDA, EMA, PMDA, etc.)?
- What is the mechanism of action for each approved drug?

#### 2. Investigational and Pipeline Drugs
- What drugs are currently in clinical trials for this disease?
- What phase are they in (Phase I, II, III, IV)?
- What are the key clinical trial identifiers (NCT numbers)?

#### 3. Drug Repurposing Candidates
- Are there drugs approved for other indications that show promise for this disease?
- What evidence supports their potential use (case reports, off-label experience, preclinical data)?

#### 4. Contraindications
- Which drugs are contraindicated in patients with this disease?
- What is the mechanistic basis for contraindication?

#### 5. Adverse Events of Relevance
- Are there drugs known to cause or worsen this disease as an adverse effect?
- What is the frequency and severity of these adverse effects?

#### 6. Combination Therapies
- Are there established combination therapy regimens?
- What synergistic or additive effects are documented?

### Citation Requirements

- Cite primary literature (PMID preferred) for all claims
- Include clinical trial references (NCT numbers) where applicable
- Prioritize recent systematic reviews, meta-analyses, and landmark clinical trials
- Include direct quotes where possible to support key statements
- For regulatory approvals, reference the specific label or approval document

### Output Format

Structure your response as a narrative report with:
- Drug names (use generic/INN names)
- Disease-drug relationship type (indication, contraindication, adverse event, investigational)
- Evidence strength and source type (clinical trial, case report, regulatory label, etc.)
- PMIDs for all cited literature


## Output

Question: You are an expert researcher providing comprehensive, well-cited information.

Provide detailed information focusing on:
1. Key concepts and definitions with current understanding
2. Recent developments and latest research (prioritize 2023-2024 sources)
3. Current applications and real-world implementations
4. Expert opinions and analysis from authoritative sources
5. Relevant statistics and data from recent studies

Format as a comprehensive research report with proper citations. Include URLs and publication dates where available.
Always prioritize recent, authoritative sources and provide specific citations for all major claims.

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** hemophilia A
- **MONDO ID:** MONDO:0010602

## Research Objectives

Please provide a comprehensive research report on **drug therapies for hemophilia A**.
Focus on approved treatments, investigational drugs, repurposing candidates, and off-label uses.

### Required Information

#### 1. Approved Drug Therapies
- What drugs are currently approved for this disease?
- Which regulatory agencies have approved them (FDA, EMA, PMDA, etc.)?
- What is the mechanism of action for each approved drug?

#### 2. Investigational and Pipeline Drugs
- What drugs are currently in clinical trials for this disease?
- What phase are they in (Phase I, II, III, IV)?
- What are the key clinical trial identifiers (NCT numbers)?

#### 3. Drug Repurposing Candidates
- Are there drugs approved for other indications that show promise for this disease?
- What evidence supports their potential use (case reports, off-label experience, preclinical data)?

#### 4. Contraindications
- Which drugs are contraindicated in patients with this disease?
- What is the mechanistic basis for contraindication?

#### 5. Adverse Events of Relevance
- Are there drugs known to cause or worsen this disease as an adverse effect?
- What is the frequency and severity of these adverse effects?

#### 6. Combination Therapies
- Are there established combination therapy regimens?
- What synergistic or additive effects are documented?

### Citation Requirements

- Cite primary literature (PMID preferred) for all claims
- Include clinical trial references (NCT numbers) where applicable
- Prioritize recent systematic reviews, meta-analyses, and landmark clinical trials
- Include direct quotes where possible to support key statements
- For regulatory approvals, reference the specific label or approval document

### Output Format

Structure your response as a narrative report with:
- Drug names (use generic/INN names)
- Disease-drug relationship type (indication, contraindication, adverse event, investigational)
- Evidence strength and source type (clinical trial, case report, regulatory label, etc.)
- PMIDs for all cited literature


## Comprehensive research report: drug therapies for Hemophilia A (MONDO:0010602)

### Scope and evidence approach
This report synthesizes peer‑reviewed literature (emphasis on 2023–2024 where available), guideline-style expert guidance, and ClinicalTrials.gov records to summarize (1) key concepts, (2) approved therapies and mechanisms, (3) investigational/pipeline drugs with trial identifiers, (4) repurposing/off‑label candidates, (5) contraindications and adverse events relevant to hemophilia A, and (6) combination regimens. Where the retrieved evidence did not include primary regulatory labels or legacy product approvals (e.g., older FVIII concentrates), the report is limited to what is explicitly supported by the available sources.

### 1) Key concepts and current understanding (2024-focused)

#### Disease definition and severity
Hemophilia A (HA) is characterized by deficiency of factor VIII (FVIII). Severity is typically defined by FVIII activity: severe <1%, moderate 1–<5%, mild 5–<40% (samelsonjones2024roctaviangenetherapy pages 1-2). In the gene-therapy review of Roctavian, FVIII levels ≈12–20% are noted as protective against spontaneous hemorrhage and joint bleeds (samelsonjones2024roctaviangenetherapy pages 1-2).

#### Core management concepts
**Prophylaxis vs on-demand.** Prophylaxis aims to prevent spontaneous and activity-related bleeding; on-demand therapy treats acute bleeds and perioperative hemostasis. A contemporary perioperative review emphasizes that patients now present on standard half-life or extended half-life FVIII, non‑factor therapy, or after gene therapy; however, many of these newer modalities still require supplementation with traditional factor therapy for surgery (lowell2024perioperativemanagementof pages 4-5).

**Factor replacement vs non‑factor therapy vs gene therapy.**
* Nonfactor therapies are defined as agents that “correct defects in the coagulation process without replacing the missing protein” (lowell2024perioperativemanagementof pages 4-5). They include FVIII mimetics (e.g., emicizumab) and “rebalancing” approaches that inhibit anticoagulant pathways (anti‑TFPI antibodies; antithrombin lowering via siRNA such as fitusiran; and APC inhibition via SerpinPC) (peyvandi2024exploringnonreplacementtherapies’ pages 1-2, wu2024advancesinbiopharmaceutical pages 10-11).
* Gene therapy for HA is currently dominated by AAV-mediated gene addition aiming for episomal hepatocyte expression of B‑domain deleted FVIII (samelsonjones2024roctaviangenetherapy pages 1-2).

**Inhibitors.** FVIII inhibitors (neutralizing alloantibodies) profoundly affect treatment choice, shifting management toward bypassing agents and nonfactor therapies. A recent iScience review reports inhibitor prevalence estimates of 5–7% overall and 20–40% in severe HA, and cites immune tolerance induction (ITI) success rates around 60–70% (wu2024advancesinbiopharmaceutical pages 7-8).

### 2) Approved drug/biologic therapies for Hemophilia A

#### 2.1 FVIII replacement products (class-level, legacy standard of care)
FVIII concentrates remain the foundational therapy class for HA, used for prophylaxis, on-demand treatment, and perioperative management. Inhibitor eradication can involve ITI using FVIII concentrates (wu2024advancesinbiopharmaceutical pages 7-8). The retrieved evidence did not contain a consolidated list of specific FVIII concentrate brands or their historical FDA/EMA approval dates; therefore, this report provides class-level coverage.

#### 2.2 Extended half-life FVIII: **efanesoctocog alfa (Altuviiio; BIVV001)**
*Relationship type:* Indication (routine prophylaxis, on-demand bleed control, and perioperative management).

**Mechanism / key concept.** Efanesoctocog alfa is described as an antihemophilic factor (recombinant) “Fc‑VWF‑XTEN fusion protein” and is characterized as a VWF‑independent FVIII replacement with prolonged half‑life/clearance independent of endogenous VWF (keam2023efanesoctocogalfafirst pages 5-6).

**Regulatory approvals (as supported by retrieved sources).** A Drugs “First Approval” review reports US approval in Feb 2023 and cites FDA approval media releases dated 23 Feb 2023; a 2024 Haematologica review states it received regulatory approval in 2023 in the United States and Japan, while EMA review was ongoing at the time (keam2023efanesoctocogalfafirst pages 5-6).

**Key trials (NCT).** XTEND‑1: NCT04161495; long‑term extension XTEND‑ed: NCT04644575 (keam2023efanesoctocogalfafirst pages 5-6).

**Evidence strength.** Strong for approval status and mechanism from a “first approval” synthesis; the retrieved excerpts did not include primary label excerpts or pivotal efficacy statistics.

#### 2.3 Nonfactor FVIIIa mimetic: **emicizumab** (Hemlibra)
*Relationship type:* Indication (prophylaxis); combination‑dependent contraindication/safety limitation (with aPCC).

**Mechanism.** Emicizumab is a bispecific monoclonal antibody that bridges activated factor IX and factor X, mimicking activated FVIII function (lowell2024perioperativemanagementof pages 4-5).

**Approval status in retrieved evidence.** A perioperative review states emicizumab is the only FDA‑approved nonfactor product for bleed prophylaxis (lowell2024perioperativemanagementof pages 4-5). (The retrieved evidence did not include the original FDA/EMA approval dates.)

**Real‑world implementation: perioperative/surgical management.** In a perioperative synthesis of HAVEN surgical experience (215 minor, 18 major surgeries), 86% of minor procedures performed without additional prophylactic factors had no postoperative bleeds, and 80% of major surgeries had no intra‑ or postoperative bleeds; no perioperative thrombotic events were reported (lowell2024perioperativemanagementof pages 4-5).

**Important safety issue.** Thromboembolism risk rises when emicizumab is co‑administered with activated prothrombin complex concentrate (aPCC): one review reports thromboembolism in the range 0.5–5.4% in those who received at least one dose of aPCC, with no thrombotic events when aPCC was dosed <100 IU/kg/day (lowell2024perioperativemanagementof pages 4-5).

**Evidence strength.** High for mechanism and perioperative outcomes from peer‑reviewed synthesis, plus guideline-style recommendations (see Contraindications/Combination therapy sections).

#### 2.4 AAV gene therapy: **valoctocogene roxaparvovec (Roctavian)**
*Relationship type:* Indication (adults with severe HA; approval conditions vary by region).

**Mechanism.** Roctavian is an AAV-mediated gene addition therapy delivering episomal B‑domain deleted FVIII expression in hepatocytes (samelsonjones2024roctaviangenetherapy pages 1-2).

**Regulatory approvals (supported in retrieved sources).** Roctavian is described as the first licensed HA gene therapy, conditionally approved in Europe (Aug 2022) and approved in the United States (Jun 2023) (samelsonjones2024roctaviangenetherapy pages 1-2).

**Trial landscape (NCT).** A Blood Advances review lists Roctavian clinical trials including NCT02576795, NCT03520712, NCT04684940, NCT03370913, NCT03392974, and NCT04323098 (samelsonjones2024roctaviangenetherapy pages 1-2). Table 1 in that review summarizes these trials and reports key outcomes (samelsonjones2024roctaviangenetherapy media 699684eb).

**Key safety statistic.** In a summarized dataset (GENEr8‑3), >90% of participants experienced ALT elevations, and FVIII was ~14.5% at year 1 (samelsonjones2024roctaviangenetherapy pages 1-2, samelsonjones2024roctaviangenetherapy media 699684eb).

**Comparative effectiveness (2024).** A post hoc comparative effectiveness analysis leveraging an external control cohort reports substantially lower ABRs in the gene‑therapy cohort versus FVIII prophylaxis (treated ABR 0.85 vs 4.40; all‑bleed ABR 1.54 vs 5.01) and higher proportions achieving zero bleeds (zero treated bleeds 82.1% vs 32.9%; zero all bleeds 58.0% vs 28.5%) (liu2024comparativeeffectivenessof pages 14-15).

**Evidence strength.** High for approval timing and mechanistic class; moderate-to-high for safety/efficacy statistics as these are extracted from review summaries and a comparative analysis rather than the original pivotal NEJM report (not retrievable in this run).

### 3) Investigational and pipeline drugs (with trial identifiers)

A major 2024 synthesis highlights “rebalancing” agents (fitusiran, concizumab, marstacimab, SerpinPC) and next-generation FVIII mimetics (Mim8) as active areas of clinical development (mancuso2024benefitsandrisks pages 2-3, wu2024advancesinbiopharmaceutical pages 10-11).

Key pipeline programs with ClinicalTrials.gov identifiers include:

* **Fitusiran** (antithrombin-lowering siRNA): phase 3 inhibitor and non-inhibitor studies NCT03417102 and NCT03417245; additional phase 3 trials NCT03549871 and long-term extension NCT03754790; pediatric NCT03974113; post‑emicizumab switch phase 4 NCT06145373 (wu2024advancesinbiopharmaceutical pages 10-11).
* **Concizumab** (anti‑TFPI): phase 3 programs NCT04082429 (without inhibitors), NCT04083781 (with inhibitors), and NCT05135559; earlier phase 2 NCT03196284/NCT03196297 (wu2024advancesinbiopharmaceutical pages 7-8).
* **Marstacimab** (anti‑TFPI): phase 3 pediatric BASIS KIDS NCT05611801 and phase 3 open-label extension NCT05145127; transition/switch study NCT06703606 (NCT05611801 chunk 1, NCT05145127 chunk 1).
* **Mim8** (FVIII mimetic bispecific): phase 3 program includes NCT05053139, NCT05685238 (FRONTIER 4), NCT05306418 (pediatric), and switching from emicizumab NCT05878938 (wu2024advancesinbiopharmaceutical pages 7-8).
* **SerpinPC** (APC inhibitor): trials NCT04073498, NCT05789524, NCT05789537 (wu2024advancesinbiopharmaceutical pages 10-11).

Efficacy signals (where explicit in the retrieved evidence) include: concizumab phase 3 median ABR outcomes (see Statistics section) and SerpinPC phase I/IIa median all-bleed ABR 1.0 representing 96% reduction (wu2024advancesinbiopharmaceutical pages 10-11).

### 4) Drug repurposing and off-label use

#### Emicizumab in acquired hemophilia A (AHA)
*Relationship type:* Off-label/repurposed candidate.

An ASH Education Program review (Dec 2023) describes emicizumab—approved for prophylaxis in congenital HA with/without inhibitors—as a potential outpatient prophylaxis option in acquired hemophilia A, where standard acute hemostasis relies on bypassing agents (rFVIIa or aPCC) or recombinant porcine FVIII (rpFVIII) for clinically significant bleeding or invasive procedures (poston2023theroleof pages 1-2). A 2023 Diagnostics review summarizes accumulating case reports/series supporting emicizumab prophylaxis in AHA, emphasizing that the evidence base is largely observational/case-based (zanon2023acquiredhemophiliaa pages 18-19).

**Evidence strength.** Low-to-moderate (case reports/series and expert reviews), suitable for hypothesis-generating and selected clinical contexts rather than universal adoption.

### 5) Contraindications and adverse events of special relevance

#### 5.1 Emicizumab + aPCC: contraindication-like safety limitation
A guideline-style practical guidance document (GTH Haemophilia Board) states thrombotic microangiopathy (TMA) and thrombosis events were associated with concomitant aPCC when **>100 IU/kg** was administered for **>24 hours**, and recommends avoiding this exposure pattern (holstein2020practicalguidanceof pages 3-4). The guidance provides explicit dosing constraints: initial aPCC dose should not exceed 50 U/kg; lower doses (15–25 U/kg) have been used; rFVIIa is recommended as first-line for clinically relevant bleeds in inhibitor patients (holstein2020practicalguidanceof pages 6-7).

**Mechanistic basis.** The same guidance provides a mechanism: aPCC contains FIXa and FX (substrates for emicizumab), promoting excessive thrombin generation when combined (holstein2020practicalguidanceof pages 6-7).

#### 5.2 Thrombosis risk signals in rebalancing therapies
A 2024 iScience review notes that anti‑TFPI antibody **befovacimab** was discontinued after three thrombosis cases, and flags thrombotic risk as a general concern for nonfactor rebalancing agents (wu2024advancesinbiopharmaceutical pages 10-11).

#### 5.3 Gene therapy: liver enzyme elevations
Roctavian development is marked by frequent liver enzyme elevations; one summarized dataset reports **>90%** ALT elevations (samelsonjones2024roctaviangenetherapy pages 1-2, samelsonjones2024roctaviangenetherapy media 699684eb). This is clinically consequential because it affects patient selection and post‑infusion monitoring and may entail immunosuppressive management (details not available in retrieved excerpts).

### 6) Combination therapies and real-world implementation

#### 6.1 Emicizumab + bypassing agents (inhibitor patients)
**Preferred regimen:** emicizumab prophylaxis with **rFVIIa** used for clinically relevant breakthrough bleeds and perioperative coverage (holstein2020practicalguidanceof pages 6-7). 

**Avoid / limit:** emicizumab + **aPCC**, particularly prolonged high-dose exposure (>100 U/kg for >24h), due to thrombosis/TMA risk and mechanistic synergy (holstein2020practicalguidanceof pages 6-7, holstein2020practicalguidanceof pages 3-4).

**Implementation evidence:** perioperative HAVEN summary reports high proportions without perioperative bleeding (86% minor; 80% major) and no thrombotic events in that perioperative window (lowell2024perioperativemanagementof pages 4-5).

#### 6.2 Emicizumab + FVIII concentrates (non-inhibitor patients)
In surgical contexts for persons without inhibitors, FVIII concentrate supplementation may be used; however, emicizumab interferes with aPTT and one‑stage FVIII assays, and a bovine chromogenic FVIII assay is recommended for FVIII monitoring (lowell2024perioperativemanagementof pages 4-5, holstein2020practicalguidanceof pages 6-7).

#### 6.3 Adjunct antifibrinolytics
Guidance supports tranexamic acid use locally or systemically as an adjunct (e.g., with rFVIIa) in relevant situations (holstein2020practicalguidanceof pages 5-6).

#### 6.4 Post–gene therapy supplementation
Perioperative review highlights that recipients of gene therapy may still need traditional factor supplementation for surgery (lowell2024perioperativemanagementof pages 4-5).

### 7) Recent developments and notable statistics (prioritize 2023–2024)

* **Roctavian approvals and evolving evidence base (2024):** Europe conditional approval Aug 2022 and US approval Jun 2023; ongoing concerns about year‑over‑year FVIII expression declines (samelsonjones2024roctaviangenetherapy pages 1-2). ALT elevations are common (>90% in one summarized cohort) (samelsonjones2024roctaviangenetherapy pages 1-2, samelsonjones2024roctaviangenetherapy media 699684eb).
* **Comparative effectiveness (2024):** Gene therapy cohort vs FVIII prophylaxis showed markedly lower ABR and higher zero‑bleed proportions (liu2024comparativeeffectivenessof pages 14-15).
* **Perioperative outcomes on emicizumab (2024 synthesis):** 86% of minor and 80% of major surgeries with no post/intra-operative bleeds under described management approaches; thromboembolism risk concentrates in settings using aPCC (lowell2024perioperativemanagementof pages 4-5).
* **Concizumab ABR signals (2024 review):** explorer7 median ABR 0 (with inhibitors); explorer8 median ABR 1.7 in HA without inhibitors (wu2024advancesinbiopharmaceutical pages 7-8).
* **SerpinPC early-phase signal (2024 review):** phase I/IIa median all‑bleed ABR 1.0 (96% reduction) (wu2024advancesinbiopharmaceutical pages 10-11).

### Evidence gaps and limitations (important for interpretation)
* The evidence retrieved in this run does not include primary FDA/EMA label documents for Hemlibra, Roctavian, or Altuviiio, nor does it include pivotal NEJM XTEND‑1 or GENEr8‑1 full trial publications; therefore, several label-level details (complete indications, contraindications, boxed warnings wording, and granular adverse-event rates) cannot be quoted directly here.
* Legacy FVIII concentrates and bypassing agents (rFVIIa, aPCC) are discussed as classes but not exhaustively enumerated with regulator approvals in the retrieved texts.

### Summary table
The following table consolidates approved and investigational therapies, mechanisms, approval status (as supported), trial identifiers, and key statistics.

| Therapy class | Drug (INN) | Mechanism | Indication context (with/without inhibitors) | Regulatory status (agency/country + year if available) | Key trials (NCT) | Notable efficacy/safety statistics | Key references with URLs and publication dates |
|---|---|---|---|---|---|---|---|
| **Approved therapies** |  |  |  |  |  |  |  |
| Non-factor FVIII mimetic | Emicizumab | Bispecific monoclonal antibody bridging FIXa and FX; functions as an FVIIIa mimetic and “corrects defects in the coagulation process without replacing the missing protein” (lowell2024perioperativemanagementof pages 4-5) | Hemophilia A prophylaxis in people with and without inhibitors; not for acute bleed treatment alone (lowell2024perioperativemanagementof pages 4-5, peyvandi2024exploringnonreplacementtherapies’ pages 1-2) | FDA-approved nonfactor product for bleed prophylaxis; broader approval timing not specified in these contexts (lowell2024perioperativemanagementof pages 4-5) | HAVEN 1-4; perioperative HAVEN analyses; STASEY NCT03191799 mentioned in related source set, but only HAVEN data are quantified in the cited contexts (lowell2024perioperativemanagementof pages 4-5) | HAVEN perioperative summary: 215 minor and 18 major surgeries; 86% of minor procedures without additional prophylaxis had no postoperative bleeds; 80% of major surgeries had no intra/postoperative bleeds; thromboembolism reported with concomitant aPCC in 0.5%–5.4%; no thrombotic events seen when aPCC was kept **<100 IU/kg/day**; assay interference with aPTT/one-stage FVIII can persist up to 6 months (lowell2024perioperativemanagementof pages 4-5) | Lowell et al., *Current Anesthesiology Reports*, Jun 2024, https://doi.org/10.1007/s40140-024-00635-y (lowell2024perioperativemanagementof pages 4-5); Peyvandi et al., *RPTH*, May 2024, https://doi.org/10.1016/j.rpth.2024.102434 (peyvandi2024exploringnonreplacementtherapies’ pages 1-2) |
| Extended half-life FVIII replacement | Efanesoctocog alfa (Altuviiio) | Recombinant FVIII Fc-VWF-XTEN fusion protein; VWF-independent FVIII replacement with prolonged half-life/clearance independent of endogenous VWF (keam2023efanesoctocogalfafirst pages 5-6) | Inherited hemophilia A; sources describe adults and children, generally without specifying inhibitor use (keam2023efanesoctocogalfafirst pages 5-6) | FDA approval on **23 Feb 2023**; approved in the US in 2023 and in Japan in 2023; EMA review was ongoing at time of cited review (keam2023efanesoctocogalfafirst pages 5-6) | XTEND-1 **NCT04161495**; XTEND-ed **NCT04644575** (keam2023efanesoctocogalfafirst pages 5-6) | Source set does not provide numeric ABR/zero-bleed data in extract, but notes once-weekly prophylaxis effective in phase III XTEND-1 and identifies agent as new VWF-independent class of FVIII replacement (keam2023efanesoctocogalfafirst pages 5-6) | Keam, *Drugs*, Apr 2023, https://doi.org/10.1007/s40265-023-01866-9 (keam2023efanesoctocogalfafirst pages 5-6) |
| AAV gene therapy | Valoctocogene roxaparvovec (Roctavian) | AAV-mediated gene addition delivering episomal B-domain-deleted FVIII expression in hepatocytes (samelsonjones2024roctaviangenetherapy pages 1-2) | Severe hemophilia A; cited development/approval context focused on adults without inhibitors in pivotal studies (samelsonjones2024roctaviangenetherapy pages 1-2) | Conditionally approved in **Europe (Aug 2022)**; approved in the **United States (Jun 2023)**; described as the first licensed hemophilia A gene therapy (samelsonjones2024roctaviangenetherapy pages 1-2) | **NCT02576795, NCT03520712, NCT04684940, NCT03370913, NCT03392974, NCT04323098** (samelsonjones2024roctaviangenetherapy pages 1-2) | GENEr8-3/related dataset: **>90%** had ALT elevations; FVIII about **14.5% at year 1** in one cohort (samelsonjones2024roctaviangenetherapy pages 1-2). Comparative effectiveness analysis vs FVIII prophylaxis: mean treated ABR **4.40 vs 0.85**, all-bleed ABR **5.01 vs 1.54**, zero treated bleeds **82.1% vs 32.9%**, zero all bleeds **58.0% vs 28.5%** for gene therapy vs FVIII prophylaxis cohorts (liu2024comparativeeffectivenessof pages 14-15) | Samelson-Jones et al., *Blood Advances*, Oct 2024, https://doi.org/10.1182/bloodadvances.2023011847 (samelsonjones2024roctaviangenetherapy pages 1-2); Oldenburg et al., *Advances in Therapy*, Apr 2024, https://doi.org/10.1007/s12325-024-02834-9 (liu2024comparativeeffectivenessof pages 14-15) |
| Conventional factor replacement / bypassing therapy | FVIII concentrates; rFVIIa; aPCC | Replace missing FVIII or bypass FVIII-dependent coagulation steps; used for on-demand, perioperative support, and inhibitor settings (lowell2024perioperativemanagementof pages 4-5, peyvandi2024exploringnonreplacementtherapies’ pages 1-2) | With and without inhibitors depending on product; rFVIIa and aPCC mainly relevant in inhibitor patients (lowell2024perioperativemanagementof pages 4-5) | Longstanding standard-of-care classes; specific agency/date details not provided in these contexts (lowell2024perioperativemanagementof pages 4-5, peyvandi2024exploringnonreplacementtherapies’ pages 1-2) | Not specified in provided contexts | Key safety interaction: with emicizumab, **aPCC** is associated with thrombotic microangiopathy/thromboembolism risk, whereas **rFVIIa** is generally preferred in this setting (lowell2024perioperativemanagementof pages 4-5) | Lowell et al., *Current Anesthesiology Reports*, Jun 2024, https://doi.org/10.1007/s40140-024-00635-y (lowell2024perioperativemanagementof pages 4-5); Peyvandi et al., *RPTH*, May 2024, https://doi.org/10.1016/j.rpth.2024.102434 (peyvandi2024exploringnonreplacementtherapies’ pages 1-2) |
| **Investigational / pipeline therapies** |  |  |  |  |  |  |  |
| Antithrombin-lowering siRNA (rebalancing therapy) | Fitusiran | Small interfering RNA that lowers antithrombin to increase thrombin generation (wu2024advancesinbiopharmaceutical pages 10-11, lowell2024perioperativemanagementof pages 4-5) | Hemophilia A or B, with and without inhibitors; monthly SC prophylaxis (lowell2024perioperativemanagementof pages 4-5) | Investigational; phase III program and later studies ongoing/recruiting (wu2024advancesinbiopharmaceutical pages 10-11) | **NCT03417102, NCT03417245, NCT03549871, NCT03754790, NCT03974113, NCT05662319, NCT06145373** (wu2024advancesinbiopharmaceutical pages 10-11) | Phase III prophylaxis reported significant ABR reductions in the cited review set; safety concern includes thrombosis, including a fatal sinus thrombosis during earlier program with concomitant high-dose FVIII, prompting major risk-mitigation emphasis (wu2024advancesinbiopharmaceutical pages 10-11, lowell2024perioperativemanagementof pages 4-5) | Wu et al., *iScience*, Dec 2024, https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 10-11); Lowell et al., *Current Anesthesiology Reports*, Jun 2024, https://doi.org/10.1007/s40140-024-00635-y (lowell2024perioperativemanagementof pages 4-5) |
| Anti-TFPI monoclonal antibody | Concizumab | Anti-TFPI antibody that rebalances coagulation by inhibiting tissue factor pathway inhibitor (wu2024advancesinbiopharmaceutical pages 7-8, peyvandi2024exploringnonreplacementtherapies’ pages 1-2) | Hemophilia A or B with and without inhibitors (wu2024advancesinbiopharmaceutical pages 7-8) | Approved in **Canada in 2023** for inhibitor population per cited source; phase III studies active for broader use (wu2024advancesinbiopharmaceutical pages 7-8) | **NCT03196284, NCT03196297, NCT04082429, NCT04083781, NCT05135559** (wu2024advancesinbiopharmaceutical pages 7-8) | explorer7 (with inhibitors): median ABR **0**; explorer8 (without inhibitors): median ABR **1.7** in hemophilia A and **2.8** in hemophilia B (wu2024advancesinbiopharmaceutical pages 7-8). Patient-reported outcomes favored concizumab vs no prophylaxis in explorer7 (wu2024advancesinbiopharmaceutical pages 7-8) | Wu et al., *iScience*, Dec 2024, https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 7-8); Tran et al., *RPTH*, May 2024, https://doi.org/10.1016/j.rpth.2024.102476 (wu2024advancesinbiopharmaceutical pages 7-8) |
| Anti-TFPI monoclonal antibody | Marstacimab | Anti-TFPI agent given SC once weekly (NCT05611801 chunk 1, NCT05145127 chunk 1) | Severe hemophilia A/B with or without inhibitors; pediatric and extension studies include inhibitor and non-inhibitor cohorts (NCT05611801 chunk 1, NCT05145127 chunk 1) | Investigational in cited contexts; phase III pediatric and open-label extension recruiting (NCT05611801 chunk 1, NCT05145127 chunk 1) | **NCT05611801, NCT05145127, NCT06703606, NCT02974855** (NCT05611801 chunk 1, NCT05145127 chunk 1) | Safety monitoring explicitly includes thrombotic events, TMA, and DIC/consumption coagulopathy; efficacy endpoint is ABR, but no numeric ABR results are given in the cited contexts (NCT05611801 chunk 1, NCT05145127 chunk 1) | ClinicalTrials.gov summaries for NCT05611801 and NCT05145127 (NCT05611801 chunk 1, NCT05145127 chunk 1) |
| FVIII-mimetic bispecific antibody | Mim8 | Next-generation bispecific FVIII mimetic; does not cross-react with anti-emicizumab antibodies (wu2024advancesinbiopharmaceutical pages 7-8) | Hemophilia A with and without inhibitors (wu2024advancesinbiopharmaceutical pages 7-8) | Investigational; phase III program active/completed (wu2024advancesinbiopharmaceutical pages 7-8) | **NCT04204408, NCT05053139, NCT05306418, NCT05685238, NCT05878938** (wu2024advancesinbiopharmaceutical pages 7-8) | FRONTIER 2 described as positive in cited review; no quantitative ABR values included in provided context extract (wu2024advancesinbiopharmaceutical pages 7-8) | Wu et al., *iScience*, Dec 2024, https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 7-8) |
| Activated protein C inhibition / rebalancing therapy | SerpinPC | Recombinant serine protease inhibitor that reduces APC activity, thereby enhancing thrombin generation (wu2024advancesinbiopharmaceutical pages 10-11) | Hemophilia A/B development context (wu2024advancesinbiopharmaceutical pages 10-11) | Investigational; multiple trials ongoing per cited review (wu2024advancesinbiopharmaceutical pages 10-11) | **NCT04073498, NCT05789524, NCT05789537** (wu2024advancesinbiopharmaceutical pages 10-11) | Phase I/IIa: median all-bleed ABR **1.0**, representing **96% reduction** in cited source; positioned as a promising rebalancing approach (wu2024advancesinbiopharmaceutical pages 10-11) | Wu et al., *iScience*, Dec 2024, https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 10-11) |
| Anti-TFPI / other rebalancing antibodies | Befovacimab; MG1113; KN057; BAY1093884 | Rebalancing via TFPI inhibition or related anticoagulant-pathway modulation (wu2024advancesinbiopharmaceutical pages 10-11) | Hemophilia A/B investigational context (wu2024advancesinbiopharmaceutical pages 10-11) | Investigational; some programs early phase or terminated (wu2024advancesinbiopharmaceutical pages 10-11) | **NCT05493631, NCT03855696, NCT06747416, NCT03597022** (wu2024advancesinbiopharmaceutical pages 10-11) | Important safety signal: **befovacimab** development discontinued after **3 thrombosis cases** (wu2024advancesinbiopharmaceutical pages 10-11) | Wu et al., *iScience*, Dec 2024, https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 10-11) |


*Table: This table summarizes key approved and investigational therapies for hemophilia A using only the cited context IDs. It highlights mechanisms, indication context, regulatory status, major trial identifiers, and the most salient efficacy and safety signals available from the evidence set.*

### Selected key sources (URLs; publication dates)
* Roctavian gene therapy review: Blood Advances, Oct 2024 — https://doi.org/10.1182/bloodadvances.2023011847 (samelsonjones2024roctaviangenetherapy pages 1-2)
* Efanesoctocog alfa first approval: Drugs, Apr 2023 — https://doi.org/10.1007/s40265-023-01866-9 (keam2023efanesoctocogalfafirst pages 5-6)
* Perioperative management review (emicizumab, factor supplementation): Current Anesthesiology Reports, Jun 2024 — https://doi.org/10.1007/s40140-024-00635-y (lowell2024perioperativemanagementof pages 4-5)
* Benefits/risks of nonfactor therapies (emicizumab, rebalancing agents): Haemophilia, Mar 2024 — https://doi.org/10.1111/hae.14976 (mancuso2024benefitsandrisks pages 2-3)
* Pipeline overview and safety signals (befovacimab thrombosis; SerpinPC ABR signal): iScience, Dec 2024 — https://doi.org/10.1016/j.isci.2024.111436 (wu2024advancesinbiopharmaceutical pages 10-11)
* Emicizumab in acquired hemophilia A (off-label): ASH Education Program, Dec 2023 — https://doi.org/10.1182/hematology.2023000462 (poston2023theroleof pages 1-2)


References

1. (samelsonjones2024roctaviangenetherapy pages 1-2): Benjamin J. Samelson-Jones, Juliana C. Small, and Lindsey A. George. Roctavian gene therapy for hemophilia a. Blood Advances, 8:5179-5189, Oct 2024. URL: https://doi.org/10.1182/bloodadvances.2023011847, doi:10.1182/bloodadvances.2023011847. This article has 35 citations and is from a peer-reviewed journal.

2. (lowell2024perioperativemanagementof pages 4-5): Aliena E. Lowell, Michael P. Calgi, Joseph J. Caruso, Louise M. Man, and John S. McNeil. Perioperative management of hemophilia patients. Current Anesthesiology Reports, 14:354-365, Jun 2024. URL: https://doi.org/10.1007/s40140-024-00635-y, doi:10.1007/s40140-024-00635-y. This article has 8 citations and is from a peer-reviewed journal.

3. (peyvandi2024exploringnonreplacementtherapies’ pages 1-2): Flora Peyvandi, Omid Seidizadeh, Samin Mohsenian, and Isabella Garagiola. Exploring nonreplacement therapies’ impact on hemophilia and other rare bleeding disorders. Research and Practice in Thrombosis and Haemostasis, 8:102434, May 2024. URL: https://doi.org/10.1016/j.rpth.2024.102434, doi:10.1016/j.rpth.2024.102434. This article has 16 citations and is from a peer-reviewed journal.

4. (wu2024advancesinbiopharmaceutical pages 10-11): Junzheng Wu, Xiaoling Liu, Huichuan Yang, Yanlin He, and Ding Yu. Advances in biopharmaceutical products for hemophilia. iScience, 27:111436, Dec 2024. URL: https://doi.org/10.1016/j.isci.2024.111436, doi:10.1016/j.isci.2024.111436. This article has 4 citations and is from a peer-reviewed journal.

5. (wu2024advancesinbiopharmaceutical pages 7-8): Junzheng Wu, Xiaoling Liu, Huichuan Yang, Yanlin He, and Ding Yu. Advances in biopharmaceutical products for hemophilia. iScience, 27:111436, Dec 2024. URL: https://doi.org/10.1016/j.isci.2024.111436, doi:10.1016/j.isci.2024.111436. This article has 4 citations and is from a peer-reviewed journal.

6. (keam2023efanesoctocogalfafirst pages 5-6): Susan J. Keam. Efanesoctocog alfa: first approval. Drugs, 83:633-638, Apr 2023. URL: https://doi.org/10.1007/s40265-023-01866-9, doi:10.1007/s40265-023-01866-9. This article has 28 citations and is from a domain leading peer-reviewed journal.

7. (samelsonjones2024roctaviangenetherapy media 699684eb): Benjamin J. Samelson-Jones, Juliana C. Small, and Lindsey A. George. Roctavian gene therapy for hemophilia a. Blood Advances, 8:5179-5189, Oct 2024. URL: https://doi.org/10.1182/bloodadvances.2023011847, doi:10.1182/bloodadvances.2023011847. This article has 35 citations and is from a peer-reviewed journal.

8. (liu2024comparativeeffectivenessof pages 14-15): JOHCH Liu, C Hawes, and XYXYV Newman. Comparative effectiveness of valoctocogene roxaparvovec and prophylactic factorviii replacement in severe hemophiliaa. Unknown journal, 2024.

9. (mancuso2024benefitsandrisks pages 2-3): Maria Elisa Mancuso, Stacy E. Croteau, and Robert Klamroth. Benefits and risks of non‐factor therapies: redefining haemophilia treatment goals in the era of new technologies. Haemophilia, 30:39-44, Mar 2024. URL: https://doi.org/10.1111/hae.14976, doi:10.1111/hae.14976. This article has 34 citations and is from a peer-reviewed journal.

10. (NCT05611801 chunk 1):  A Clinical Trial of Study Medicine (Marstacimab) in Pediatric Patients With Hemophilia A or Hemophilia B. Pfizer. 2022. ClinicalTrials.gov Identifier: NCT05611801

11. (NCT05145127 chunk 1):  Open-Label Extension Study of Marstacimab in Hemophilia Participants With or Without Inhibitors. Pfizer. 2021. ClinicalTrials.gov Identifier: NCT05145127

12. (poston2023theroleof pages 1-2): Jacqueline N Poston and Rebecca Kruse-Jarres. The role of emicizumab in acquired hemophilia a. Hematology. American Society of Hematology. Education Program, 2023 1:24-30, Dec 2023. URL: https://doi.org/10.1182/hematology.2023000462, doi:10.1182/hematology.2023000462. This article has 9 citations.

13. (zanon2023acquiredhemophiliaa pages 18-19): Ezio Zanon. Acquired hemophilia a: an update on the etiopathogenesis, diagnosis, and treatment. Diagnostics, 13:420, Jan 2023. URL: https://doi.org/10.3390/diagnostics13030420, doi:10.3390/diagnostics13030420. This article has 45 citations.

14. (holstein2020practicalguidanceof pages 3-4): Katharina Holstein, Manuela Albisetti, Christoph Bidlingmaier, Susan Halimeh, Sabine Heine, Robert Klamroth, Christoph Königs, Karin Kurnik, Christoph Male, Johannes Oldenburg, Werner Streif, Cornelia Wermes, and Carmen Escuriola-Ettingshausen. Practical guidance of the gth haemophilia board on the use of emicizumab in patients with haemophilia a. Hämostaseologie, 40:561-571, Jun 2020. URL: https://doi.org/10.1055/a-1127-6476, doi:10.1055/a-1127-6476. This article has 32 citations.

15. (holstein2020practicalguidanceof pages 6-7): Katharina Holstein, Manuela Albisetti, Christoph Bidlingmaier, Susan Halimeh, Sabine Heine, Robert Klamroth, Christoph Königs, Karin Kurnik, Christoph Male, Johannes Oldenburg, Werner Streif, Cornelia Wermes, and Carmen Escuriola-Ettingshausen. Practical guidance of the gth haemophilia board on the use of emicizumab in patients with haemophilia a. Hämostaseologie, 40:561-571, Jun 2020. URL: https://doi.org/10.1055/a-1127-6476, doi:10.1055/a-1127-6476. This article has 32 citations.

16. (holstein2020practicalguidanceof pages 5-6): Katharina Holstein, Manuela Albisetti, Christoph Bidlingmaier, Susan Halimeh, Sabine Heine, Robert Klamroth, Christoph Königs, Karin Kurnik, Christoph Male, Johannes Oldenburg, Werner Streif, Cornelia Wermes, and Carmen Escuriola-Ettingshausen. Practical guidance of the gth haemophilia board on the use of emicizumab in patients with haemophilia a. Hämostaseologie, 40:561-571, Jun 2020. URL: https://doi.org/10.1055/a-1127-6476, doi:10.1055/a-1127-6476. This article has 32 citations.