---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T15:03:23.705757'
end_time: '2026-04-04T15:05:35.620538'
duration_seconds: 131.91
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: facioscapulohumeral muscular dystrophy
  mondo_id: MONDO:0001347
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 50
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** facioscapulohumeral muscular dystrophy
- **MONDO ID:** MONDO:0001347

## Research Objectives

Please provide a comprehensive research report on **drug therapies for facioscapulohumeral muscular dystrophy**.
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

# Comprehensive Research Report on Drug Therapies for Facioscapulohumeral Muscular Dystrophy

Facioscapulohumeral muscular dystrophy (FSHD) represents one of the most common hereditary muscle disorders, affecting an estimated 30,000 to 87,000 individuals across the United States and European Union, yet it has long remained without any disease-modifying pharmacological treatment[1][1]. This comprehensive research report examines the current therapeutic landscape for FSHD, encompassing approved medications, investigational compounds in clinical development, repurposed drug candidates, contraindicated agents, and emerging combination therapy approaches as of April 2026. The field has undergone dramatic transformation in recent years, with multiple disease-modifying therapies now advancing through clinical trials following decades during which symptomatic management represented the only available pharmaceutical option. This report synthesizes recent developments in the FSHD drug development pipeline, providing detailed analysis of therapeutic mechanisms, clinical trial progress, regulatory designations, and the evidence supporting various treatment approaches.

## Current Status of Approved Therapies for FSHD

### Absence of Disease-Modifying Approved Treatments

As of April 2026, there are no FDA-approved disease-modifying therapies specifically indicated for the treatment of FSHD[8][16]. This represents a significant gap in the treatment landscape, as FSHD remains characterized by progressive, relentless muscle degeneration for which only supportive care and symptomatic management have been available to patients. The disease causes weakness in facial, shoulder, and lower limb muscles in an asymmetric pattern, often accompanied by pain, fatigue, and respiratory complications in severe cases[8]. The prolonged absence of approved treatments has driven extensive clinical research and drug development efforts, but the complexity of FSHD pathophysiology has created substantial barriers to therapeutic success. The underlying genetic basis of FSHD involves epigenetic derepression of the double homeobox 4 (DUX4) gene, which normally remains silent in somatic tissues but becomes aberrantly expressed in skeletal muscle in both FSHD1 and FSHD2[1][8]. This toxic gain-of-function mechanism has proven uniquely challenging to target therapeutically compared to the loss-of-function pathology underlying other muscular dystrophies.

### Limited Efficacy of Previously Investigated Agents

Historical clinical trials have established that several pharmacological agents previously investigated for potential benefit in FSHD lack efficacy for improving muscle strength or slowing disease progression[8][16]. Corticosteroids, including prednisone and deflazacort, have been extensively studied but show no meaningful benefit specifically for FSHD, though they remain the standard of care for Duchenne muscular dystrophy (DMD)[8]. Multiple randomized controlled trials have demonstrated that albuterol, a beta-2 adrenergic agonist, produces no significant improvement in muscle strength in FSHD patients despite initial theoretical promise[16][16]. Creatine supplementation, while showing modest benefits across certain muscular dystrophies, failed to demonstrate statistically significant benefit in FSHD populations specifically[16][16]. The myostatin inhibitor MYO-029, which showed theoretical promise for muscle growth stimulation, was evaluated in a Phase 1/2 trial including FSHD patients but showed no improvements in exploratory endpoints of muscle strength or function, despite evidence of biological activity through increased muscle mass on histology[17]. These consistent failures across diverse therapeutic approaches highlight the particular challenges in treating this complex genetic disease and have informed the current focus on targeting DUX4 directly.

## Investigational Drugs in Clinical Development

### DUX4-Targeting Oligonucleotide Therapies

#### Del-brax (Delpacibart Braxlosiran) from Avidity Biosciences

Del-brax represents one of the most advanced investigational therapies for FSHD, currently in Phase 1/2 clinical development as part of the FORTITUDE trial[4][43][46][50]. This agent is an antibody oligonucleotide conjugate (AOC) composed of a monoclonal antibody targeting transferrin receptor 1 conjugated with a small interfering RNA (siRNA) designed to degrade DUX4 mRNA[39][43]. The FORTITUDE trial enrolled 90 adult participants with FSHD who received either 2 mg/kg or 4 mg/kg of del-brax administered intravenously every six weeks initially, then every thirteen weeks[4][46]. Avidity has completed enrollment in the dose escalation cohorts and identified 2 mg/kg every six weeks as the recommended dose for future studies[4]. The third biomarker cohort of the FORTITUDE trial, which enrolled 51 participants and has completed enrollment, was specifically designed to support a potential accelerated approval pathway in the United States[4][46].

The most recent topline results from the FORTITUDE Phase 1/2 trial, presented in June 2025, demonstrated unprecedented safety and efficacy signals[43][46][50]. Del-brax treatment resulted in greater than 50% reduction in DUX4-regulated genes in muscle, with a rapid and consistent decline in the novel circulating biomarker KHDC1L, which showed baseline elevation of 6 to 9-fold higher in FSHD patients compared to healthy controls[43][50]. Creatine kinase levels, a marker of muscle damage, also demonstrated consistent decline in treated participants compared to placebo[43][50]. Importantly, exploratory functional measures showed trends of improvement, with del-brax treated participants demonstrating improvements in the 10-meter walk-run test, timed-up-and-go test, and quantitative muscle strength testing compared to placebo[43][50]. Patient-reported outcomes and quality of life measures also showed improvements aligned with functional gains[43][50]. Safety data were notably encouraging, with no serious adverse events attributable to del-brax reported, no treatment discontinuations due to adverse events, and most reported adverse events being mild to moderate in severity[43][50]. The FDA has granted Fast Track designation and opened an accelerated approval pathway for del-brax[43][50].

In response to these positive Phase 1/2 results, Avidity initiated the global Phase 3 FORWARD trial involving approximately 200 participants across North America, Europe, and Japan, which will formally assess efficacy over 18 months focusing on functional mobility and muscle strength[43][50]. This trial represents a critical next step in establishing del-brax as a potential first approved therapy directly targeting the DUX4 pathology underlying FSHD. The identification of the KHDC1L circulating biomarker has particular significance, as it could potentially accelerate access to treatment by up to two years through enabling earlier regulatory decisions[43].

#### EPI-321 from Epicrispr Biotechnologies

EPI-321 represents a fundamentally different approach to DUX4 silencing, utilizing epigenetic modulation rather than RNA interference[2][45][49]. This investigational therapy consists of a one-time gene-modulating treatment delivered via an adeno-associated virus (AAV) vector that re-methylates the D4Z4 array in skeletal muscle, restoring epigenetic silencing of DUX4[2][49]. The disease mechanism in FSHD involves hypomethylation of the D4Z4 array, often driven by array contraction or genetic deletions in the region, which allows aberrant DUX4 expression[49]. EPI-321 works by reversing this epigenetic derepression through re-methylation of the D4Z4 region in muscle tissue[49].

Preclinical data collected from both in vitro culture systems and in vivo animal models demonstrated strong efficacy of EPI-321[49]. Studies showed robust suppression of the DUX4 transcript and re-methylation of the D4Z4 array in treated muscle tissue[2][49]. Additionally, treatment with EPI-321 resulted in improved muscle contractility and strong reduction in muscle cell death compared to untreated controls[49]. In September 2024, Epicrispr Biotechnologies announced that it had secured $68 million in Series B financing and received FDA clearance for the Investigational New Drug (IND) application for EPI-321[2]. The company subsequently obtained regulatory approvals from New Zealand's Medsafe regulatory agency for a first-in-human trial[2]. In August 2025, Epicrispr announced that the first patient had been dosed in its global first-in-human clinical trial of EPI-321[45]. The trial is evaluating safety, tolerability, and pharmacodynamics of a single intravenous dose in adults with genetically confirmed FSHD, with initial data expected in early 2026[45].

EPI-321 has received multiple FDA designations including Fast Track, Rare Pediatric Disease, and Orphan Drug status, highlighting the agency's recognition of its potential to address the significant unmet medical need in FSHD[2][45]. The potential advantage of a single intravenous administration delivering potentially durable or permanent silencing of DUX4 through epigenetic reprogramming distinguishes EPI-321 from repeat-dosing RNA interference approaches[2][45].

#### ARO-DUX4 (SRP-1001) from Arrowhead Research

ARO-DUX4, also known as SRP-1001, represents an RNA interference-based therapy designed to target DUX4 mRNA directly[44]. This investigational siRNA therapeutic is being studied in a Phase 1/2 clinical trial in adult and adolescent participants with FSHD[14][14][14][44]. The mechanism involves systemically administered unconjugated antisense oligonucleotides targeting DUX4 that have demonstrated promising preclinical activity, including improvement in muscular injury and motor function in FSHD model mice[20]. ARO-DUX4 received FDA Fast Track designation, and clinical trial recruitment information has been available through ClinicalTrials.gov[14]. The therapy represents part of the expanding pipeline of oligonucleotide therapeutics specifically targeting DUX4 expression in FSHD[20][44].

### Anti-Myostatin Biological Therapies

#### Apitegromab (SRK-015) from Scholar Rock

Apitegromab is a fully human monoclonal antibody that selectively inhibits the activation of myostatin, a negative regulator of skeletal muscle growth[15][38][47]. Unlike previous myostatin inhibitors that bind to the mature, active form of myostatin and showed limited efficacy or concerning safety profiles, apitegromab specifically targets the proforms of myostatin—promyostatin and latent myostatin—thereby preventing their activation to the mature growth factor[38][42]. This selective targeting was designed to address the lack of specificity and potential off-target effects observed with previous antimyostatin therapies[38]. Preclinical pharmacology studies confirmed the ability of apitegromab to inhibit the activation of promyostatin through blocking the proteolytic cleavage that releases mature myostatin[38]. In vitro assays measured apitegromab's ability to block proteolytic cleavage across species, demonstrating inhibition of human myostatin activation (IC50 = 286 nM), cynomolgus myostatin (IC50 = 626 nM), and rat myostatin (IC50 = 178 nM)[38].

Apitegromab is primarily in Phase 3 clinical development for spinal muscular atrophy (SMA), where it has demonstrated clinical proof-of-concept for improving motor function in patients receiving concomitant SMN-targeted therapy[15][42]. However, Scholar Rock expanded development of apitegromab to include FSHD following preclinical evidence in a mouse model of FSHD showing that the murine analog of apitegromab significantly increased skeletal muscle mass, muscle force, and endurance[47]. In January 2026, Scholar Rock announced that the IND application for apitegromab in FSHD had been cleared by the FDA, and the company plans to initiate a Phase 2 randomized, double-blind, placebo-controlled trial called FORGE in mid-2026[15][47]. The FORGE trial is designed to assess the efficacy, pharmacokinetics, pharmacodynamics, safety, and tolerability of apitegromab 10 mg/kg administered intravenously every four weeks in approximately 60 ambulatory adult patients aged 18-60 years with genetically confirmed FSHD (FSHD1 or FSHD2) and a Ricci severity score of 1.5-3.0[47]. The primary endpoint will be percent change from baseline in total lean muscle volume by MRI at week 52[47].

Scholar Rock has completed a subcutaneous formulation study of apitegromab in healthy volunteers, demonstrating favorable bioavailability and a pharmacodynamic profile comparable to intravenous administration[15]. This subcutaneous development could provide patients with a small volume, self-administered or caregiver-administered treatment option using an autoinjector[15].

#### Emugrobart (RO7204239) from Hoffmann-La Roche

Emugrobart, designated as GYM329 or RO7204239 in development, is a humanized monoclonal antibody targeting myostatin designed to promote muscle growth. The drug was evaluated in the Phase 2 MANOEUVRE trial in adults with FSHD. Despite demonstrating effective suppression of myostatin activity in study participants—indicating that the drug successfully engaged its biological target—Emugrobart failed to show statistically significant benefit over placebo after 52 weeks of treatment on the primary endpoint of change in quadriceps muscle volume measured by MRI. The trial also assessed change in muscle volume and fat fraction of 36 muscles by whole-body MRI and measured myostatin level modifications, but emugrobart did not consistently deliver the hoped-for improvements in muscle growth and function. Following these disappointing Phase 2 results, Hoffmann-La Roche announced in March 2026 that the company would stop development and not proceed to Phase 3 studies in FSHD with emugrobart. Notably, the trial did include plans for continued development of emugrobart in obesity, suggesting the mechanism may remain relevant for other indications even if ineffective for FSHD.

The failure of emugrobart, despite effective target engagement, reinforces the complexity of developing muscle growth treatments for FSHD and suggests that myostatin inhibition alone may be insufficient to overcome the complex pathology driven by aberrant DUX4 expression.

### Small Molecule DUX4 Inhibitors

#### GBC0905 (Rebastinib) from Myocea

GBC0905 is a small molecule DUX4 inhibitor being developed by Myocea, a biotechnology company that was spun out of Genea Biocells in 2019[30][30]. The compound represents a repurposed drug candidate originally developed for the treatment of solid tumors by another pharmaceutical company and is currently being evaluated in Phase 2 clinical studies[30]. In early screening efforts using various FSHD cell lines, GBC0905 displayed robust anti-DUX4 activity by modulating both toxic gene activation and toxic protein production[30][30]. This activity was recapitulated in a mouse model of FSHD featuring cells transplanted from patients, performed by collaborators at the University of Massachusetts Medical School[30].

The proposed mechanism of GBC0905 appears distinct from losmapimod (a p38 inhibitor in failed trials), as studies established multiple likely mechanisms of DUX4 reduction, including both stabilization of cellular structure and blockade of damage in skeletal muscle cells caused by DUX4 and its gene products[30][30]. Importantly, GBC0905 appears to silence multiple toxic targets triggered by DUX4 activation rather than affecting a single pathway[30][30]. The compound received orphan drug designation from the FDA in May 2018[30]. In the preclinical development stage, Myocea successfully completed dose range-finding studies and toxicity assessments of GBC0905 in rodents, which are required by the FDA before initiating clinical trials[30][30]. Myocea has been conducting additional studies to establish specific dosing and treatment regimens for humans as well as evaluate long-term effects of the molecule on DUX4 and its products[30][30].

Based on the available preclinical evaluation, Myocea believes GBC0905 has potential to become a once-daily oral therapeutic with dosing of less than 50 mg per day that targets the disease via multiple complementary mechanisms and displays robust long-term FSHD-modulating effect in patients[30][30]. Additionally, Myocea is exploring muscle stem cells (satellite cells) as a potential therapeutic modality to treat specific muscles affected by FSHD, though these studies remain at the early proof-of-concept stage[30][30].

#### DX5057 from Altay Therapeutics

DX5057 represents the first and only oral small molecule DUX4 inhibitor currently in development for FSHD[31][31]. The Food and Drug Administration granted Orphan Drug Designation to DX5057 in December 2025, which provides meaningful regulatory incentives including seven years of market exclusivity and exemption from FDA application fees[31][31]. DX5057 directly targets DUX4-driven pathology and has demonstrated strong preclinical activity with a convenient oral route of administration[31][31]. The designation will help accelerate this first-in-kind oral treatment for FSHD through the regulatory approval process[31][31]. Friends of FSH Research, a patient advocacy organization, supported Altay Therapeutics with seed funding in 2021 for the discovery of a novel small molecule inhibitor for DUX4 and again in 2022 for determining in vitro and in vivo efficacy of novel DUX4 inhibitors, enabling the company to develop DX5057 and attract further funding[31][31]. Despite the achievement of Orphan Drug status and the theoretical advantages of an oral formulation, DX5057 still faces the substantial hurdle of transitioning from preclinical and early development into larger clinical trials, with enrollment of sufficient participants representing a significant challenge.

### Inflammatory Modulation Therapies

#### Satralizumab (IL-6 Receptor Antagonist)

Satralizumab is an IL-6 receptor antagonist monoclonal antibody originally developed for other indications that is now being investigated in FSHD based on emerging evidence of inflammatory pathophysiology in the disease[25][44]. Recent research has identified elevated interleukin-6 (IL-6) levels as a potential disease activity biomarker in FSHD, displaying robust correlations with established clinical severity and functional scores[25]. A study that assessed serum cytokines in 100 adult FSHD1 patients found that out of 20 cytokines examined, 10 showed significantly altered expression levels compared to healthy controls of similar age and sex, with FSHD1 patients exhibiting heightened levels of inflammatory cytokines and diminished anti-inflammatory cytokines, signaling chronic inflammation[25]. IL-6 emerged as a particularly promising disease activity biomarker in this analysis[25].

The ReInForce study (NCT06222827) is evaluating satralizumab, an IL-6 receptor antagonist, for its efficacy in specifically reducing muscle and systemic inflammation in FSHD1 patients[25][44]. By antagonizing IL-6R downstream signaling, satralizumab holds promise in mitigating inflammation and potentially curtailing fibrofatty degeneration in FSHD[25]. The trial is structured to evaluate efficacy of satralizumab compared to placebo measured by change in whole-body muscle MRI, including composite scores describing muscle fat infiltration, lean muscle volume, and muscle fat fraction in intermediate muscles[25]. Additional primary efficacy endpoints include change in RICCI clinical severity scale score and reachable workspace results, with muscle strength determined by quantitative isometric dynamometry also being assessed[25]. The trial takes place in Nice, France, and Ottawa, Canada, with evaluations scheduled from baseline to week 96[25][44].

### Combination Hormone Therapy

#### Combined Growth Hormone and Testosterone

Recent research has demonstrated that a combined regimen of recombinant human growth hormone (rHGH) and testosterone represents a novel therapeutic approach with potential clinical benefit in FSHD[36][40]. In a Phase 1/2 investigator-initiated, single-center, single-arm, proof-of-concept study conducted at the University of Rochester, researchers enrolled 20 adult men with FSHD who remained ambulatory[36][40]. Participants received daily injections of recombinant human growth hormone, which helps cells grow and regenerate, combined with testosterone enanthate injections administered every two weeks for 24 weeks, followed by a 12-week washout period to assess durability of effects[36][40].

Safety and tolerability represented the primary study objective[36][40]. Nearly every participant (19 of 20) completed the program without serious adverse events, with most participants reporting only mild soreness at the injection site[36]. By the end of the six-month treatment period, participants had gained an average of approximately 4.5 pounds of lean muscle and lost around 3 pounds of fat, representing meaningful improvements in body composition[36]. In a simple six-minute walking test where patients walk as far as they safely can, participants improved by roughly 37 meters (approximately 120 feet), a distance that would make everyday activities like walking to the mailbox or down a hallway noticeably easier[36]. Muscle strength increased by approximately 3 percent over what was expected for their age and size, and men reported a reduction in their overall disease burden as measured by the FSHD-HI, a clinical trial outcome measure developed at the University of Rochester with extensive patient input[36].

Importantly, many of these gains remained evident three months after participants stopped the hormone injections, suggesting durable benefits beyond the active treatment period[36]. This represents a notable distinction from many FSHD therapeutic approaches, as previous investigators noted they had "never seen a therapy in FSHD deliver both real gains in strength and lasting benefit after treatment stops."[36] The findings suggest that combination therapy could mark the first treatment not only to slow FSHD disease progression but help patients regain function[36]. The University of Rochester team is planning larger, controlled, multi-center, randomized studies to confirm these benefits, fine-tune dosing, and include women with FSHD[36]. Researchers emphasize that this therapy has potential applications across multiple muscular dystrophies beyond FSHD, and given that many companies are pursuing genetic therapies for common muscular dystrophies, this combination therapy could represent a broadly applicable approach for the hundreds of different types of neuromuscular diseases with no effective treatments[36].

## Previously Investigated Failed Therapies

### Losmapimod (p38 Inhibitor)

Losmapimod, a p38 mitogen-activated protein kinase inhibitor originally developed by GlaxoSmithKline for cardiovascular disease treatment but now exclusively in-licensed by Fulcrum Therapeutics, was investigated as a potential FSHD therapy based on the rationale that p38 inhibition reduces DUX4 expression[5][9][22][24][29]. Initial Phase 2 data from the FIS-002-2019 trial (NCT04003974) showed nominal benefit at 48 weeks, with the primary endpoint of reduction in DUX4-regulated gene expression not being met[9][22]. A Phase 3 randomized, double-blind, placebo-controlled trial called REACH (ClinicalTrials.gov ID NCT05397470) was subsequently conducted, enrolling 260 participants across multiple global sites[24][29]. However, the company reported disappointing results, noting that the drug failed to show improvements in several of the tests carried out in the trial over a 48-week period[24]. While safety concerns did not emerge, no improvement was observed in participants who received losmapimod compared to those who received placebo[24]. Following these Phase 3 results, Fulcrum Therapeutics announced that it was suspending further development of losmapimod for FSHD[24].

### ACE-083 (Locally Acting Myostatin Inhibitor)

ACE-083 is a recombinant fusion protein composed of modified human follistatin linked to the human immunoglobulin G2 Fc domain that functions as a ligand trap for the transforming growth factor-beta (TGF-β) superfamily, particularly activins and myostatin, which inhibit skeletal muscle growth and regeneration[11]. The compound was designed as a locally acting agent administered through direct intramuscular injection rather than systemic delivery[11]. In a Phase 2 randomized, double-blind clinical trial, ACE-083 was safe and well tolerated in participants with FSHD, with the most common adverse events being mild or moderate injection-site reactions[11]. The trial evaluated ACE-083 240 mg per muscle versus placebo injected bilaterally every three weeks in either the biceps brachii or tibialis anterior muscles, followed by six months of open-label treatment[11].

While ACE-083 induced statistically significant increases in total muscle volume (TMV) compared to placebo—with 12.0 percent (standard error 4.89) in the biceps brachii group and 9.5 percent (3.2-15.9) in the tibialis anterior group—these increases in muscle volume did not result in consistent functional or patient-reported outcome improvements with up to 12 months of treatment[11]. Specifically, there were no consistent improvements in muscle strength, motor function, or patient-reported outcome measures in either treatment group[11]. Post hoc subgroup analyses suggested more significant improvements in certain subgroups with milder baseline disease, but overall efficacy was insufficient[11]. Based on these Phase 2 results demonstrating muscular hypertrophy without functional benefit, the ACE-083 development program for treatment of FSHD was discontinued[11].

## Contraindicated Medications

### Statins (HMG-CoA Reductase Inhibitors)

While statins are not absolutely contraindicated in patients with FSHD, they require careful monitoring due to an increased risk of statin-associated muscle symptoms (SAMS)[32][33]. Statins are the most widely prescribed drugs in the world and are used to lower blood cholesterol to reduce cardiovascular disease risk, but they can affect muscle function[32][33]. A retrospective analysis of safety and tolerability of statins in genetic myopathies examined 135 patients with various muscular dystrophies, including 22 with FSHD[33]. The study found that SAMS occurred in 36 of 135 patients (26.67%), including 7 of the 22 FSHD patients (approximately 32%)[33]. Myalgias (muscle aches and pains) were the most frequent manifestation of SAMS, occurring in 29 patients[33].

The mechanisms underlying statin-induced muscle toxicity include effects on metabolic processes beyond cholesterol synthesis[32]. Statins block the synthesis of substances other than cholesterol, and this metabolic effect can lead to muscle symptoms[32]. Coenzyme Q10 (CoQ10), an important muscle protein, may be affected by statin synthesis suppression[32]. Additionally, very rarely (probably in fewer than 1 in 10,000 people), statins may cause rhabdomyolysis with widespread muscle pain and weakness due to extensive breakdown of muscle, potentially leading to kidney failure in severe cases[32]. Most importantly, statins can trigger immune-mediated myositis, whereby the body produces antibodies attacking muscle, though this is uncommon[32].

Despite these risks, it is considered wrong to deprive FSHD patients of the potential cardiovascular benefits of statins without attempting a therapeutic trial, as the absolute cardiovascular benefits likely outweigh the muscular risks in many patients[32]. General precautions include monitoring for muscle pain, periodic measurement of creatine kinase levels, and discontinuing or switching statins if unacceptable symptoms or very large persistent increases in creatine kinase develop[32]. The data suggest that SAMS occur at frequencies similar to the general population in common genetic myopathies except in metabolic myopathies and mitochondrial conditions where rhabdomyolysis risk is elevated[33].

### Corticosteroids

Although corticosteroids are the standard of care for Duchenne muscular dystrophy and show clear efficacy in slowing disease progression in that condition, evidence-based reviews conclude that there is no role for corticosteroids in improving strength or slowing disease progression in FSHD specifically[8][16]. Early case reports and clinical observations of inflammatory infiltrates in FSHD muscle biopsies initially prompted investigation of corticosteroid therapy, but systematic reviews of available evidence demonstrate lack of efficacy[8][16]. A prospective 12-week open-label uncontrolled trial of prednisone in FSHD enrolled eight patients who received 1.5 mg/kg per day of prednisone (maximum 80 mg daily) but showed no improvement in muscle strength[16]. Based on this evidence, clinical guidelines recommend that clinicians should not prescribe corticosteroids for improving strength in FSHD patients[8].

## Adverse Event Monitoring and Management

### Pain Management in FSHD

Pain is a common complaint in FSHD affecting up to 79 percent of patients, with the most common sites being the lower back, legs, shoulders, and neck[8]. Pain appears to be mostly musculoskeletal in origin and can compound muscle weakness to have significant impact on quality of life[8]. The frequency of clinically significant pain was noted at approximately 10.8 percent of FSHD patients[8]. Clinical guidelines recommend that treating physicians should routinely inquire about pain in patients with FSHD[8]. Referral for physical therapy evaluation may prove helpful as an initial nonpharmacologic intervention[8]. In patients with persistent pain and no contraindications, a trial of nonsteroidal anti-inflammatory medications is appropriate for acute pain, while antidepressants or antiepileptic agents may be considered for chronic pain[8].

### Monitoring for Extramuscular Manifestations

Beyond muscle weakness, FSHD can involve multiple extramuscular complications that require monitoring[8]. Respiratory insufficiency and reduced pulmonary function may occur with estimated frequencies ranging from 1.25 percent to 13 percent depending on the study population[8]. Retinal vascular disease has been documented in up to 25 percent of FSHD patients, with 0.6 percent experiencing symptomatic retinal disease that can rarely lead to exudative retinopathy and visual loss[8]. Hearing loss has also been reported as an extramuscular manifestation[8]. There is possibly an increased incidence of cardiac arrhythmias in FSHD, though data are limited[8]. Large D4Z4 deletion sizes (contracted D4Z4 allele of 10-20 kb) should alert clinicians that patients are more likely to develop more significant disability at earlier ages and are more likely to develop symptomatic extramuscular manifestations[8].

## Biomarker Development for Treatment Monitoring

### DUX4-Regulated Gene Biomarkers

The identification of circulating biomarkers for DUX4 activity has advanced significantly, enabling non-invasive monitoring of disease activity and treatment effects[27]. Researchers have identified that certain DUX4 target genes show correlation between their expression levels in muscle tissue and in peripheral blood mononuclear cells (PBMCs)[27]. A refined 143-gene PAX7 target gene signature (termed the "FSHD muscle-blood biomarker") in PBMCs was shown to correlate with widely used FSHD clinical severity scores, offering promise for biomarker-driven clinical trial designs and patient stratification[27]. Associations between the PAX7 target gene biomarker and clinical severity were demonstrated in both TIRM-negative and TIRM-positive FSHD samples, supporting its use across disease phenotypes[27].

### KHDC1L as a Novel Circulating Biomarker

The FORTITUDE trial identified KHDC1L, an RNA-based biomarker regulated by DUX4, as a particularly promising circulating biomarker for monitoring DUX4 activity and treatment response[43][50]. Baseline levels of KHDC1L were significantly elevated in FSHD patients, approximately 6 to 9-fold higher than in healthy controls[43][50]. Following del-brax treatment, KHDC1L levels dropped rapidly and consistently in participants receiving active drug compared to placebo, demonstrating clear drug engagement with the root cause of disease[43][50]. The identification of this circulating biomarker has particular significance as it could potentially accelerate access to treatment by up to two years through enabling earlier regulatory decisions[43].

### Inflammatory Biomarkers

Emerging evidence has identified IL-6 and TNF as potential inflammatory biomarkers in FSHD pathophysiology[25][28][29]. A translational case-control study characterized serum concentration of circulating inflammatory markers, the cytokine production capacity of monocytes and NK cells, and the cytokine production capacity of muscle specimens in FSHD patients compared to matched healthy subjects[28]. IL-6 and IL-1β were measured in serum samples of 150 FSHD patients and 98 healthy controls, with TNF measured in 150 FSHD patients and 59 healthy controls[28]. Results showed that IL-6 concentration in serum was higher in the patient group than in the control group, and TNF concentration was also elevated in FSHD patients, though the TNF finding did not retain statistical significance after multiple comparison corrections[28]. These results suggest that IL-6 and TNF may contribute to FSHD pathology and suggest novel therapeutic targets through inflammatory modulation[28].

## Clinical Trial Infrastructure and Outcome Measures

### FSHD Clinical Trial Research Network

The Facioscapulohumeral Muscular Dystrophy Clinical Trial Research Network (CTRN) represents a consortium of academic research centers with expertise in FSHD clinical research and conducting neuromuscular clinical trials[3]. The CTRN seeks to hasten drug development for FSHD by validating new clinical outcome assessments and refining trial planning strategies[3]. This coordinated infrastructure has been essential in supporting the multiple clinical trials that have advanced the field in recent years.

### Outcome Measure Standardization

Standardized outcome measures have been developed and validated for FSHD clinical trials, including the FSHD Health Index (FSHD-HI), a 15-domain questionnaire measuring total FSHD health-related quality of life incorporating both motor impairment and social and emotional impact[9]. The FSHD-HI combines 116 questions into a total score transformed onto a percentage scale ranging from 0 (no disability) to 100 (maximal disability)[9]. Additionally, the Reachable Workspace (RWS) assessment measures the relative surface area a participant may reach with an outstretched arm, rated on a scale from 0 (no reachable workspace) to 1.25 (maximal reachable workspace), with higher scores indicating better outcomes[29]. The Patient Global Impression of Change (PGIC) is a standard validated participant-report outcome measuring self-reported change in health status compared to study baseline on a 7-point scale[9][29].

## Emerging Therapeutic Mechanisms and Future Directions

### Gene Therapy Approaches Beyond DUX4 Targeting

While most current FSHD gene therapies focus on silencing DUX4, alternative approaches are being explored for other neuromuscular conditions that may have applicability to FSHD. Delandistrogene moxeparvovec (SRP-9001) is an investigational gene transfer therapy being developed for Duchenne muscular dystrophy that delivers a shortened dystrophin retaining key functional domains of the wild-type protein using an adeno-associated virus rhesus isolate serotype 74 (AAVrh74) vector[7]. While this approach specifically addresses dystrophin deficiency in DMD rather than DUX4 pathology in FSHD, the therapeutic principles and AAV delivery technology platform inform FSHD gene therapy development[7]. Treatment with delandistrogene moxeparvovec resulted in correct localization of SRP-9001 dystrophin to the sarcolemma in a large proportion of muscle fibers, with sustained improvement and stabilization of motor function observed up to 4 years post-treatment in patients who would have been predicted to show steep decline based on natural history[7].

### Antisense Oligonucleotide Advantages

Antisense therapies for FSHD targeting DUX4 mRNA represent the furthest along in preclinical and early clinical development compared to other oligonucleotide approaches[20]. Multiple antisense oligonucleotide and RNA interference therapies have shown promising indications both in vitro and in vivo[20]. Antisense therapies present several advantages for FSHD treatment: they are highly specific and employ potent molecules with relatively simple mechanisms of action often taking advantage of conserved cellular processes[20]. Importantly, compared with gene editing approaches that prevent DUX4 expression, antisense therapies involve no changes to genomic DNA and act only at the RNA level[20]. This makes them more acceptable from a regulatory standpoint and avoids moral concerns surrounding CRISPR/Cas9 editing of the human genome, even for therapeutic purposes[20]. For these reasons, antisense therapies may represent a more clinically viable form of targeted genetic therapy for FSHD than alternative approaches[20].

### Exercise and Physical Rehabilitation

While not pharmacological in nature, exercise and physical rehabilitation represent important components of FSHD management. Evidence suggests that physical therapy evaluation and exercise programs can be helpful as initial interventions, particularly for pain management[8]. The effects of exercise and muscle strength training in FSHD are the subject of ongoing systematic reviews and clinical investigation[16]. Future therapeutic approaches may benefit from combining disease-modifying pharmacological treatments with structured exercise and rehabilitation protocols to maximize functional recovery and long-term outcomes.

## Global FSHD Research Initiatives and Clinical Trial Landscape

### Diversity of Clinical Trial Approaches

The current FSHD drug development pipeline demonstrates remarkable diversity in therapeutic approaches being pursued simultaneously[1][1]. More than 10 companies and research laboratories are working on early-stage and advanced treatments for FSHD as part of the drug development pipeline[1]. These efforts encompass small molecules, biologics including monoclonal antibodies and recombinant proteins, oligonucleotide therapies, and gene therapy approaches[1][1]. Small molecules are tiny chemical compounds made in a laboratory that can often be taken as a pill and travel through the body to reach cells where they are needed[1]. Biologics are made from living cells or natural materials like proteins or DNA and are usually much larger and more complex than small molecules, often given by injection or through an IV[1]. Gene therapies aim to fix or control disease by changing how a person's genes work through adding a healthy gene, turning off a faulty gene, or changing how a gene is expressed, typically given through an IV[1].

### Drug Repurposing Strategy

Some treatments being investigated for FSHD were originally developed for other diseases[1]. When a repurposed drug shows promise for FSHD, it can often move quickly into clinical trials, saving time and money because the drug has already been tested for safety[1]. Losmapimod, originally developed for cardiovascular disease, represents an example of this approach, though ultimately unsuccessful for FSHD[1][5]. GBC0905 similarly was originally developed for solid tumors and is being repurposed for FSHD after demonstrating anti-DUX4 activity[30][30].

### International Research Collaboration

FSHD research and drug development efforts span the globe, with clinical trials and research centers located across North America, Europe, Asia, and other regions[1][3][1]. This international collaboration facilitates patient recruitment across diverse populations and enables more comprehensive understanding of disease heterogeneity. The global first-in-human trial of EPI-321 by Epicrispr Biotechnologies represents a recent example of multinational clinical trial coordination[45].

## Conclusion

The landscape of pharmacological treatment for facioscapulohumeral muscular dystrophy has undergone transformative change from a state of complete therapeutic nihilism through the emergence of multiple disease-modifying approaches advancing through clinical development. As of April 2026, no FDA-approved disease-modifying therapies are available, yet unprecedented progress has been achieved with multiple investigational drugs demonstrating preliminary evidence of safety and efficacy. Del-brax from Avidity Biosciences represents one of the most advanced candidates, with Phase 1/2 FORTITUDE trial data demonstrating unprecedented reductions in DUX4-regulated genes and early functional improvement signals, supporting progression to Phase 3 trials with potential accelerated approval pathways. EPI-321 from Epicrispr Biotechnologies offers a potentially transformative one-time epigenetic gene therapy approach that has advanced to first-in-human clinical trials. Multiple additional investigational therapies including apitegromab, ARO-DUX4, small molecule DUX4 inhibitors, and combination hormone therapies are advancing through clinical development pipelines.

The convergence of mechanistic understanding of FSHD pathophysiology around DUX4 toxic gain-of-function has enabled rational drug design targeting this central pathologic driver. Biomarker development, particularly the identification of circulating DUX4-regulated genes including KHDC1L, has advanced clinical trial design and may accelerate regulatory pathways. However, the historical failures of myostatin inhibition approaches despite target engagement and the disappointment of losmapimod despite p38 inhibition provide important reminders that FSHD pathophysiology may require multi-targeted or combinatorial approaches to achieve disease-modifying benefit.

The potential approval of the first disease-modifying therapy for FSHD within the next one to two years represents a watershed moment for patients and families who have long awaited effective treatment options. Continued research into combination therapies, exploration of adjunctive approaches including exercise rehabilitation, and investigation of additional mechanisms beyond DUX4 silencing may further expand the therapeutic armamentarium. The expanding clinical trial infrastructure and international collaborative efforts position the field well to accelerate translation of promising therapeutic concepts into approved medications. Future decades will likely witness not only approval of multiple individual therapies but also refined understanding of patient stratification, optimal dosing regimens, and combination therapy approaches that maximize functional restoration and quality of life for patients with this previously untreatable disease.