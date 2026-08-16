---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T10:15:14.515763'
end_time: '2026-04-04T10:17:41.000796'
duration_seconds: 146.49
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: myotonic dystrophy type 1
  mondo_id: MONDO:0008056
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 51
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** myotonic dystrophy type 1
- **MONDO ID:** MONDO:0008056

## Research Objectives

Please provide a comprehensive research report on **drug therapies for myotonic dystrophy type 1**.
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

# Comprehensive Research Report on Drug Therapies for Myotonic Dystrophy Type 1

This report synthesizes current knowledge regarding pharmaceutical treatments for myotonic dystrophy type 1 (DM1), a progressive autosomal dominant neuromuscular disorder caused by expanded CTG repeats in the dystrophia myotonica protein kinase (DMPK) gene. As of early 2026, no disease-modifying therapies have received regulatory approval, though multiple investigational compounds have advanced to clinical trials with promising early data[2]. The therapeutic landscape has undergone substantial evolution, with FDA Fast Track designations granted to novel gene therapies and multiple clinical programs demonstrating proof-of-concept for targeting the underlying molecular pathology of the disease. This report provides comprehensive coverage of approved symptomatic treatments, investigational agents in various trial phases, repurposing candidates, contraindicated medications, and emerging combination therapy approaches.

## Current Approved Therapies and Their Limitations

### Symptomatic Management and the Absence of Disease-Modifying Treatments

The clinical management of DM1 remains fundamentally limited by the absence of approved disease-modifying therapies[2]. Treatment currently focuses on managing individual symptoms and complications across the disease's multisystemic manifestations, which affect skeletal muscle, cardiac tissue, respiratory function, endocrine systems, and cognitive domains[2]. This symptomatic approach addresses manifestations as they arise but does not alter the underlying genetic defect or halt disease progression, which occurs at a relatively slow rate of one to three percent muscle strength decline per year[31].

Among available medications for symptomatic care, mexiletine represents the most disease-specific pharmacological intervention with regulatory recognition. The FDA granted mexiletine orphan drug designation for myotonic disorders, acknowledging its specific mechanism of action targeting myotonia, the characteristic inability to rapidly relax muscles following voluntary contraction[7]. Mexiletine functions as a sodium channel antagonist, enhancing fast inactivation of sodium channels and thereby counteracting the slowed rate of sodium channel inactivation that leads to repetitive muscle fiber firing and delayed muscle relaxation[8][8]. In randomized controlled trials in patients with non-dystrophic myotonia, mexiletine at doses of 167 to 500 milligrams per day significantly reduced myotonia compared to placebo[7]. However, a Phase 1/2 study in DM1 patients demonstrated that while mexiletine improved grip strength, it failed to meet its primary endpoint of improved six-minute walk distance[8][8], suggesting that symptomatic relief of myotonia may not translate directly to functional improvement in mobility. Clinical guidelines recommend that mexiletine use in DM1 patients should be preceded by comprehensive cardiac workup to exclude structural or functional abnormalities, with monitoring conducted by cardiologists experienced in DM1 care, as mexiletine is a class 1B antiarrhythmic with potential cardiac effects[12][31]. Phase 3 trials further investigating mexiletine efficacy remain ongoing[8].

### Modafinil for Excessive Daytime Sleepiness

Excessive daytime sleepiness (EDS) represents one of the most disabling symptoms reported by DM1 patients, with prevalence estimates indicating that eighty percent or more of individuals struggle with severe fatigue and EDS despite these symptoms receiving less emphasis in clinical discussions than myotonia or muscle weakness[14]. Modafinil, a psychostimulant drug approved for narcolepsy, has been extensively used off-label in DM1 populations to address this symptom. Clinicians with extensive experience in DM1 management have found modafinil to be extremely effective in appropriately selected patients with a very low incidence of serious adverse effects[22]. However, the European Medicines Agency conducted a review concluding that on current evidence regarding safety and efficacy, modafinil's use should be restricted to treatment of narcolepsy, noting that for other conditions there was insufficient evidence of benefit to outweigh potentially serious side effects including severe skin reactions and cardiac arrhythmia[22]. Despite these regulatory concerns, patient advocacy and clinician experience have supported continued use in carefully selected DM1 populations, and modafinil remains a pragmatic therapeutic option when other approaches prove insufficient[22].

### Respiratory and Gastrointestinal Supportive Care

Beyond pharmacological interventions, the management of respiratory complications in DM1 relies on mechanical ventilation strategies and exercise-based approaches. Noninvasive positive pressure ventilation or bilevel positive airway pressure ventilation may relieve chronic hypoventilation-related symptoms and sleep apnea-hypopnea[9]. Recent evidence demonstrates that respiratory strength and endurance training represents a safe and effective intervention, with a randomized controlled trial showing significant improvements in maximal inspiratory pressure (MIP) compared to control groups[23]. Strength training increased MIP by 76.82 percent of target value, while endurance training increased MIP by 66.06 percent, both with large effect sizes compared to control groups that improved only 10.49 percent[23]. These interventions highlight the importance of nonpharmacological approaches in comprehensive DM1 management.

Gastrointestinal dysfunction, present in approximately thirty to sixty percent of DM1 patients, requires targeted symptomatic management using medications such as metoclopramide for gastroparesis, standard stool softeners and laxatives for constipation, and dietary modification for acid reflux[38]. The medical management of gastrointestinal complications remains largely supportive rather than disease-modifying, addressing the secondary effects of smooth muscle dysfunction and dysmotility rather than correcting the underlying molecular pathology.

## Investigational Drugs in Clinical Development

### Gene Therapy Approaches Targeting DMPK Expression

The most advanced investigational therapies for DM1 employ gene therapy strategies designed to silence expression of the mutant DMPK gene, thereby eliminating the toxic RNA foci responsible for disease pathology. The central pathogenic mechanism in DM1 involves formation of nuclear foci containing expanded CUG-repeat RNA that sequesters RNA-binding proteins such as muscleblind-like (MBNL) proteins, resulting in dysregulated alternative splicing, mRNA translation impairments, and mRNA instability[8][29]. By reducing DMPK transcripts, investigational therapies aim to eliminate these toxic RNA foci and restore normal splicing and protein translation.

#### SAR446268: Sanofi's One-Time AAV Gene Therapy

Sanofi's SAR446268 represents the most recently advanced investigational gene therapy for DM1, having earned FDA Fast Track designation in September 2025[10]. The therapy employs a vectorized RNA interference approach to silence DMPK expression through a single intravenous administration[10]. By reducing DMPK transcripts, SAR446268 aims to eliminate the abnormal and toxic RNA foci responsible for splicing defects in muscle tissue, thereby restoring normal splicing and improving muscular function[10]. This one-time administration approach offers distinct practical advantages over repeated dosing regimens, potentially enhancing patient compliance and reducing treatment burden. The therapy has potential to address key symptoms of DM1 including progressive muscle weakness, difficulty relaxing muscles (myotonia), and effects on multiple body systems including heart, lungs, and endocrine functions[10]. SAR446268 currently is under investigation in a first-in-human Phase 1-2 study to evaluate safety, tolerability, and efficacy (clinical study identifier NCT06844214), with the first patient enrollment planned for late 2025[10]. Sanofi has already been granted orphan designations for SAR446268 in both the US (July 2024) and EU (October 2024), recognizing its potential to address a serious unmet medical need in a disease affecting fewer than 200,000 patients in the United States[10].

#### ARO-DM1/SRP-1003: Arrowhead Therapeutics' RNAi Therapeutic

Arrowhead Pharmaceuticals' ARO-DM1, now designated SRP-1003 following license to Sarepta Therapeutics in 2025, represents an investigational RNA interference therapeutic designed to reduce expression of the DMPK gene[6][28]. The company filed regulatory clearance to initiate a Phase 1/2a clinical trial in November 2023, with the trial designated ARODM1-1001, a dose-escalating study to evaluate safety, tolerability, pharmacokinetics, and pharmacodynamics in up to 48 subjects with DM1[6]. The RNAi approach silences aberrantly transcribed DMPK mRNA, potentially leading to improvements in multiple symptoms including muscle strength and function[6]. As of November 2025, Arrowhead announced achievement of the second development milestone event in the Phase 1/2 clinical study, triggering a $200 million milestone payment from Sarepta[28]. This milestone was reached following drug safety committee review, authorization to dose escalate, and achievement of pre-specified patient enrollment targets, with accrual of patients in cohort 4 (6 mg/kg) of the multiple ascending dose portion nearly complete and initiation of cohort 5 (12 mg/kg) planned for first quarter 2026[28]. The advancement to higher dose cohorts indicates that safety signals have not prevented further development, supporting continued investigation of this RNAi platform.

#### Vertex Pharmaceuticals' VX-670: Peptide-Conjugated Oligonucleotide

Vertex Pharmaceuticals is developing VX-670, a peptide conjugated oligonucleotide designed to target DMPK mRNA[5][16]. The therapy entered Phase 1/2 clinical testing through the Galileo study (NCT06185764), a randomized, double-blind, placebo-controlled trial evaluating safety, tolerability, pharmacokinetics, and pharmacodynamics at different single and multiple doses in adult participants with DM1[16]. The trial involves dose escalation studies in approximately 44 participants and will assess changes in splicing index in muscle biopsies at baseline, day 15, and day 120 as pharmacodynamic markers of drug target engagement[16]. By delivering an oligonucleotide with improved tissue penetration through peptide conjugation, VX-670 aims to achieve more efficient silencing of DMPK compared to unconjugated oligonucleotides.

#### DYNE-101/Zeleciment Basivarsen: Antibody-Conjugated Oligonucleotide with Enhanced Muscle Targeting

Dyne Therapeutics' zeleciment basivarsen, previously known as DYNE-101, represents an innovative delivery strategy combining an antisense oligonucleotide with an antibody fragment conjugate. The therapeutic consists of an antisense oligonucleotide conjugated to a fragment antibody that binds to transferrin receptor 1, which is highly expressed on muscle tissue, thereby achieving targeted delivery to the primary disease-affected tissue[15][39]. DYNE-101 is designed to reduce levels of mutant DMPK RNA in the nucleus, release sequestered splicing proteins, allow normal mRNA processing and translation of normal proteins, and potentially stop or reverse disease progression[15]. The Phase 1/2 ACHIEVE clinical trial (NCT05481879) demonstrated substantial clinical benefit, with data presented at the 2026 Muscular Dystrophy Association Clinical & Scientific Conference showing functional improvements relative to placebo across multiple clinical measures[39]. Specifically, zeleciment basivarsen led to improvements in video hand opening time (vHOT), an assessment of hand myotonia, with other measures of muscle strength and upper and lower limb function showing similar improvements[39]. Beyond objective measures of function, participants completed the myotonic dystrophy health index, a patient-reported measure of disease burden spanning seventeen areas of health relevant to DM1, with more than three-quarters of patients and clinicians perceiving clinical status as much or very much improved after one year on zeleciment basivarsen at the selected dose[39]. The therapy demonstrated a very good and favorable safety profile with more than one thousand doses administered[39]. These compelling Phase 1/2 results supported advancement to Phase 3 testing through the HARMONIA study, which will run globally and include approximately 150 people with DM1 ages sixteen and older[39]. HARMONIA participants will be randomly assigned to receive either zeleciment basivarsen at 6.8 mg/kg or placebo once every eight weeks for approximately one year, followed by all participants receiving zeleciment basivarsen in a long-term extension phase[39]. The trial will further assess DM1's multisystem impact through neurological effect evaluation including cognition, emotional issues, sleep, and fatigue[39]. DYNE-101 has been granted orphan drug designation for DM1 treatment by both the European Medicines Agency and the US Food and Drug Administration[15].

#### PGN-EDODM1: PepGen's Peptide-Conjugated Antisense Oligonucleotide

PepGen is developing PGN-EDODM1, a peptide conjugated antisense oligonucleotide targeting DMPK mRNA[5]. The compound is being evaluated in the FREEDOM-DM1 study (NCT06204809), a Phase 1/2 trial assessing safety, tolerability, pharmacokinetics, and pharmacodynamics of single intravenous doses in participants with DM1[17]. The study consists of a screening period of up to thirty days and a treatment and observation period of sixteen weeks[17]. Primary outcome measures focus on adverse events and laboratory abnormalities, while secondary measures evaluate maximum plasma drug concentration, time to maximum concentration, terminal half-life, and area under the concentration-time curve[17].

#### AOC 1001: Avidity Biosciences' Antibody-Oligonucleotide Conjugate

Avidity Biosciences developed AOC 1001, an antibody oligonucleotide conjugate successfully delivering small interfering RNA (siRNA) to muscle, resulting in DMPK mRNA reductions and splicing improvements leading to functional improvements[40]. The MARINA Phase 1/2 trial demonstrated that AOC 1001 achieved directional improvements in multiple clinical endpoints in the dose range of 2-4 mg/kg, with improvements in myotonia (vHOT) observed as early as six weeks after dosing with sustained effects at month 6[40]. Quantitative muscle testing total strength measure showed improvement at month 6, and early signs of mobility improvements appeared in the 10-meter walk/run test and timed up-and-go test[40]. AOC 1001 demonstrated DMPK mRNA reduction in evaluable muscle biopsies and corresponding splicing improvements[40]. The therapy had a generally favorable safety and tolerability profile, supporting advancement into Phase 3 studies[40].

#### AT466: Astellas Gene Therapies' AAV-Antisense Approach

Astellas Gene Therapies is developing AT466, an adeno-associated viral antisense therapeutic targeting DMPK[5][26]. This program originated from AskBio's gene therapy research for DM1 before being integrated into the Astellas pipeline[26]. The AAV delivery platform offers potential advantages in sustained DMPK suppression through persistent transgene expression.

### Small Molecule Approaches to DM1 Therapy

#### Tideglusib: Glycogen Synthase Kinase 3 Beta Inhibition

AMO Pharma's tideglusib represents a small molecule approach targeting glycogen synthase kinase 3 beta (GSK3β) and has demonstrated particular promise in congenital and childhood-onset DM1[5][13][13]. The compound is being evaluated in the REACH CDM X study (NCT05004129), an open-label Phase 2/3 trial assessing safety and efficacy in individuals with congenital or childhood-onset DM1 ages six to forty-five years[13][13]. This fifty-two-week treatment period study uses either weight-adjusted fixed doses of 1000 mg or weight-banded fixed dosing of tideglusib[13]. Notably, tideglusib demonstrates specific activity in the younger-onset forms of DM1, which present with more severe muscle involvement and developmental complications compared to adult-onset disease. GSK3β inhibition represents a distinct molecular mechanism from DMPK targeting, potentially offering complementary therapeutic benefits or distinct advantages in particular patient populations.

#### Metformin: Repurposing of Antidiabetic Agent

Metformin, a biguanide antidiabetic drug, has emerged as a promising repurposing candidate for DM1 based on multiple preclinical and clinical studies[18][8]. Different studies revealed that metformin rescues multiple phenotypes of DM1 disease[18]. In particular, recent evidence suggests that metformin corrects DM1-related alternative splicing defects, alleviates several age-related molecular alterations, reduces the risk of developing cancer, and improves mobility in DM1 patients[18]. A small clinical trial explored metformin administration effects on mobility in non-diabetic DM1 patients through a fifty-two-week monocentric, randomized, placebo-controlled, double-blind Phase II study in which oral metformin or placebo was provided three times daily, with a dose-escalation period over four weeks up to three grams per day, followed by forty-eight weeks at maximum dose[18]. For the twenty-three of forty patients who completed the one-year study, statistically significant differences between groups were observed, with the treated group (n = 9) improving six-minute walk test distance by approximately 29 meters compared to the placebo group (n = 14)[18][8]. Moreover, there was statistically significant improvement in total mechanical power during gait, although metformin did not appear to have visible effects on myotonia or muscle strength[18]. These encouraging results supported the putative role of metformin in treating myotonic dystrophy patients, prompting initiation of a multicenter Phase III clinical trial (2018-000692-32) in Italy with approximately 100 DM1 patients receiving metformin for twenty-four months[18].

#### Pitolisant: Histamine H3 Receptor Antagonism

Harmony Biosciences conducted a Phase 2 signal detection study evaluating pitolisant for DM1 patients[14]. Pitolisant is marketed as Wakix and is currently approved for treatment of excessive daytime sleepiness and cataplexy in adults with narcolepsy, as well as EDS in children with narcolepsy ages six or older[14]. The drug works by targeting histamine H3 receptors and regulating histamine release, with histamine playing a role in wakefulness and alertness as a neurotransmitter[14]. When the body promotes histamine release, it reduces fatigue. The Phase 2 study enrolled twenty-five individuals with DM1 who were split into three groups: the first received placebo, the second a low dose of pitolisant, and the third a high dose[14]. Both high- and low-dose pitolisant improved fatigue and excessive daytime sleepiness, with improvement more significant in the higher-dose group[14]. Importantly, pitolisant demonstrated consistency in safety and tolerability compared to its known safety profile from narcolepsy studies[14]. Although this was a signal detection study lacking statistical significance, the results were sufficiently promising that Harmony Biosciences planned to launch further studies including a Phase 3 trial for pitolisant in DM1[14].

### Combination Therapeutic Approaches

#### Erythromycin and Pafuramidine Combination

Recent research has explored combination approaches targeting complementary mechanisms in DM1. A study examining combination treatment of erythromycin and pafuramidine (also known as furamidine) demonstrated additive and synergistic rescue of mis-splicing in myotonic dystrophy type 1 models[24]. Erythromycin was proposed to rescue molecular phenotypes of DM1 by disrupting the MBNL-CUG RNA interaction via binding the CUG-repeat RNA, thereby releasing sequestered RNA-binding proteins[24]. In DM1 patient-derived myotubes containing approximately 2900 CTG-repeats, using concentration ranges of 0.25–1 μM pafuramidine in combination with 25–100 μM erythromycin tested whether combination treatment would be more effective in rescuing mis-splicing than either drug alone[24]. Additive rescue was observed for all combination treatments, with 50 μM erythromycin in combination with 0.25–1 μM pafuramidine not displaying dose-dependent increase in mis-splicing rescue[24]. Importantly, all combinations tested displayed little to no cell toxicity in DM1 myotubes[24]. At 50 μM erythromycin, cell viability was reduced to 0.69 relative to untreated DM1 myotubes, but addition of 0.25–1 μM pafuramidine in combination with 50 μM erythromycin increased cell viability to the same level as untreated DM1 myotubes[24]. Global RNA-seq analysis revealed that the average percent rescue for exon skipping events using 25 μM erythromycin, 0.5 μM pafuramidine, and the combination were approximately 61, approximately 64, and approximately 68 percent, respectively[24], demonstrating synergistic correction of splicing defects. For specific splice events, such as the AGRN exon, the rescue was 74 ± 26, 79 ± 12, and 92 ± 15 percent with erythromycin, pafuramidine, and combination, respectively[24]. These findings provide proof-of-concept for rational combination approaches targeting multiple aspects of DM1 pathophysiology.

## Drug Repurposing and Off-Label Use Candidates

### Exercise and Rehabilitation as Therapeutic Interventions

While not pharmacological, structured exercise represents an evidence-based therapeutic approach supported by multiple studies. Regular exercise, including both aerobic and strength training, has been shown to improve cardiorespiratory fitness, muscle function, and quality of life in individuals with DM1[8][8]. A Cochrane review examining safety and efficacy of strength and aerobic training in neuromuscular diseases identified thirty-six studies total but found only three randomized controlled trials meeting inclusion criteria[44]. Based on these studies, authors concluded that strengthening exercises at moderate intensity did not worsen disease progression in persons with myotonic dystrophy[44]. Multiple authors concluded that strengthening exercises in combination with aerobic exercises are "likely to be effective"[44]. Given evidence that moderate exercise does not worsen disease progression and may be effective, general recommendations can be made: depending on activity level, individuals may benefit from a strengthening program, while those who lead active lifestyles may not have much disuse weakness[44]. Cardiovascular exercise performed at low to moderate intensity has been found safe in people with myotonic dystrophy[44].

### Cannabinoid-Based Therapeutics

Patient advocacy has driven investigation of cannabinoid-based therapies for symptom management in DM1. A pilot survey of cannabis use in DM confirmed interest of the patient community, showing that 33 percent of U.S. DM1 patients regularly utilize cannabis or cannabinoids for symptomatic relief[35]. While interest in cannabinoids, particularly for pain management in DM1, has been acute, evidence of efficacy can best be characterized as anecdotal[35]. Dr. Federica Montagnese and colleagues at Ludwig-Maximilians-University Munich provided a cannabidiol and tetrahydrocannabinol (CBD/THC) cocktail to four patients with DM1 or DM2 and two patients with CLCN1-myotonia through a compassionate use protocol[35]. Study duration was four weeks, with weekly assessed endpoints including myotonia behavior scale, hand-opening time, visual analogue scales for myalgia and myotonia, and fatigue and daytime sleepiness severity scale[35]. Almost all patients reported improvement in myotonia and hand-opening time, with myotonia behavior scale values improving for all patients[35]. Some improvement in myalgia was also noted, and significant improvement in gastrointestinal symptoms was reported by patients experiencing symptoms at study start[35]. However, the research was limited by small sample size, use primarily of patient-reported outcomes, and heterogeneity of the DM population, with considerably more controlled studies needed before understanding whether and how to use this compound class in DM1 patients[35]. Nexien BioPharma announced intent to seek a pre-IND meeting with FDA and pursue a clinical development program in DM1 with specific cannabinoid formulations[35].

### Corticosteroid Effect on Myotonia

A case report documented an unexpected association between methylprednisolone dosing and cessation of myotonia in a DM1 patient receiving corticosteroid treatment for ulcerative colitis[36]. The patient reported cessation of myotonia three weeks after starting methylprednisolone at 32 mg with weekly 4 mg dose reductions, achieving total disappearance of myotonia by four weeks after starting methylprednisolone[36]. The first symptoms of myotonia returned approximately one month after the final dose and reached peak severity more than two months after final dose discontinuation[36]. This observation suggests potential mechanistic connections between immune function and myotonia manifestation, though evidence remains anecdotal and further investigation would be required before recommending corticosteroids as therapeutic agents for DM1.

## Contraindicated Medications and Drugs Causing Adverse Effects

### Anesthetic Complications and Drug Sensitivities

DM1 patients present unique challenges for perioperative management due to exquisite sensitivity to multiple medication classes. Patients with DM1 are far more likely than the general population to have adverse reactions to medications used for anesthesia and analgesia, with interactions of cardiac, respiratory, muscle, and central nervous systems leading to serious complications[12][19][12]. Succinylcholine must be strictly avoided due to risk of masseter spasm and hyperkalemia, despite DM1 not increasing true malignant hyperthermia reactions beyond the general population[12][19][12]. Anticholinesterase agents such as neostigmine have been found to cause incomplete reversal and depolarization in DM1 patients, with sugammadex recommended as preferred reversal agent for neuromuscular blocking drugs[21].

Opioid medications present particular concerns due to heightened sensitivity and prolonged interaction with various aspects of myotonic dystrophy. DM1 patients demonstrate prolonged and heightened sensitivity to sedatives and analgesics, resulting in serious complications in the post-anesthesia period[12][19][12]. Use of shorter-acting opioids is recommended for intraoperative periods, but opioids should be avoided in preoperative and postoperative settings due to higher risk of respiratory depression and higher risk of postoperative ileus[21]. Propofol, while demonstrated safe in many reported cases, can induce myotonia in some patients and may cause prolonged recovery after targeted controlled infusions[21].

### Statin-Associated Myopathy in DM1

Statins, the most commonly prescribed medications for hyperlipidemia, carry significant risk in DM1 populations due to drug-induced myopathy potential[42]. Although statins have strong potential to reduce cardiovascular disease risk (the most common cause of death in developed societies), neuromuscular symptoms represent the most common cause of medication withdrawal[42]. According to studies, the incidence of myopathy in patients using statins ranges from 5–20 percent, though variations reflect different myopathy definitions across investigators[42]. A higher incidence of myotoxicity occurs with lipophilic statins compared to hydrophilic ones and with higher drug doses[42]. Because high serum creatine kinase levels are very frequently found in hereditary myopathies including DM1, physicians are reluctant to use statins in such patients[42]. Recent literature on statin side effects in hereditary myopathies suggests particular caution in myotonic dystrophy type 2, with some conditions considered contraindicated for statin usage[42]. Possible solutions to the therapeutic dilemma include prescribing alternative cholesterol-lowering agents and carefully monitored treatment initiation of statins at reduced doses with frequent laboratory monitoring[42].

### Drug-Induced Myopathies Affecting DM1 Populations

Beyond statins, multiple medication classes carry potential to worsen muscle function or cause myopathic manifestations in genetically predisposed patients. Antimalarial drugs such as chloroquine and hydroxychloroquine cause myopathy as a known complication, usually mild but characterized by decreased strength of proximal muscles with potential severe dysphagia in some cases[20]. Higher cumulative doses and longer drug exposure associate with more severe symptoms and higher risk of myocardial involvement and dysphagia[20]. Cyclosporine can cause myopathy manifested by myalgia, muscle weakness, and increased creatine kinase activity, with post-marketing surveillance data indicating an incidence of 0.17 percent[20]. The pathogenesis appears related to mitochondrial dysfunction from therapy, though conclusive work remains unpublished[20]. Immune checkpoint inhibitors, particularly PD-1 inhibitors, carry risk of treatment-emergent myositis, with biopsy-proven myositis incidence reaching 0.8 percent in some reports[20].

## Natural History and Biomarker Development for Therapeutic Assessment

### Splicing Index as Disease Biomarker and Therapeutic Target

Recent advances in biomarker discovery have substantially improved disease monitoring and treatment assessment capabilities. The Myotonic Dystrophy Splice Index (SI), a composite RNA splicing biomarker incorporating twenty-two disease-specific events, represents a crucial development for assessing therapeutic response to disease-modifying interventions[30]. The SI demonstrated significant associations with measures of muscle strength and ambulation, including ankle dorsiflexion strength (Pearson's r = -0.719) and ten-meter run/fast walk (r = -0.680)[30]. Importantly, the SI was relatively stable over three months (intraclass correlation coefficient = 0.863), suggesting utility as a reliable biomarker[30]. Latent-class analysis identified three DM1 subgroups stratified by baseline SI (SIMild, SIModerate, and SISevere), with SIModerate individuals showing significant SI increase over three months[30]. Multiple linear regression modeling revealed that baseline ankle dorsiflexion and SI were predictive of strength at three months (adjusted R² = 0.830)[30]. These findings establish the SI as a reliable biomarker capturing associations of RNA mis-splicing with physical strength and mobility and having prognostic utility to predict future function[30].

### Longitudinal Natural History Studies

Large international natural history studies are currently underway to characterize DM1 phenotypic heterogeneity and disease progression. The Establishing Biomarkers and Clinical Endpoints in Myotonic Dystrophy Type 1 (END-DM1) protocol describes an international, prospective, multi-site observational study with twenty-four-month follow-up including approximately 700 adult DM1 patients[32]. Visits occur at baseline and months 12 and 24, with all patients undergoing strength testing, myotonia assessment, functional outcome assessments battery, spirometry, and various questionnaires and cognitive tests[32]. Blood and urine samples are collected at each visit for biomarker studies, with a subset of 60 patients undergoing muscle biopsy at baseline and at additional 3-month visit[32]. The study determines sensitivity to disease progression and minimally clinically important differences for various clinical outcome measures and evaluates associations between baseline patient characteristics and disease progression rate[32]. These natural history data will contribute substantially to optimizing clinical trial design and identifying patient populations most likely to benefit from specific therapeutic interventions.

## Multisystem Management and Quality of Life Considerations

### Cognitive and Neuropsychological Aspects

Cognitive impairment represents a central nervous system manifestation increasingly recognized in DM1[46]. Recent research has linked DM1 to alterations in white matter integrity and identified relationships between cognitive impairment and white matter structure abnormalities[46]. Neuroimaging studies show structural changes including ventriculomegaly, white matter lesions, and brain atrophy, with changes potentially associated with family history of lesions, disease duration, and inheritance pattern but not with CTG repeat size[46]. Cognitive impairment and psychological problems in DM1 patients can affect their behaviors toward medical providers and their access to appropriate care, leading to worse quality of life[34].

Neuropsychological assessment of thirty-one DM1 patients using standardized instruments including MMSE, Frontal Assessment Battery, and comprehensive neuropsychological batteries found that mini-mental state examination scores were normal in 96.8 percent of entire DM1 population, with only one patient reporting severe impairment[34]. Concerning specific cognitive domains assessed through comprehensive neuropsychological batteries, 80.6 percent of patients were in the range of normality with 19.4 percent below norm[34]. The most negatively affected domains were attention (25.8 percent), mental representation (29.0 percent), praxis (32.3 percent), and discrimination (22.6 percent)[34]. Longer disease duration was associated with cognitive impairment, suggesting progressive nature of central nervous system involvement[34].

Psychotherapy has demonstrated potential to help DM1 patients and their families learn new behaviors and coping strategies for managing disease symptoms[34]. Given that chronic diseases present substantial challenges to health-related quality of life, individual perceived physical and mental well-being, psychological support appears essential. A study evaluating health-related quality of life in 200 DM1 patients found that patients scored lower on all SF-36 physical health subscales compared with normative data but did not differ with respect to mental health function. Regression analysis revealed that psychological distress, fatigue, severe muscular impairment, emotional stability, not having worked within previous 12 months, and lower intellectual quotient were associated with lower physical health function scores. Neuroticism, daytime sleepiness, dissatisfaction with social participation, and lower conscientiousness were associated with lower mental health function scores. These findings underscore that factors amenable to treatment and psychosocial interventions—including fatigue, daytime sleepiness, psychological distress, unemployment, and social participation dissatisfaction—significantly affect quality of life and represent important targets for comprehensive DM1 management integrating health, social, and community services.

### Pain Management in Muscular Dystrophy

Nearly one-third of people with muscular dystrophy experience moderate pain, though prevalence varies substantially by MD type[33]. Among those with at least one recorded pain score (from 0 to 10), approximately half (54 percent) reported any pain (score greater than 0) and approximately one-third (34 percent) reported moderate pain (score of at least 5)[33]. The proportion reporting moderate pain varied by MD type, from congenital MD (13 percent) to limb-girdle MD (53 percent)[33]. Across all MD types combined, most (78.2 percent) had no recorded prescription pain medications[33]. For those who did, opioids and anticonvulsants were most frequent, with tramadol, acetaminophen-hydrocodone, gabapentin, cyclobenzaprine, and meloxicam representing the most common individual medications[33]. The most common first medication type was non-opioid only (57 percent), followed by opioids only (28.3 percent), and both combined (14.7 percent)[33]. During follow-up, non-opioid and opioid combination use increased (33.7 percent), while opioid monotherapy lessened (17.2 percent), and non-opioids remained most common (49.1 percent)[33]. Impaired mobility was the most significant factor associated with prescription pain medication use[33].

### Cardiac Complications and Risk Stratification

Cardiac involvement affects up to 80 percent of DM1 patients and results from progressive myocardial fibrosis, with arrhythmias representing the second most common cause of death in DM1[37][50]. Cardiac abnormalities manifest on electrocardiogram (ECG) often prior to development of cardiac symptoms and may precede muscular symptoms[37]. ECG abnormalities are present in approximately 65 percent of DM1 and 20 percent of DM2 patients[37]. In DM1 patients, first-degree atrioventricular delay is the most common abnormality (42 percent), followed by non-specific intraventricular conduction delay (12 percent)[37]. Atrial arrhythmias are the most common clinical arrhythmias associated with DM1 and represent independent predictors of increased mortality[37]. Atrial fibrillation and atrial flutter have estimated prevalence of 10.9 percent and 8.5 percent, respectively in DM1[37].

Clinical characteristics including age, number of CTG repeats, presence of atrial tachyarrhythmia, and left ventricular ejection fraction appear useful in predicting conduction disease progression rate[50]. A retrospective study identified that patients with DM1 can develop rapid changes in cardiac conduction intervals, with paroxysmal atrial flutter or fibrillation, older age, and larger CTG expansions predicting greater time-dependent PR and QRS interval prolongation[50]. These findings warrant particular attention in arrhythmic evaluation of high-risk patient subsets[50]. Recent data show that sudden cardiac death, striking up to one-third of patients, can occur from ventricular tachyarrhythmias, suggesting that other predictive factors like syncope, family history of sudden death, or non-sustained ventricular tachycardia should be considered for risk stratification[45]. Prophylactic ICD implantation in patients with neuromuscular disorders should follow criteria used in non-ischemic dilated cardiomyopathy, with ICD implantation reasonable in DM1 patients when pacing is needed[45].

## Future Directions and Emerging Therapeutic Paradigms

### Acceleration of Clinical Development Through Regulatory Pathways

The granting of FDA Fast Track designation to SAR446268 and other investigational DM1 therapies reflects regulatory recognition of the substantial unmet medical need in this disease and potential of novel molecular-targeting approaches[1][10]. Fast Track designation aims to facilitate development and expedite review of medicines treating serious conditions, covering broad range of serious illnesses[10]. This regulatory pathway enables more frequent communication with FDA during development and allows priority review designation upon submission for approval[10]. The advancement of multiple Phase 1/2 and Phase 2/3 trials simultaneously represents unprecedented pace in DM1 therapeutic development, driven by molecular insights into disease pathogenesis and availability of validated biomarkers such as the Splice Index for assessing target engagement and predicting clinical benefit.

### Gene Therapy as Standard-of-Care Treatment

If current Phase 1/2 and Phase 3 trials prove successful, gene therapy approaches targeting DMPK silencing may establish a new treatment paradigm for DM1. One-time or limited-dosing AAV-based approaches like SAR446268 offer practical advantages over chronic medication regimens, potentially improving compliance and patient quality of life. However, implementation challenges remain regarding patient selection, timing of intervention to maximize benefit, and long-term durability of therapeutic effect. The potential for accelerated or regular FDA approval pathways based on Phase 1/2 data with substantial functional improvements suggests that disease-modifying therapy could become available to DM1 patients within the next one to two years, substantially altering disease management paradigms.

### Combination Therapy Development

As understanding of DM1 pathophysiology deepens, rational combination approaches targeting complementary mechanisms may offer superior efficacy compared to monotherapies. The proof-of-concept data for erythromycin and pafuramidine combination suggest that synergistic correction of splicing defects may be achievable through coordinated targeting of MBNL sequestration and CUG-repeat RNA interactions. Future clinical trials might explore combining DMPK-silencing therapies with agents targeting compensatory splicing factor dysregulation or mitochondrial dysfunction, potentially achieving more comprehensive disease modification.

### Biomarker-Guided Patient Stratification

Increasingly sophisticated biomarker approaches, exemplified by the Splice Index, will enable better patient stratification for clinical trials and personalized medicine approaches. Identifying patient subgroups most likely to benefit from specific therapeutic interventions based on baseline biomarker profiles and genetic characteristics could substantially improve trial efficiency and therapeutic success rates. The END-DM1 natural history study will provide crucial data for this biomarker-guided stratification approach.

## Conclusion

The therapeutic landscape for myotonic dystrophy type 1 has undergone remarkable transformation from a disease with purely symptomatic management options to one with multiple disease-modifying approaches in advanced clinical development. While no curative therapies currently exist, saxenda progression of gene therapy and antisense oligonucleotide programs through Phase 1/2 and Phase 2/3 trials with convincing functional improvements offers genuine hope for disease-modifying treatment availability within the immediate future. Current symptomatic therapies including mexiletine for myotonia and modafinil for excessive daytime sleepiness provide meaningful benefit for selected patients, while comprehensive multidisciplinary management addressing cardiac, respiratory, cognitive, and gastrointestinal complications remains essential pending disease-modifying therapy availability.

The advancement of multiple therapeutic platforms simultaneously—including AAV gene therapy, RNA interference therapeutics, antisense oligonucleotides, small molecule inhibitors, and repurposing candidates—reflects multiple distinct approaches to DM1 pathophysiology, each with potential advantages and limitations. Integration of sophisticated biomarkers such as the Splice Index with longitudinal natural history data will enable increasingly precise therapeutic trial design and patient stratification. The convergence of molecular insights into DM1 pathogenesis, regulatory recognition of unmet medical need through Fast Track designations, and development of patient-centric outcome measures positions the field for potentially transformative advances in disease management within the coming years. Continued investment in clinical research, biomarker development, and comprehensive patient care coordination will be essential to translating promising preclinical and early clinical data into meaningful improvements in patient outcomes and quality of life for individuals affected by this serious progressive neuromuscular disorder.