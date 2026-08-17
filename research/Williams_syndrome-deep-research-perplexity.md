---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T10:34:12.175902'
end_time: '2026-04-04T10:36:43.380074'
duration_seconds: 151.2
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: Williams syndrome
  mondo_id: MONDO:0008678
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
- **Disease Name:** Williams syndrome
- **MONDO ID:** MONDO:0008678

## Research Objectives

Please provide a comprehensive research report on **drug therapies for Williams syndrome**.
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

# Comprehensive Pharmacological Management of Williams Syndrome: Drug Therapies, Clinical Applications, and Emerging Therapeutic Strategies

## Executive Summary

Williams syndrome (Williams-Beuren syndrome, WBS) is a rare genetic disorder resulting from a microdeletion of 26-28 genes on chromosome 7q11.23, affecting approximately 1 in 7,500 to 1 in 10,000 individuals[7][26]. The condition manifests as a complex multisystem disorder characterized by distinctive facial features, intellectual disability, hypersociability, and most critically, cardiovascular abnormalities that represent the primary source of morbidity and mortality[9][26]. Currently, there is no cure for Williams syndrome; instead, therapeutic management focuses on addressing the diverse medical complications that emerge across the patient's lifespan[7]. This comprehensive report examines the landscape of pharmacological interventions for Williams syndrome, encompassing approved medications, investigational agents in active clinical trials, repurposing candidates with supportive evidence, and emerging molecular targets identified through recent mechanistic research. The treatment approach must be highly individualized, accounting for the complex comorbidities inherent to the condition and the unique medication sensitivities observed in this population. Recent discoveries identifying sphingosine kinase 1 as a critical early node in vascular smooth muscle proliferation, coupled with ongoing Phase III clinical trials of clemastine fumarate for neurodevelopmental improvements, signal a paradigm shift toward mechanism-directed interventions that may address the root causes rather than merely managing symptoms.

## Foundational Pathophysiology and Treatment Rationale

### Genetic Basis and Molecular Mechanisms

Williams syndrome arises from a 1.5-megabase hemizygous deletion on chromosome 7q11.23, resulting in the haploinsufficiency of approximately 26-28 genes[4][10]. Among these genes, the elastin gene (ELN) plays a particularly central role in the pathophysiology of cardiovascular manifestations. **Elastin haploinsufficiency leads to a 50% reduction in elastin production**, a critical protein that confers elasticity to blood vessels, enabling them to recoil like a rubber band[3]. This reduction fundamentally alters vascular architecture and cellular signaling, initiating a cascade of pathological processes[3][4].

The cascade begins early in vascular development, with evidence of vascular smooth muscle cell proliferation detectable as early as embryonic day 15.5 in elastin-deficient mice[10]. The aberrant proliferation of vascular smooth muscle cells (VSMCs) establishes new layers of cells, each producing a new elastic lamella in an effort to compensate for elastin insufficiency[9]. This compensatory response paradoxically produces inward wall thickening rather than outward expansion, leading to progressive arterial stenosis and the characteristic supravalvular aortic stenosis and pulmonary artery stenosis observed in 87.5% and 53.8% of patients, respectively[42][45]. Recent mechanistic work has identified sphingosine kinase 1 as an early molecular node in this process[3][3], suggesting that therapies targeting this enzyme represent a promising avenue for preventive intervention during critical developmental windows.

Secondary genetic modifiers significantly influence phenotypic variability. Hemizygosity at the NCF1 gene, which encodes the p47phox subunit of NADPH oxidase and is sometimes included in the chromosomal microdeletion, provides a protective effect against hypertension by reducing angiotensin II-mediated oxidative stress in the vasculature[4]. Approximately 56% of WBS patients carry more than one copy of NCF1 and demonstrate a 4-fold increased risk of hypertension compared with those carrying only one functional allele[4].

### Multi-System Disease Manifestations and Their Therapeutic Implications

While cardiovascular disease dominates discussions of mortality risk in Williams syndrome, the disorder affects virtually every organ system and requires a comprehensive, multidisciplinary approach to management[7][21]. The cardiovascular, metabolic, endocrine, neurologic, gastrointestinal, genitourinary, dental, and ocular/auditory systems all demonstrate characteristic abnormalities requiring distinct therapeutic interventions[21][25]. Psychiatric comorbidities occur at elevated rates, with anxiety disorders affecting individuals at a 4-fold higher rate than neurotypical populations[18], and attention-deficit/hyperactivity disorder (ADHD), depression, and sleep disorders also showing significant prevalence[21][25]. The metabolic complications include hypercalcemia in approximately 15% of infants with WBS, which can progress to severe, refractory forms requiring multi-modal pharmacological intervention[14][32]. Endocrine abnormalities include subclinical hypothyroidism in 15-30% of patients and a dramatically elevated risk of glucose intolerance and type 2 diabetes in older patients, approaching 75% prevalence in some cohorts[23][26].

The clinical heterogeneity of Williams syndrome necessitates that pharmacological management be highly individualized and evidence-based. Importantly, the medication profile for WBS patients differs substantially from that of the general population due to the specific vulnerabilities of this cohort. For instance, stimulant medications used for ADHD carry increased risk in patients with structural cardiac abnormalities, and careful cardiac clearance is mandatory before initiating such therapy[21][21]. Similarly, selective serotonin reuptake inhibitors (SSRIs), while frequently used for anxiety management, carry the potential for QTc prolongation in susceptible patients—a particular concern given the elevated rates of cardiac dysrhythmias in WBS[21][21]. This requirement for medication vigilance is a defining characteristic of pharmaceutical management in this population.

## Approved and Established Drug Therapies

### Cardiovascular Management: Antihypertensive Pharmacotherapy

Hypertension affects 40-50% of WBS patients and represents a critical therapeutic target given its association with accelerated cardiovascular morbidity and sudden cardiac death[4][9]. Current guidelines do not recommend any antihypertensive agent class with exclusive preference; rather, multiple classes have demonstrated efficacy in managing blood pressure and arterial stiffness in WBS populations[8][8]. The three major antihypertensive classes employed clinically in WBS are angiotensin II receptor blockers (ARBs), beta-blockers, and calcium channel blockers[4][8][8].

Losartan, an ARB, has been studied extensively in both elastin-deficient mice (Eln+/− models) and human WBS patients[8][8]. In a chronic treatment study using elastin-insufficient mice, losartan produced significant reductions in systolic and diastolic blood pressure and also reduced pulse pressure, a critical marker of arterial stiffness[8][8]. The mechanistic basis for ARB efficacy relates to the central role of the renin-angiotensin-aldosterone system (RAAS) in WBS pathophysiology; elevated circulating renin levels are present in both Eln+/− mice and 40-50% of WBS patients, suggesting RAAS dysregulation[9][15]. Beyond blood pressure reduction, ARBs offer additional benefits. Angiotensin-converting enzyme inhibitors (ACEis), a related class that also blocks the RAAS, have demonstrated superior effects on endothelial function compared with other antihypertensive classes, with perindopril showing significantly greater improvement in flow-mediated endothelium-dependent vasodilation than beta-blockers, calcium channel blockers, and some ARBs in a prospective randomized trial of 168 hypertensive individuals[15]. ACEis show particular advantage in patients with heart failure and after myocardial infarction, though specific data in the WBS population remain limited[15].

Beta-blockers, particularly propranolol in experimental models and atenolol or carvedilol in clinical practice, represent a second major antihypertensive class. Beta-blockers are particularly attractive in WBS patients with certain cardiac lesions, as they reduce myocardial oxygen demand and can help prevent ischemia in patients with coronary artery stenosis, a feature present in 5-9% of WBS patients and up to 45% of those with concurrent supravalvular aortic stenosis[9][23]. However, recent comparative effectiveness data suggest that beta-blockers, including both first-generation agents like atenolol and newer third-generation agents like carvedilol and nebivolol, demonstrate inferior effectiveness compared with ACEis and thiazide/thiazide-like diuretics when used as monotherapy for hypertension in routine clinical practice[43].

Calcium channel blockers, particularly nicardipine and amlodipine, represent the third major class[8][8]. In elastin-deficient mice, nicardipine reduced systolic and diastolic blood pressure and pulse pressure to a degree comparable with losartan[8][8]. Calcium channel blockers may be particularly attractive in WBS patients given the prominent role of calcium dysregulation in the condition's pathophysiology and the link between infantile hypercalcemia and subsequent hypertension[4]. A multicenter study examining arterial stiffness in WBS patients found reduced pulse wave velocity (PWV) in individuals maintained on antihypertensives, though the study was underpowered to determine class superiority[8][8].

Pragmatically, many WBS patients require multidrug regimens for adequate blood pressure control[4]. Current clinical experience suggests that antihypertensive combinations targeting different pathways—such as an ACEi combined with a calcium channel blocker—often provide superior efficacy compared with monotherapy. Given the importance of controlling blood pressure to reduce sudden cardiac death risk and slow vascular disease progression, aggressive blood pressure management beginning in childhood is recommended for all hypertensive WBS patients[9].

### Cardiovascular Management: Surgical and Catheter-Based Interventions

While not pharmacological, surgical and catheter-based interventions form the backbone of definitive management for severe stenotic lesions and warrant brief mention as they define the context within which pharmacotherapy operates. Supravalvular aortic stenosis (SVAS), the most common cardiovascular lesion in WBS, presents in 87.5% of patients[42][45]. The natural history demonstrates concerning progression, with initially severe SVAS showing statistically significant worsening over time[42][45]. In contrast, branch pulmonary stenosis (found in 53.8% of patients) often spontaneously improves, particularly in childhood, with peak velocity of branch pulmonary stenosis decreasing from 3.08 to 1.65 m/s over time (P<0.001) even in patients with initially severe lesions[42][45].

Surgical repair remains the only definitive therapy for symptomatic SVAS, as transcatheter interventions are ineffective for this lesion[9]. Surgical outcomes are favorable, with 27.5% of a cohort of 80 WBS patients requiring disease-specific interventions over a median follow-up of 11 years, and notably, no perioperative mortality was observed in this series[42][45]. Current consensus recommendations specify surgical repair for severe SVAS or moderate SVAS accompanied by another lesion requiring surgery[9].

Transcatheter interventions for peripheral pulmonary artery stenosis have been employed at many centers, though long-term follow-up data reveal problems specific to WBS. Significant and rapid restenosis in stented segments is common due to an intense neointimal hyperplastic response—a manifestation of the abnormal smooth muscle proliferation that characterizes WBS pathophysiology[9]. The use of drug-coated balloons has been reported for successful redilation of in-stent stenosis in individual WBS cases[9]. Current consensus recommendations against routine stenting for peripheral pulmonary artery stenosis in WBS reflect the poor long-term outcomes with this approach[9].

### Medical Management of Supravalvular Aortic Stenosis and Coronary Artery Disease

In patients with coronary artery stenosis, a devastating complication found in a minority of patients but conferring substantial risk of sudden cardiac death, medical management is limited to beta-blockers to decrease myocardial oxygen demand, ischemic risk, and arrhythmia while awaiting surgical intervention[9]. Antiplatelet therapies are explicitly not warranted for coronary artery pathology in WBS, despite being standard in coronary artery disease in the general population[9]. Long-term antiplatelet therapy (aspirin or clopidogrel) is typically administered for at least 6 months following coronary artery repair, with additional imaging (CT/CMR or cardiac catheterization) recommended 1 year after repair if clinical or echocardiographic concerns persist[9].

### Management of Hypercalcemia and Hypercalciuria

Hypercalcemia represents a significant comorbidity in WBS, with approximately 15% of infants presenting with elevated serum calcium levels and approximately 5% developing severe hypercalcemia sufficient to cause nephrocalcinosis[14][32][14]. The mechanisms underlying hypercalcemia in WBS remain incompletely understood but are thought to involve increased intestinal absorption and/or decreased renal clearance of calcium, with high serum 1,25-dihydroxyvitamin D levels potentially playing a contributory role[28][32].

The first-line pharmacological approach to hypercalcemia in WBS involves conservative management using intravenous (IV) hydration with isotonic saline and dietary calcium restriction[5][32]. This strategy remains the initial intervention for most patients[14]. Furosemide, a loop diuretic, is administered in parallel with IV hydration to enhance urinary calcium excretion, typically at a dose of 1 mg/kg administered as frequently as every 12 hours[32]. However, careful attention to electrolyte replacement and avoidance of hypokalemia is essential[5][14][32].

In patients with persistent or refractory hypercalcemia despite these first-line interventions, several additional pharmacological agents have proven effective. **Pamidronate, a bisphosphonate compound**, has emerged as the most consistently effective second-line agent. Pamidronate inhibits osteoclast-mediated bone resorption and has been documented to produce rapid and substantial reductions in serum calcium in WBS patients refractory to conventional therapy[5][5][32]. A typical pamidronate regimen involves intravenous infusion at a dose of 1 mg/kg, which typically produces serum calcium reduction from severely elevated levels (13.9-17.7 mg/dL) to normal or near-normal ranges (9.5-9.9 mg/dL) within 24-72 hours[5][5]. The duration of the hypocalcemic effect typically lasts 2-4 weeks, and doses may be repeated at 1-month intervals if hypercalcemia recurs[5][32]. To date, at least five WBS patients with refractory hypercalcemia have been successfully treated with bisphosphonate infusions, establishing this agent class as evidence-based therapy for severe cases[5][5].

Important considerations regarding pamidronate use in the pediatric WBS population include potential nephrotoxicity (warranting caution in patients with renal impairment), the risk of acute-phase reactions (manifesting as low-grade fever, headache, nausea, vomiting, rash, tachycardia, myalgia, and bone pain), and the possibility of hypocalcemia and hypophosphatemia development, necessitating baseline and post-treatment laboratory monitoring of serum calcium and phosphorus[5][5][32]. The lack of FDA approval for bisphosphonate use in the pediatric population means that pamidronate should be considered a second-line therapy, used only when first-line interventions fail and employed with full informed consent regarding off-label use[5][5].

Glucocorticoids, such as methylprednisolone at a dose of 2 mg/kg/day, have also been employed in acute hypercalcemia management[32]. These agents work through suppression of intestinal calcium absorption and inhibition of calcitriol production. Glucocorticoids are most effective in treating hypercalcemia related to inflammation but have been utilized in WBS patients[32]. However, prolonged glucocorticoid therapy must be avoided due to risks of cushingoid features, osteoporosis, and iatrogenic adrenal suppression[32].

Calcitonin, a hormone that inhibits osteoclast activity, has been administered at doses of 2-4 U/kg as frequently as every 12 hours[32]. The hypocalcemic effect of calcitonin is rapid (4-6 hours) but short-lived (48 hours), as tachyphylaxis rapidly develops, limiting its utility for sustained management[32]. Given these limitations, calcitonin is now rarely used as a primary agent but may be considered in acute settings where rapid calcium lowering is needed.

Dietary calcium restriction to the dietary reference intake (DRI) for age represents a key component of chronic management[31]. However, indefinite long-term calcium restriction is cautioned against due to the risk of developing osteopenia and osteoporosis in adulthood, which may increase bone fracture risk[28][31]. The contemporary recommendation from specialized WBS centers is not to restrict calcium intake in children with WBS and normal blood and urine calcium levels, and to avoid dropping calcium intake below the DRI even in children with mildly elevated calcium levels[31]. Vitamin D supplementation is generally not recommended in WBS, though breastfed infants should be monitored for signs of vitamin D deficiency and rickets, and vitamin D supplementation may be recommended in specific cases with appropriate monitoring[31].

### Psychiatric and Behavioral Pharmacotherapy

Psychiatric comorbidities are remarkably common in Williams syndrome, with anxiety disorders affecting the population at a 4-fold increased rate compared with neurotypical controls[18]. Attention-deficit/hyperactivity disorder, depression, and sleep disorders also occur at elevated rates[21][25]. The psychopharmacological management of these conditions in WBS requires careful consideration of the specific medical comorbidities present in this population and their potential interactions with psychotropic medications.

Selective serotonin reuptake inhibitors (SSRIs) represent first-line agents for anxiety and depressive disorders in WBS. Among a series of five children treated with SSRIs for anxiety, two no longer met criteria for an anxiety disorder at follow-up, and four of five were rated as "much improved" on the Clinical Global Impression scale[21][21]. Common SSRIs employed include citalopram (typically at doses of 10-20 mg daily, substantially lower than the 20-60 mg range used in adults), sertraline, and fluoxetine[19][49]. However, SSRI use in WBS requires particular vigilance due to several WBS-specific concerns. First, citalopram has been associated with QTc interval prolongation in the general population—a concern of particular relevance to WBS patients given their elevated baseline risk of QTc prolongation and cardiac dysrhythmias[21][21]. Second, SSRIs can occasionally trigger hypomanic or manic reactions in individuals with undiagnosed bipolar spectrum illness, an observation documented in at least one adult WBS patient who experienced a brief hypermanic episode with increased uninhibited sexual behavior and irritability following SSRI initiation[19]. Third, potential adverse effects of SSRIs relevant to the WBS population include metabolic risk, QTc prolongation, increased falls/fracture risk (concerning in a population with skeletal fragility issues), and mildly increased blood pressure—all comorbidities of concern in this population[21][21][25].

When SSRIs are employed, lower-than-standard doses are recommended, typically one-quarter to one-half of standard adult dosing[19][25]. Concurrent low-dose antipsychotic therapy has emerged as a particularly effective adjunctive strategy. In two adult WBS patients treated with combination SSRI plus low-dose sedative antipsychotic (levomepromazine), marked improvement in anxiety, mood regulation, self-control, mental concentration, social skills, and sleep quality was achieved[19]. Treatment led to decreased irritability and aggressiveness and stabilization of mood, with one patient achieving "remarkable improvement in autonomy and social skills since the beginning of treatment"[19]. Current recommendations suggest that the combination of SSRIs at reduced doses with low doses of low-potency antipsychotics (such as levomepromazine) represents the most suitable medication regimen for managing generalized anxiety disorder in individuals with Williams syndrome[19][25].

Importantly, manic reactions and increased anxiety must be closely monitored during SSRI treatment, and control of anxiety and sleep should be prioritized as preventive measures[19][25]. The lack of prospective psychiatric medication trials in WBS means that most recommendations are based on clinical experience and case series, underscoring the need for careful individual titration and monitoring[21][25][25].

Regarding stimulant medications for ADHD, which affects a substantial proportion of the WBS population, use requires extreme caution. Stimulants carry an FDA warning regarding the association between sudden death and stimulant treatment in youth with structural cardiac abnormalities[21][21]. Given that approximately 80% of WBS patients have cardiovascular abnormalities[9], stimulant use should be restricted to patients with thoroughly cleared cardiac status. When stimulants are considered, collaboration with the patient's cardiologist is essential to ensure recent cardiology follow-up with electrocardiogram and echocardiography[21][21]. Vital signs should be monitored prior to initiating stimulant therapy and periodically thereafter, and growth (particularly in children) should be monitored regularly due to appetite suppression[21][21]. Among children with WBS treated with stimulants, the most common adverse effects reported include irritability (38%), "zoning out" (31%), anxiety (29%), and weight loss (28%)[21][21]. Notably, sadness was reported as a striking and dramatic adverse effect in 61% of one small series, which is atypical compared with stimulant effects in the general population and may warrant particular attention[21].

### Sleep Disorder Management

Sleep disturbances occur at very high rates in Williams syndrome, with multiple investigations documenting frequent nighttime awakenings, prolonged sleep latency, restless sleep, and excessive daytime sleepiness[46][48]. These sleep problems are not incidental; they correlate with poorer cognitive and behavioral outcomes and developmental progression[46][48]. However, treatment approaches must be carefully calibrated given the complex medical comorbidities of WBS and the unique medication sensitivities of this population.

Behavioral interventions represent the first-line approach and should be attempted before pharmacological therapy[46][48]. These interventions include establishing consistent bedtime and wake times, maintaining good sleep hygiene, using visual schedules for bedtime routines, limiting screen time before bed (to preserve melatonin production), and helping the child learn to associate the bed with sleep[46][48].

When behavioral measures prove insufficient, pharmacological therapy may be considered as an adjunct. Melatonin has emerged as the most commonly used sleep medication in WBS, with 67% of a sample of WBS individuals who used sleep medications having tried melatonin, and 91% reporting the medication as "helpful" or "somewhat helpful"[48][49]. Melatonin carries very few reported side effects in this population, making it an attractive first-line pharmacological agent. Typical melatonin dosing ranges from 0.5 to 10 mg at bedtime, with individual titration required[48].

For children with sleep maintenance insomnia or restlessness, clonidine (typically 0.05-0.1 mg at bedtime) or clonidine extended-release has shown efficacy[48]. Clonidine is an alpha-2 adrenergic agonist with hypotensive properties, requiring blood pressure monitoring. Other agents used in WBS populations for sleep disturbance include mirtazapine (a noradrenergic and specific serotonergic antidepressant), trazodone, and occasionally low doses of atypical antipsychotics such as quetiapine[48][49]. Antihistamines, particularly diphenhydramine, have been used in approximately 29% of WBS individuals taking sleep medications; however, 18% of those taking diphenhydramine reported behavioral and neurological side effects, limiting their attractiveness compared with other options[48][49]. Notably, no pediatric FDA-approved sleep aid for insomnia exists, and all sleep medications used in children are off-label, underscoring the need for careful informed consent and monitoring[48].

The underlying causes of sleep disturbance in WBS must also be addressed. Sleep-disordered breathing and obstructive sleep apnea occur at elevated rates in WBS and may require polysomnographic evaluation and specific treatment such as adenotonsillectomy[46][48]. Restless leg syndrome, potentially related to iron metabolism abnormalities, may respond to iron supplementation targeted to achieve a serum ferritin >75 ng/mL[48].

## Investigational and Pipeline Drugs in Active Clinical Trials

### Clemastine Fumarate for Neurodevelopmental Delays (Phase III)

A landmark Phase III clinical trial currently in active recruitment represents the most advanced investigational drug program for Williams syndrome. This trial (NCT06315699) evaluates clemastine fumarate, an FDA-approved first-generation antihistamine, for the treatment of neurodevelopmental delays in children with Williams syndrome ages 3-6 years[1][13][1][13]. The study is being conducted at Qilu Hospital of Shandong University in China and focuses on reversing brain myelin defects caused by GTF2I gene haploinsufficiency.

**Clemastine fumarate** is a selective H1-receptor antagonist that crosses the blood-brain barrier and has been used clinically for allergic conditions and pruritus for decades[1][13][1][13]. The investigation of clemastine in WBS represents a repurposing approach based on preclinical evidence that H1-receptor antagonism may promote oligodendrocyte maturation and myelin formation in the central nervous system, potentially addressing the cognitive and motor delays characteristic of WBS[1][13][1][13]. The trial employs a randomized, double-blind, placebo-controlled crossover design with 50 participants divided into two groups. Group A receives clemastine at a weight-dependent dose in the first cycle and placebo in the second cycle, while Group B receives the reverse sequence[1][13][1][13]. The primary objectives are to evaluate the initial efficacy and safety of clemastine fumarate for treating cognitive, motor, and social impairments in Williams syndrome and to investigate the mechanisms of action and safety of the agent[1][13][1][13].

Inclusion criteria specify: age 3-6 years; positive fluorescence in situ hybridization (FISH) confirmation of Williams syndrome; GTF2I gene mutation detected by whole exon sequencing; and normal heart safety variables including normal ECG and blood pressure 120-129/80-84[1][13][1][13]. Exclusion criteria encompass WBS patients with other gene mutations; prior use of antihistamines, monoamine oxidase inhibitors, barbiturates, sedatives, or drugs affecting cognitive behavior, limb movement, white matter myelin within 2 months prior to enrollment; patients with narrow-angle glaucoma, peptic ulcer disease, or obstructed urinary outflow; patients allergic to clemastine or other arylalkylamine antihistamines; presence of obvious brain lesions unrelated to WBS on MRI; and clinically significant comorbid disease affecting study interpretation or patient safety[1][13][1][13]. The careful attention to cardiac safety in eligibility criteria reflects the heightened cardiovascular risk profile of this population.

### Combination Antihypertensive Trial (Phase III)

An additional Phase III clinical trial (NCT06643130) is recruiting participants to evaluate the efficacy and safety of a combination antihypertensive regimen (JW0104+C2402) in patients with hypertension and dyslipidemia, with Williams syndrome specifically listed as a recruitment indication[12]. This multi-center, randomized, double-blind, parallel-group trial is sponsored by JW Pharmaceutical and is comparing combination therapy (JW0104+C2402) with either JW0104+C2403 combination or C2402 monotherapy[12]. The trial anticipates enrollment of 162 participants and is scheduled for completion by April 30, 2026[12]. While not Williams-syndrome-specific in design, this trial may provide additional evidence regarding optimal antihypertensive combinations for WBS patients, who commonly require multidrug regimens for adequate blood pressure control.

## Drug Repurposing Candidates with Supporting Evidence

### Potassium Channel Openers: Minoxidil

Minoxidil, a potassium channel opener vasodilator marketed for oral treatment of resistant hypertension and topical treatment of male-pattern baldness, has emerged as a compelling repurposing candidate for Williams syndrome based on preclinical evidence and limited clinical experience. The theoretical basis for minoxidil efficacy in WBS derives from its ability to stimulate elastogenesis—the synthesis and deposition of elastin—in vascular smooth muscle cells and fibroblasts in a dose-dependent manner[10][33]. In hypertensive animal models (Brown Norway rats, which naturally exhibit low elastin levels), minoxidil increased elastin levels in mesenteric, abdominal, and renal arteries through mechanisms involving decreased elastase enzyme activity[10][33]. Mechanistically, potassium channel openers suppress calcium influx, which inhibits elastin gene transcription through suppression of the extracellular signal-regulated kinase 1/2 (ERK1/2)-activator protein 1 signaling pathway[10][33]. ERK1/2 activation increases elastin gene transcription and enhances cross-linked elastic fiber synthesis by smooth muscle cells while decreasing the number of cells in the aorta[10][33].

A randomized controlled double-blind study assessed the effect of minoxidil on common carotid artery intima-media thickness (IMT), a surrogate marker of vascular remodeling and stiffness, in children and adolescents with WBS[33]. The study found a slight increase in IMT after 12 and 18-month follow-up, suggesting minimal direct benefit on arterial wall remodeling[33]. However, the authors proposed that more understanding of the biological changes induced by minoxidil is required to better explain its potential role in elastogenesis in Williams syndrome, and they noted that the low dose of minoxidil used in children compared with doses employed in animal studies, combined with the shorter duration of exposure relative to children's lifespans, may account for the inconclusive results[33]. While this initial clinical trial did not demonstrate robust efficacy, minoxidil remains a candidate for further investigation, potentially at higher doses or with longer treatment duration, particularly if administered during critical developmental windows when elastin synthesis is most active.

### Rapamycin (mTOR Inhibitor)

Rapamycin, an immunosuppressive agent that inhibits the mammalian target of rapamycin (mTOR) kinase, has demonstrated remarkable preclinical efficacy in reducing vascular smooth muscle cell proliferation and aortic obstruction in elastin-deficient mouse models. Research by multiple investigators has outlined the dramatic increase in vascular smooth muscle cell numbers that occurs in elastin-deficient arteries, particularly during the postnatal period[10][10]. Increased signaling of the mTOR pathway has been documented in elastin-deficient murine aortas and in elastin-haploinsufficient vascular smooth muscle cells isolated directly from patients with supravalvular aortic stenosis and Williams syndrome[34][38].

In experimental systems, rapamycin treatment at 10 ng/mL dosing inhibited prolonged mTOR activation and suppressed enhanced proliferation of smooth muscle cells derived from WBS patients[34][38]. In vivo, rapamycin treatment reduced aortic obstruction in elastin-deficient mice, demonstrating the principle that pharmacological mTOR inhibition can ameliorate the vascular pathology of elastin insufficiency[34][38]. However, important caveats emerged from these studies. Rapamycin did not prolong the survival of completely elastin-deficient (Eln−/−) pups, and it retarded somatic growth of juvenile Eln+/− and wild-type littermates[34][38]. These findings indicate that while mTOR inhibition represents a promising pharmacological strategy to reduce smooth muscle cell proliferation and aortic obstruction attributable to elastin deficiency, significant challenges remain, particularly the need to improve therapeutic efficacy without producing substantial somatic growth retardation before clinical trials in children with WBS can be justified[34][38].

### MicroRNA-29 Inhibitors

MicroRNA-29 (miR-29) family members represent compelling therapeutic targets for elastin insufficiency based on their role as negative regulators of elastin expression. The miR-29 family has 14 binding sites within elastin (ELN) exons and the 3' untranslated region, making it a particularly significant regulator of elastin production[10][10]. Mechanistically, binding of miR-29 to the elastin transcript causes translational repression and mRNA degradation, thereby suppressing elastin protein production[10][10]. Consequently, inhibition of miR-29 would be expected to increase ELN transcript translation and protein production.

This hypothesis was tested experimentally by Zhang and colleagues, who treated cells and engineered vessels derived from WBS patients with miR-29 inhibitors[10][10]. When cells were treated with miR-29 mimics, ELN transcript levels decreased; conversely, treatment with miR inhibitors increased ELN expression levels above untreated control levels and resulted in increased elastin accumulation in the extracellular matrix[10][10]. These findings establish proof-of-concept that miR-29 inhibition can augment elastin deposition in WBS disease models and potentially reverse elastin deficiency at the molecular level.

However, significant challenges limit the clinical applicability of anti-miR-29 therapeutics. The miR-29 family regulates multiple targets beyond elastin, including fibronectin, laminin, integrin-B1, multiple collagen types, and matrix metalloproteinase-2, with the family heavily implicated in fibrosis regulation[10][10]. Consequently, use of anti-miR-29 therapeutics requires the ability to tune the miRNA manipulation to specific target mRNAs of interest and to deliver the medication specifically to the vascular tissues where enhanced elastin deposition is desired, so as to avoid inappropriate extracellular matrix production in tissues where it would be pathological[10][10]. These delivery and specificity challenges remain formidable obstacles to clinical translation at present.

### Sphingosine Kinase 1 Inhibition

Recent mechanistic research conducted at Yale School of Medicine has identified sphingosine kinase 1 as a critical early node in the cascade of molecular changes triggered by elastin deficiency. A study published in Nature Cardiovascular Research by Daniel Greif's laboratory discovered that sphingosine kinase 1, a specific enzyme involved in lipid signaling, causes excess smooth muscle cells to proliferate in response to elastin deficiency[3][3][3]. Importantly, sphingosine kinase 1 is altered early in the developmental process, with changes to downstream pathways including NOTCH3 and integrin β3 occurring later[3][3][3]. This temporal sequencing suggests that sphingosine kinase 1 may represent a node that causes downstream changes, making it a strong candidate for therapeutic targeting[3][3][3].

The authors conclude: "Based on our research, we think sphingosine kinase 1 may represent a node that causes those other changes, making it a strong candidate for additional research, and in the future, a possible treatment target to help people with Williams-Beuren syndrome"[3][3][3]. While no specific sphingosine kinase 1 inhibitors have yet entered clinical trials for WBS, this discovery has profound implications for future drug development efforts, as targeting this early molecular node during critical developmental windows might prevent the cascade of vascular smooth muscle proliferation that establishes the foundation for arterial stenosis.

### NOTCH3 Pathway Modulation

NOTCH3 signaling has emerged as another critical mediator of vascular smooth muscle cell expansion and loss of vascular patency in the context of elastin insufficiency. Researchers identified vascular smooth muscle cell-derived NOTCH3 signaling as a critical mediator of aortic hypermuscularization and loss of vascular patency using a combination of gene-targeted mice, tissues and cells from WBS patients, and targeted elastin manipulation in human VSMCs[40][44]. VSMC-specific loss of JAG1 (the NOTCH3 ligand) or global loss of NOTCH3 reversed the occlusive phenotype seen in Eln−/− mice, establishing NOTCH3 as a therapeutic target[40][44].

However, the path to NOTCH3-targeting therapeutics is complex. Gamma-secretase inhibitors, which inhibit NOTCH processing and signaling broadly, were initially investigated in Alzheimer's disease but encountered undesirable side effects related to widespread inhibition of NOTCH signaling[40][44]. These agents are now primarily being studied in malignancies with known NOTCH involvement[40][44]. The challenge of achieving specific NOTCH3 inhibition while avoiding off-target effects has driven investigation of alternative approaches, including monoclonal antibodies that target specific NOTCH receptors or ligands[40][44]. Many such antibodies show promise in preclinical models with reduced side-effect profiles compared with broad NOTCH inhibitors[40][44]. For example, a NOTCH3 antibody-drug conjugate (PF-06650808) has demonstrated benefit in patients with NOTCH3-positive breast cancer[40][44].

Other NOTCH-targeting therapeutics currently undergoing clinical trial evaluation include gamma-secretase modulators, synthetic chemicals, molecular decoys, SERCA inhibitors (which inhibit NOTCH1 in addition to their primary targets), modulators of NOTCH glycosylation, and microRNAs[40][44]. The prospect of selective NOTCH3 inhibition combined with therapeutics specifically targeting vascular smooth muscle cells offers potential to revolutionize cardiovascular disease management in WBS with minimal side effects compared with currently available broad-spectrum NOTCH inhibitors[40][44].

### Integrin β3 Blockade

Integrin β3 has been identified as a downstream mediator of elastin insufficiency-induced vascular pathology. Research demonstrated increased β3 integrin expression in Eln+/− aortas and in tissue taken directly from WBS patients[10][10]. When Eln mutants were raised in a β3 null genetic background, alterations in the mechanosensor response to elastin insufficiency were observed[10][10]. While integrin β3 blockers have not yet entered clinical trials specifically for WBS, the availability of approved integrin β3 antagonists, such as cilostazol and abciximab (though these were developed for other indications), raises the possibility that existing agents could be repurposed if preclinical work establishes efficacy in WBS models.

### Endothelin Receptor Antagonists

Endothelin-1 (ET-1) is a potent vasoconstrictor that exerts mitogenic effects on vascular cells and may contribute to excessive smooth muscle proliferation in WBS. Endothelin receptor antagonists (ERAs), including bosentan, ambrisentan, and the newer dual endothelin receptor antagonist aprocitentan, improve exercise capacity, functional class, and hemodynamics in pulmonary arterial hypertension[22][35][39]. Given that pulmonary arterial hypertension represents a potential complication in some WBS patients and that ET-1-mediated vasoconstriction may be involved in arterial stenosis pathophysiology, ERAs have theoretical appeal as WBS therapies.

However, use of current ERAs is limited by adverse effects. Endothelin receptor antagonists carry risks of acute hepatotoxicity, anemia, and fluid retention[22]. Additionally, bosentan carries a teratogenicity warning and requires monthly liver function tests and dual contraception use in reproductive-age females[22]. The recent PRECISION trial demonstrated that aprocitentan, a newer dual endothelin receptor antagonist, produces significant blood pressure lowering effects in resistant hypertension with a better tolerability profile than earlier agents, though fluid retention remains a potential issue requiring close clinical monitoring[39]. If mechanical testing in WBS disease models establishes efficacy with acceptable tolerability, ERAs might be considered for clinical trial evaluation in WBS patients with severe arterial disease.

### Inhaled Nitric Oxide

Inhaled nitric oxide (iNO) represents an intriguing candidate for WBS patients with pulmonary arterial hypertension or other forms of pulmonary vascular disease. Nitric oxide is a selective pulmonary vasodilator that decreases pulmonary artery pressure and pulmonary vascular resistance through activation of soluble guanylyl cyclase and downstream increases in cyclic guanosine monophosphate (cGMP), which causes vasorelaxation and inhibits pulmonary artery smooth muscle cell proliferation[22][24]. The recommended dose for neonates with hypoxic respiratory failure associated with pulmonary hypertension is 20 ppm delivered via constant concentration during inspiration for up to 14 days or until hypoxia resolves[22].

Long-term (>1 month) pulsed inhaled nitric oxide dosing has shown favorable effects on pulmonary hemodynamics in small series of patients with idiopathic pulmonary arterial hypertension and has been explored as a bridge to lung transplantation and as an adjunctive therapy to currently approved PAH drugs[22]. Important concerns include potential for methemoglobinemia formation at high doses (though doses <100 ppm generally avoid this complication), as well as reports of severe epistaxis and hypotensive bradycardia upon attempted weaning in individual patients[22]. While iNO has not been specifically studied in WBS-related pulmonary hypertension, it represents a potential future application worth investigating in WBS patients who develop pulmonary vascular disease.

### Sildenafil and Other PDE-5 Inhibitors

Phosphodiesterase-5 (PDE-5) inhibitors such as sildenafil have been studied in pulmonary arterial hypertension based on their mechanism of increasing cyclic guanosine monophosphate (cGMP) by preventing its degradation, thereby achieving vasodilation and antiproliferative effects similar to those of nitric oxide[17]. The SERAPH trial compared sildenafil added to conventional treatment with bosentan in 26 patients with pulmonary arterial hypertension of World Health Organization functional class III, finding that sildenafil-treated patients who completed the protocol showed significant reductions in right ventricular mass and plasma brain natriuretic peptide levels, improvements in 6-minute walk distance and cardiac index, and improvement in systolic left ventricular eccentricity index[17]. While not specifically studied in WBS, sildenafil might represent a therapeutic option for WBS patients who develop pulmonary hypertension as a complication of their arterial disease.

### Endocannabinoid System Modulation

An emerging mechanistic discovery in WBS pathophysiology involves the endocannabinoid system, which is known to regulate both cardiovascular function and cognition. Researchers using a mouse model of WBS that mimics the genetic alteration causing the disease evaluated the endocannabinoid system as a potential drug target[47]. The team identified alterations to the endocannabinoid system in WBS mice, with specific changes to the CB1 receptor[47]. They then treated mice with JZL184, an investigational therapy designed to increase endocannabinoid levels through monoacylglycerol lipase inhibition[47].

Over a 10-day treatment period, mice showed modulation of their endocannabinoid systems and normalization of the alterations in the CB1 receptor[47]. Strikingly, this resulted in improvements in multiple WBS phenotypes, including heart hypertrophy, hypertension, hypersociability, and memory impairment[47]. In control mice, the treatment had no effects, indicating specificity of the approach to WBS[47]. Gene expression analysis revealed that approximately 70% of genes affected by Williams syndrome in the heart returned to normalized levels of expression following JZL184 treatment[47]. The researchers concluded that modifying the endocannabinoid system could be key to more effectively treating Williams syndrome[47].

While these findings are preliminary and limited to animal models, they represent a significant conceptual advance in understanding WBS pathophysiology and provide a rationale for future clinical investigation of endocannabinoid system-modulating agents. However, no clinical trials evaluating such agents in WBS patients have been registered to date. Cannabis itself and related cannabinoid compounds are areas of active regulatory scrutiny and clinical investigation in many jurisdictions, and clinical translation of these findings may require careful navigation of legal and regulatory frameworks.

## Contraindications and Safety Considerations

### Cardiac Safety Concerns with Psychiatric Medications

As discussed earlier, stimulant medications for ADHD carry substantial risks for sudden cardiac death in patients with structural cardiac abnormalities. Given that approximately 80% of WBS patients have cardiovascular anomalies, including stenotic lesions that increase ischemic risk, stimulant medications require exceptional caution[9][21][21]. FDA warning labels specifically note this association, and current recommendations mandate cardiology clearance, including electrocardiography and echocardiography, before stimulant initiation in WBS patients[21][21].

Similarly, SSRIs and serotonin-norepinephrine reuptake inhibitors (SNRIs), while generally first-line agents for anxiety management in WBS, carry the potential for QTc interval prolongation—a concern of particular relevance in a population with baseline susceptibility to cardiac dysrhythmias and sudden death[21][21][25]. Citalopram has been associated with dose-dependent QTc prolongation in the general population, and this risk must be weighed against its demonstrated utility for anxiety in WBS[21][21]. Baseline ECG evaluation before SSRI/SNRI initiation and periodic monitoring are prudent practices.

### Antipsychotics and Metabolic Risk

While low-dose antipsychotics have demonstrated utility as adjuncts to SSRIs for anxiety management in WBS, these agents carry risks of extrapyramidal side effects, neuroleptic malignant syndrome, and prolonged QTc interval that must be weighed against their benefits[21][21][25]. Additionally, certain antipsychotics carry metabolic risks including weight gain and potential worsening of glucose intolerance—complications to which WBS patients are already vulnerable, as glucose intolerance and diabetes affect up to 75% of older WBS patients[23][26][25].

### Anesthetic Considerations and Perioperative Risk

The anesthetic management of WBS patients deserves particular mention, as numerous case reports and series have documented sudden cardiac death during anesthesia and perioperative periods in WBS patients with uncorrected or incompletely recognized cardiac abnormalities[23][26]. The perioperative mortality rate in one series of 108 WBS patients undergoing anesthesia was 0.9%, with cardiac arrest occurring at a rate of 1.85% and the overall cardiac complication rate reaching 11.1%, with bradycardia and hypotension being the most common complications[23]. Risk of sudden death during anesthesia may be 25-100 times higher than in the general population, with approximately 50% of reported deaths associated with cardiac catheterization cases[23].

Preoperative cardiac optimization is essential, including four-extremity blood pressure measurements to screen for regional stenoses, ECG evaluation for arrhythmias and QTc prolongation, and comprehensive echocardiography with consideration of cardiac catheterization in high-risk patients[23]. Intraoperative management must emphasize minimization of tachycardia and hypotension, maintenance of adequate preload, preservation of normal sinus rhythm and systemic vascular resistance, and avoidance of increases in pulmonary vascular resistance[23]. These considerations are relevant not only to surgical procedures but also to any situation requiring sedation or anesthesia, including dental procedures, imaging studies, and other interventions.

## Emerging Therapeutic Approaches and Future Directions

### Gene Therapy and Genetic Approaches

An alternative therapeutic strategy on the horizon involves targeted upregulation of a patient's own genes through in situ gene therapy. This approach has been accomplished for HIF-1α in a patient with peripheral artery disease as proof-of-concept[4]. Williams syndrome presents a particularly compelling model for this treatment approach because approximately 26-28 genes are deleted in WBS patients, yet their normal chromosome 7 homolog contains an intact copy of each allele[4]. If transcriptional control of normal ELN gene splice variants can be retained, in situ gene therapy specifically targeting ELN upregulation might theoretically circumvent elastin deficiency and prevent vascular disease progression[4]. Such approaches remain experimental and far from clinical application, but they represent a tantalizing long-term possibility.

An alternative genetic approach would involve genetic modification of autologous progenitor or already differentiated vascular cells ex vivo, followed by reintroduction into the affected individual[4]. However, formidable challenges of appropriate delivery and integration into sites of vascular lesions present substantial technical obstacles to this approach.

### Regulatable Elastin Expression Systems

Second-generation animal models with regulatable elastin expression in the vessel wall may resolve important questions about whether re-expression of elastin, or administration of antiproliferative smooth muscle cell pharmacotherapy, can reverse already-formed vascular lesions[4]. Such information would substantially inform therapeutic strategy, as it would establish whether interventions are most effective if begun during critical developmental windows or whether they can salvage existing established disease.

## Conclusion

Williams syndrome represents one of the most challenging multisystem genetic disorders encountered in clinical medicine, requiring comprehensive individualized management that addresses cardiovascular disease, metabolic dysfunction, psychiatric comorbidities, and developmental needs simultaneously. While no curative therapy exists, multiple pharmacological strategies have demonstrated efficacy in managing specific disease manifestations. Antihypertensive medications, particularly angiotensin-converting enzyme inhibitors and angiotensin II receptor blockers, form the foundation of cardiovascular medical management, with surgical intervention reserved for severe obstructive lesions. Hypercalcemia management relies on first-line conservative measures supported by evidence-based escalation to bisphosphonates for refractory cases. Psychiatric and behavioral symptoms respond to carefully titrated combinations of SSRIs and low-dose antipsychotics, administered at substantially reduced doses compared with general population standards.

The landscape of investigational agents for Williams syndrome has expanded dramatically in recent years, with clemastine fumarate now in Phase III trials for neurodevelopmental improvement, and multiple molecular pathways—including sphingosine kinase 1, NOTCH3, integrin β3, and the endocannabinoid system—identified as potential future therapeutic targets. Drug repurposing approaches offer particular promise for rapid clinical translation, as seen with potassium channel openers like minoxidil and mTOR inhibitors like rapamycin, which have demonstrated preclinical efficacy in reducing vascular smooth muscle proliferation and aortic obstruction. However, challenges related to somatic growth retardation and other systemic effects must be resolved before some of these agents can advance to clinical trials in children.

The clinician managing Williams syndrome patients must maintain vigilance regarding the unique medication sensitivities of this population, including heightened cardiac risks with stimulant medications, QTc prolongation concerns with certain SSRIs and antipsychotics, and the necessity for cardiac safety clearance before initiating medications affecting cardiac function. Individualization of therapy, multidisciplinary collaboration involving cardiology, psychology, endocrinology, nephrology, and other specialties, and long-term longitudinal follow-up are essential components of optimal care. Future therapeutic advances will likely emerge from continued mechanistic research elucidating the molecular pathways perturbed by elastin insufficiency and from clinical translation of promising repurposing candidates and novel molecular targets. In the interim, evidence-based management of complications combined with careful medication selection and monitoring can substantially improve quality of life and long-term outcomes for individuals with this complex and fascinating genetic disorder.