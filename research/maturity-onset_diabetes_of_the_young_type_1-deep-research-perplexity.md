---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:40:06.543413'
end_time: '2026-04-04T14:42:21.709210'
duration_seconds: 135.17
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: maturity-onset diabetes of the young type 1
  mondo_id: MONDO:0007452
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
- **Disease Name:** maturity-onset diabetes of the young type 1
- **MONDO ID:** MONDO:0007452

## Research Objectives

Please provide a comprehensive research report on **drug therapies for maturity-onset diabetes of the young type 1**.
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

# Comprehensive Research Report on Drug Therapies for Maturity-Onset Diabetes of the Young Type 1 (HNF4A-MODY)

**MODY type 1, caused by mutations in the hepatocyte nuclear factor-4 alpha (HNF4A) gene, represents a distinct monogenic form of diabetes characterized by impaired pancreatic beta-cell function that is remarkably responsive to sulfonylurea therapy.** This report synthesizes current evidence on approved, investigational, and repurposed drug treatments for this rare but clinically significant condition. Sulfonylureas remain the gold-standard first-line pharmacologic therapy, with established protocols for low-dose initiation and gradual titration to minimize hypoglycemia risk. Recently emerging evidence supports the use of glucagon-like peptide-1 receptor agonists and dual GIP/GLP-1 receptor agonists as effective alternatives or adjunctive therapies, particularly in patients with suboptimal glycemic control or those experiencing hypoglycemic episodes on sulfonylureas. Insulin therapy serves as second-line treatment when oral agents fail. This comprehensive review examines the molecular basis for drug responsiveness in HNF4A-MODY, documents the clinical evidence supporting various therapeutic approaches, and identifies areas requiring further investigation.

## Pathophysiology and Molecular Basis for Drug Responsiveness in HNF4A-MODY

Understanding the underlying pathophysiology of HNF4A-MODY is essential for comprehending why certain pharmacological interventions are particularly effective while others demonstrate limited utility. The HNF4A gene encodes hepatocyte nuclear factor-4 alpha, a nuclear transcription factor that plays a crucial regulatory role in both hepatic and pancreatic beta-cell development and function.[8][12] Mutations in HNF4A lead to a loss of function of the encoded protein, resulting in impaired regulation of gene expression in pancreatic beta cells and hepatocytes. This molecular defect creates a specific pattern of glucose-stimulated insulin secretion dysfunction that distinguishes HNF4A-MODY from other forms of monogenic diabetes and explains the remarkable responsiveness to certain drug classes.

The fundamental deficit in HNF4A-MODY involves impaired glucose sensing at the level of the pancreatic beta cell.[1][8] Normal glucose-stimulated insulin secretion requires intact glucose uptake and metabolism within the beta cell, processes that lead to increased intracellular adenosine triphosphate (ATP) concentrations. When glucose levels rise, the elevated ATP-to-adenosine diphosphate (ADP) ratio causes closure of ATP-sensitive potassium channels in the beta-cell membrane, leading to membrane depolarization and calcium influx through voltage-dependent calcium channels. This calcium influx triggers the fusion of insulin-containing vesicles with the cell membrane and subsequent insulin release.[1][1] In HNF4A-MODY, mutations within the HNF4A gene reduce the expression or function of proteins involved in glucose transport and metabolism, thereby impairing the ATP-dependent signaling cascade and preventing appropriate insulin secretion in response to glucose stimulation.

Critically, the deficit in glucose-stimulated insulin secretion in HNF4A-MODY can be partially circumvented by pharmacologic agents that directly stimulate insulin release through alternative mechanisms that bypass the impaired glucose-sensing machinery.[1][1][1] This physiologic principle explains why sulfonylureas, which bind to ATP-sensitive potassium channels and forcibly close them regardless of glucose concentration, can effectively restore insulin secretion in these patients despite the underlying genetic defect. Similarly, incretin-based therapies that activate adenylyl cyclase and increase cyclic adenosine monophosphate (cAMP) levels can potentiate insulin secretion through pathways that partially circumvent the glucose-sensing deficit.[12]

The clinical manifestation of HNF4A-MODY involves progressive beta-cell dysfunction characterized by a steady increase in blood glucose over time.[1][1] The beta-cell response to glucose stimulus is decreased due to impaired insulin secretion, and notably, glucagon secretion is also impaired.[1][1] This progressive worsening of blood glucose control creates a clinical situation where patients may present with the full spectrum of diabetes complications if glycemic control is not adequately maintained.[1][1] Microvascular complications, particularly those involving the retina and kidneys, are as common in HNF4A-MODY as in patients with type 1 or type 2 diabetes and are directly related to overall glycemic control.[1][1]

## Approved Drug Therapies: First-Line and Standard-of-Care Treatments

### Sulfonylureas: The Gold-Standard First-Line Pharmacological Treatment

Sulfonylureas represent the most effective and well-established pharmacological treatment for HNF4A-MODY and constitute the first-line oral hypoglycemic agent recommended by major diabetes organizations worldwide.[1][2][3][1][23][1] Both the International Society for Pediatric and Adolescent Diabetes/International Diabetes Federation and the American Diabetes Association guidelines recommend sulfonylureas as first-line treatment for HNF4A-MODY patients with hyperglycemia, as these medications stimulate insulin release from beta cells and thereby improve glycemic control.[1][1][23]

The mechanism of action of sulfonylureas provides the pharmacological basis for their exceptional efficacy in HNF4A-MODY. Sulfonylureas bind to the sulfonylurea receptor 1 (SUR1), a subunit of the ATP-dependent potassium channel located on the pancreatic beta-cell membrane.[1][1][1] This binding closes the potassium channel, leading to depolarization of the cell membrane.[1][1][1] The consequent depolarization triggers the opening of voltage-dependent calcium channels, resulting in increased calcium influx into the cell. This elevated intracellular calcium concentration mediates the fusion of insulin-containing vesicles with the cell membrane, resulting in the release of insulin.[1][1][1] Through this mechanism, sulfonylurea derivatives bypass the fundamental dysfunction caused by the HNF4A defect and restore the beta-cell response to metabolic stimulation, even though the underlying defect in glucose sensing persists.

The superior response of HNF4A-MODY patients to sulfonylureas compared with patients with type 2 diabetes has been well documented in clinical studies. In a seminal randomized crossover study conducted by Pearson and colleagues, patients with HNF1A-MODY (a closely related form of MODY with similar sulfonylurea responsiveness) demonstrated approximately a fivefold greater response to the sulfonylurea gliclazide compared with the biguanide metformin, and a fourfold greater response to gliclazide than patients with type 2 diabetes.[1][1][1] These findings underscore the unique pharmacologic sensitivity of MODY patients to this drug class. Additionally, in one documented case series, eight patients with HNF1A-MODY who had been on insulin therapy for a median of 20 years were successfully transitioned to a median dose of 80 milligrams per day of gliclazide, achieving a median reduction in hemoglobin A1c (HbA1c) of 0.8%.[1]

Several specific sulfonylureas have established roles in HNF4A-MODY management. **Gliclazide** is frequently used and available in both immediate-release and modified-release formulations. The immediate-release formulation typically requires multiple daily doses, with usual starting doses of 40 to 80 milligrams, titrated according to blood glucose response, up to a maximum daily dose of 320 milligrams.[1][21][1] The modified-release formulation of gliclazide allows for once-daily dosing, typically started at 30 milligrams daily and titrated up to a maximum of 120 milligrams daily, and may be associated with a lower risk of hypoglycemia compared with immediate-release preparations.[21][1] **Glimepiride** is another widely used sulfonylurea, typically dosed as a single daily dose with the first main meal, with usual starting doses of 1 to 2 milligrams and maximum daily doses of 4 to 8 milligrams.[1][1][1] **Glibenclamide** (also known as glyburide) is a longer-acting sulfonylurea that has been used in MODY management, though it carries a higher risk of prolonged hypoglycemia and has become less commonly used in many regions.[22][1]

The management strategy for initiating sulfonylurea therapy in HNF4A-MODY patients must account for the exaggerated insulin secretory response these patients manifest compared with type 2 diabetic individuals. The primary therapeutic goal is to avoid hypoglycemia; therefore, an initial dose of one-quarter of the normal starting dose in adults, progressively increased based on blood glucose control, is recommended.[1][1][1][1] To further reduce the risk of hypoglycemia, slow-release formulations may be prescribed when available.[1][1][1][1] If optimal glycemic control cannot be achieved with sulfonylureas alone, insulin injections may be initiated.[1][1][1][1]

The known safety profile and long-term efficacy of sulfonylureas, combined with documented improvements in quality of life and better patient compliance, have been well established in the literature.[1] Therefore, trial therapy with a sulfonylurea in hyperglycemic patients who are carriers of HNF4A mutations is considered mandatory for appropriate diabetes management.[1][1] Factors that have been identified as influencing the success of sulfonylurea therapy include the duration of diabetes, the initial HbA1c value, and patient weight gain.[1][1]

It is important to note that while sulfonylureas are highly effective in HNF4A-MODY, the disease process itself is progressive, and some patients eventually develop secondary failure to sulfonylurea therapy. Absolute primary failure to sulfonylurea medications is rare in HNF4A-MODY and has been detected in only a few patients.[1][1][1] However, the development of secondary failure has been documented after 3 to 25 years of treatment, with a reported decrease in insulin secretion of approximately 1% per year of treatment.[1][1][1] This progressive decline necessitates close long-term monitoring and potential intensification of therapy.

### Meglitinides: An Alternative to Sulfonylureas

Meglitinides represent an alternative class of insulin secretagogues that are structurally different from sulfonylureas but share a similar mechanism of action, namely stimulation of insulin secretion through effects on pancreatic beta-cell potassium channels. The two meglitinides commonly used in diabetes management are **repaglinide** and **nateglinide**. While the clinical evidence base is smaller than for sulfonylureas, meglitinides have demonstrated efficacy in HNF4A-MODY patients and may offer advantages in specific clinical circumstances.

Meglitinides function through binding to the ATP-dependent potassium channel, similar to sulfonylureas, but with a different mechanism and kinetics of action.[11][28] Importantly, meglitinides achieve lower postprandial glucose levels and pose a lower risk of delayed hypoglycemia compared with sulfonylureas in patients with HNF1A-MODY (the phenotypically similar form of MODY).[11] In a pediatric case series, three adolescents with HNF1A-MODY were successfully treated with meglitinide therapy, with one 14-year-old girl achieving an HbA1c reduction from 7.4% to 5.5% on repaglinide with no hypoglycemic episodes, while another 14-year-old boy switched from glibenclamide to nateglinide due to hypoglycemic episodes experienced on sulfonylurea therapy and achieved an HbA1c of 6.2% without further hypoglycemic episodes.[11] These case observations suggest that meglitinides may be particularly valuable in patients for whom sulfonylurea therapy is complicated by frequent hypoglycemic events.

In a larger cohort of patients, treatment with a sulfonylurea or meglitinide initiated shortly after diagnosis (at 8 months) was successful in 57% of patients, lowering HbA1c from 7.1% to 6.1% and improving residual beta-cell function after 5 years of follow-up.[1][1][1][1] However, when attempting to switch established insulin-treated patients to sulfonylurea or meglitinide therapy, success was limited, with only 3 of 10 patients remaining off insulin.[1][1][1][1] These findings suggest that earlier intervention with secretagogues before prolonged insulin therapy may yield better outcomes.

The typical dosing for **repaglinide** involves taking 1 to 2 milligrams three times daily 15 to 30 minutes before meals, with a maximum daily dose of 16 milligrams.[28] **Nateglinide** is typically dosed at 60 to 120 milligrams three times daily taken 1 to 30 minutes before meals.[28] Given the enhanced insulin secretory response in HNF4A-MODY, initiating therapy at the lower end of standard dosing ranges and titrating gradually is prudent to minimize hypoglycemia risk. Common adverse effects of meglitinides include hypoglycemia, though the risk is generally lower than with sulfonylureas, and patients should understand symptoms and appropriate management strategies.[28]

### Insulin: Second-Line Therapy When Oral Agents Fail

While insulin is not the preferred initial pharmacological treatment for HNF4A-MODY, it serves an important role when oral hypoglycemic agents fail to achieve adequate glycemic control or when patients develop secondary failure to sulfonylureas after prolonged disease duration.[1][1][1][1] Insulin therapy in HNF4A-MODY presents a paradox: although it can be highly effective, it is often problematic due to increased risk of hypoglycemia and weight gain.[3] Furthermore, misdiagnosis of HNF4A-MODY as type 1 diabetes has frequently led to prolonged, unnecessary insulin therapy in patients who would have benefited from sulfonylurea treatment after accurate diagnosis.

Several case reports have documented successful transitions from insulin to sulfonylureas in HNF4A-MODY patients. One notable case involved a patient diagnosed with HNF4A-MODY who switched from insulin therapy to sulfonylureas, resulting in improved glycemic control and quality of life.[13] Another documented case involved switching from 0.5 units per kilogram per day of insulin to 80 milligrams per day of gliclazide, achieving improved metabolic control.[1][1] These transitions are possible precisely because the fundamental defect in HNF4A-MODY involves beta-cell dysfunction rather than beta-cell destruction, distinguishing it from autoimmune type 1 diabetes where insulin replacement is absolutely necessary.

### Dietary Intervention: The Foundation of Early Management

Before initiating pharmacological therapy, dietary intervention represents an important first-line approach, particularly in newly diagnosed patients with mild hyperglycemia.[1][6][1][1][1] Diet appears to be a reasonable and effective treatment approach in many children and adolescents with HNF4A-MODY, particularly in early stages of disease.[1][1][1] Given that HNF4A-MODY patients are prone to marked hyperglycemia after oral glucose challenges, a low-carbohydrate diet may effectively reduce postprandial blood glucose levels.[1][1][1] When dietary intervention appears to be ineffective and hyperglycemia persists or progresses, switching to sulfonylureas is suggested as the next therapeutic step, rather than initiating insulin therapy.[1][1][1][1]

## Investigational and Pipeline Drug Therapies

### Glucagon-Like Peptide-1 Receptor Agonists: Emerging Evidence for Efficacy

Glucagon-like peptide-1 receptor agonists (GLP-1 RAs) represent an emerging therapeutic option for HNF4A-MODY and related monogenic forms of diabetes, with growing clinical evidence supporting their efficacy and potential advantages over traditional secretagogue therapy. A randomized clinical trial comparing the GLP-1 RA liraglutide with the sulfonylurea glimepiride was conducted specifically to evaluate the effects of GLP-1 signaling in MODY patients. In this double-blind, crossover design trial, 16 MODY patients (mean age 39 years, baseline HbA1c 6.46%) received both liraglutide and glimepiride for 6-week treatment periods separated by washout intervals. The authors found that glimepiride was more effective in reducing fasting plasma glucose and postprandial glucose excursions than liraglutide when analyzed across the entire cohort; however, glimepiride treatment was associated with a significantly greater risk for hypoglycemic events.[1][1] Importantly, patients taking liraglutide maintained good glycemic control with minimal hypoglycemia, suggesting that GLP-1 RAs offer a safer alternative to sulfonylureas in specific patient populations.

A particularly compelling case report described a 27-year-old patient with HNF1A-MODY (phenotypically similar to HNF4A-MODY in terms of sulfonylurea sensitivity) who had inadequate glucose control and was switched from sulfonylurea to once-weekly GLP-1 RA monotherapy; this switch resulted in optimal glycemic control without hypoglycemia for more than 1 year of follow-up.[1][1] More recent case series have documented impressive outcomes with GLP-1 RA therapy. A retrospective case series at Tawam Hospital (2019-2024) included six patients with genetically confirmed MODY, five with HNF1A-MODY and one with double heterozygous PAX4/PDX1 variants. Following GLP-1-based therapy with either GLP-1 receptor agonists or dual GLP-1/GIP receptor agonists, HbA1c improved in all patients with absolute reductions of 1.0 to 4.1 percentage points, and body weight decreased consistently across the series with losses ranging from 2.6 to 29 kilograms.[9] Among the four patients on insulin at baseline, three discontinued insulin completely, though the patient with homozygous HNF1A-MODY remained insulin-dependent. Both insulin-naïve patients maintained glycemic improvement without requiring insulin initiation.[9]

The mechanism underlying the efficacy of GLP-1 RAs in HNF4A-MODY involves glucose-dependent stimulation of insulin secretion that is largely independent of the glucose-sensing defect present in these patients. Activation of GLP-1 receptors on beta cells induces stimulation of adenylate cyclase, leading to an increase in cAMP levels. This mechanism is believed to bypass the reduced ATP concentrations observed in HNF1A-MODY and HNF4A-MODY, resulting in stimulation of insulin secretion and reduction of postprandial glucose levels.[12] Furthermore, since the effects of incretin hormones are strictly glucose-dependent, treatment with GLP-1 receptor agonists is rarely associated with hypoglycemia, providing a significant safety advantage over secretagogue therapy.[2]

Clinical trial evidence specifically addressing GLP-1 RA therapy in MODY comes from the MODY-TREAT trial (Clinical Trials ID: NCT01610934), an investigational study examining the effects of liraglutide in MODY patients.[2] This study employed a randomized, crossover design in which patients with MODY served as their own controls and were randomly assigned after a one-week washout of usual antidiabetic treatment to receive either liraglutide or glimepiride for 6 weeks, followed by another one-week washout period and treatment with the opposite drug for 6 weeks. Outcomes measured included fasting plasma glucose monitored twice weekly, seven-point glucose profiles every two weeks, and three blinded 48-hour continuous glucose profiles. Additional measurements included serum fructosamine levels, hypoglycemic events recorded in patient diaries, and postprandial responses of incretin hormones and beta-cell function assessment.[2]

### Dual GIP/GLP-1 Receptor Agonists: Promising New Therapeutic Option

The dual glucose-dependent insulinotropic peptide/glucagon-like peptide-1 receptor agonist (GIP/GLP-1 RA) class represents an exciting new therapeutic approach showing particular promise in HNF4A-MODY management. **Tirzepatide** is the most well-characterized agent in this class, and multiple case reports and small case series have documented remarkable efficacy in HNF4A-MODY patients.

A published case report described treatment outcomes with tirzepatide in two individuals with different disease pedigrees, one with HNF4A-MODY and one with HNF1A-MODY.[3] In the HNF4A-MODY patient (a 33-year-old Hispanic White woman), tirzepatide was initiated at 2.5 milligrams once weekly and titrated by 2.5 milligrams every 4 weeks to 7.5 milligrams once weekly. Prior to starting tirzepatide, she was on glipizide and prandial insulin. The patient tolerated this therapy well with only mild nausea as a side effect. Notably, this patient demonstrated dramatic improvements: HbA1c reduction of 3.1%, 23% body weight reduction, and a 45-unit total daily insulin dose reduction, with sulfonylurea discontinuation.[3] In the HNF1A-MODY patient (a 53-year-old non-Hispanic White woman), who was on glipizide, metformin, and insulin glargine at baseline, tirzepatide was initiated and titrated to 5 milligrams once weekly (the highest dose tolerated due to nausea). After 15 months of treatment, her HbA1c improved to 6.4%, body weight decreased by 21.3 kilograms (a 26% reduction compared with average 14.7% reduction in obese type 2 diabetic patients), and she reduced her insulin glargine dose by 1 unit with discontinuation of glipizide.[3] These cases highlight dual GIP/GLP-1 RA treatment as both adjuvant therapy and potentially as monotherapy.[3]

A comprehensive case series from 2024-2025 examined treatment outcomes with GLP-1 RAs and dual GIP/GLP-1 RAs in six MODY patients, including specific data on HNF4A-MODY and the related HNF1A-MODY form.[27] In patients with HNF1A-MODY and HNF4A-MODY treated with GLP-1 RAs, HbA1c was reduced by 1.3%, body mass index decreased by 2.90 kilograms per square meter, and total daily sulfonylurea dose decreased by 66.6%. In patients treated with dual GIP/GLP-1 RAs, there was a non-statistically significant decrease in HbA1c of 1.8%, a statistically significant reduction in body mass index of 8.73 kilograms per square meter, all patients discontinued sulfonylureas, and one patient discontinued insulin.[27] Mild gastrointestinal adverse effects were reported in isolated cases (nausea in one patient, diarrhea in one, and constipation in one).[27] These findings expand knowledge regarding the impact of GLP-1 RA and dual GIP/GLP-1 RA use in HNF4A-MODY and related forms, with results warranting further validation in randomized controlled trials.

The mechanistic basis for the efficacy of dual GIP/GLP-1 RAs involves the synergistic activation of two glucose-dependent incretin pathways. Both GIP and GLP-1 work through adenylyl cyclase-dependent mechanisms to increase cAMP levels in pancreatic beta cells, thereby bypassing the glucose-sensing defect inherent to HNF4A-MODY. Earlier work demonstrated that a trial with exogenous gastric inhibitory polypeptide (GIP) and GLP-1 infusions showed potentiation of the sulfonylurea-induced insulin secretion in 10 HNF1A-MODY cases, suggesting that concurrent activation of both incretin pathways may provide additive or synergistic benefits.[1][1][1]

### Dipeptidyl Peptidase-4 Inhibitors: Targeted Approach to Incretin Pathway

Dipeptidyl peptidase-4 (DPP-4) inhibitors represent an alternative approach to incretin pathway enhancement that works by inhibiting the enzymatic degradation of endogenous GLP-1 and GIP, thereby prolonging the duration of action of these hormones. DPP-4 inhibitors have shown promise in MODY management through a genotype-targeted approach, particularly in MODY subtypes with specific genetic defects affecting the incretin pathway.

A particularly illustrative case report describes targeted use of the DPP-4 inhibitor **sitagliptin** in a MODY type 4 (PDX1-MODY) patient carrying a novel heterozygous mutation in the PDX1 gene (c.694_697delGGCGinsAGCT p.Gly232Serfsx2).[10] This mutation caused deletion of four nucleotides and insertion of four different nucleotides, resulting in substitution of wildtype glycine with serine at position 232 and introduction of a novel stop codon at position 233. Importantly, PDX1 encodes a homeodomain-containing transcription factor that affects pancreatic development and insulin gene expression and indirectly impairs the incretin pathway. Given this genetic background with potential to impair incretin pathway function, and the patient's desire to avoid injectable therapy, the decision was made to initiate sitagliptin 100 milligrams while discontinuing the prior sulfonylurea therapy and continuing thiazolidinedione therapy. Following initiation of sitagliptin, the patient achieved significantly improved glycemic control, particularly with improved postprandial blood sugar readings.[10]

The mechanistic rationale for DPP-4 inhibitor use in MODY subtypes involves enhancement of endogenous GLP-1 levels. In normal physiology, DPP-4 enzymatically cleaves and inactivates GLP-1. By inhibiting this enzyme, DPP-4 inhibitors increase circulating GLP-1 concentrations and prolong the duration of GLP-1 receptor signaling. GLP-1 receptor signaling has been shown to modulate the endoplasmic reticulum stress response, which can promote beta-cell adaptation and survival. Since DPP-4 inhibition increases GLP-1 levels, modulation of the endoplasmic reticulum stress response likely plays a role in therapeutic response to DPP-4 inhibition in appropriate MODY subtypes.[10]

Recent systematic reviews have noted emerging evidence for the usefulness of glucagon-like peptide-1 agonists and dipeptidyl peptidase-4 inhibitors in HNF1A-MODY.[4][4] While most documented cases involve HNF1A-MODY or MODY type 3, the similar pathophysiologic mechanisms and genotype-phenotype relationships suggest that these agents may also have utility in HNF4A-MODY management. However, the evidence base for DPP-4 inhibitor use in HNF4A-MODY specifically remains limited to case reports and small case series, indicating need for prospective clinical trials.

A case report of DPP-4 inhibitor use in HNF1A-MODY involved treatment of an adult patient with sitagliptin, where combination therapy with a DPP-4 inhibitor was noted as an interesting and potentially effective approach.[45] An ongoing randomized, double-blinded, placebo-controlled crossover trial examined the combination of the sulfonylurea glimepiride and the DPP-4 inhibitor linagliptin versus glimepiride monotherapy in MODY patients, designed to provide additional data on the potential benefits of combining sulfonylureas with DPP-4 inhibitors.[1][1][1]

### Sodium-Glucose Cotransporter-2 Inhibitors: Emerging Research

Sodium-glucose cotransporter-2 (SGLT-2) inhibitors represent a novel class of glucose-lowering agents that have been investigated in MODY patients, though evidence for their utility in HNF4A-MODY specifically remains limited. SGLT-2 inhibitors work by inhibiting glucose reabsorption in the proximal renal tubule, leading to increased urinary glucose excretion and lower plasma glucose concentrations. Interestingly, genetic mutations in HNF1A-MODY affect the expression of SGLT2, the gene encoding the SGLT-2 transporter, suggesting that these patients might have unique responses to SGLT-2 inhibitor therapy.

In an investigational study, a single dose of the SGLT-2 inhibitor **dapagliflozin** (10 milligrams) induced higher glycosuria in both GCK-MODY and HNF1A-MODY patients compared with type 2 diabetic individuals, and notably reduced fasting plasma glucose in GCK-MODY patients.[20] This finding is particularly interesting because glycosuria in HNF1A-MODY results from reduced expression of SGLT2, which is under transcriptional control of HNF1A. In animals with HNF1A deficiency, there is an 80 to 90% reduction in SGLT2 expression correlating with reduced SGLT2 activity.[20] A case report documented combined therapy with a sulfonylurea and dapagliflozin in ABCC8-MODY (MODY type 12), suggesting that SGLT-2 inhibitors might have a role as adjunctive therapy.[20]

However, whether SGLT-2 inhibitors are a valid therapeutic option in HNF4A-MODY patients requires long-term clinical studies.[20] A single dose of dapagliflozin in HNF1A-MODY induced glycosuria but did not produce the same dramatic fasting glucose reduction observed in GCK-MODY patients, suggesting that the mechanism and utility may differ between MODY subtypes.[20] The International Society for Pediatric and Adolescent Diabetes/International Diabetes Federation and American Diabetes Association have not yet incorporated SGLT-2 inhibitors into standard recommendations for HNF4A-MODY management, and additional evidence from prospective trials would be necessary before making formal recommendations for their use in this population.

### GLP-1 Receptor Agonists in Pediatric HNF4A-MODY Management

Special consideration must be given to the management of HNF4A-MODY in pediatric patients. While sulfonylureas remain first-line therapy in children and adolescents, preliminary evidence suggests that GLP-1-based therapies may offer valuable alternatives in specific circumstances. A retrospective case series examining GLP-1 or dual GLP-1/GIP receptor agonist treatment in MODY included pediatric patients and documented favorable outcomes including improved glycemic control, body weight reduction, and reduced insulin requirements.[9][27]

The safety and tolerability profile of GLP-1 RAs in pediatric populations with type 2 diabetes has been established through multiple clinical trials, though dedicated trials in pediatric MODY populations remain limited. The most commonly reported adverse effects include gastrointestinal symptoms such as nausea, which typically diminish over time as patients develop tolerance.[2][9][27] Given the glucose-dependent mechanism of action, hypoglycemia is not a significant concern when GLP-1 RAs are used as monotherapy, though hypoglycemia risk increases if they are combined with insulin or other secretagogues.[2]

## Drug Repurposing Candidates and Off-Label Uses

### Metformin: Limited Utility in HNF4A-MODY

Although metformin is a cornerstone therapy for type 2 diabetes management, it has limited utility as monotherapy in HNF4A-MODY due to the pathophysiologic differences between these conditions. HNF4A-MODY is characterized by impaired insulin secretion in response to glucose stimulation (a beta-cell dysfunction), whereas type 2 diabetes is characterized by insulin resistance combined with impaired insulin secretion. Metformin works primarily through enhancing insulin sensitivity and improving hepatic glucose metabolism, mechanisms that do not address the fundamental insulin secretion deficit in HNF4A-MODY.

A randomized crossover study comparing the effects of metformin with gliclazide (a sulfonylurea) in two HNF4A-MODY patients demonstrated that gliclazide achieved significantly lower 24-hour average glucose levels and higher time-in-range values compared with metformin.[16] Specifically, in one patient, 24-hour average glucose was 7.7 millimoles per liter with metformin versus 7.6 millimoles per liter with gliclazide, with time-in-range values of 87% with metformin versus 83% with gliclazide. In the second patient, 24-hour average glucose was 6.3 millimoles per liter with metformin versus 5.8 millimoles per liter with gliclazide, with time-in-range values of 83% and 93%, respectively.[16] Notably, both metformin and gliclazide were significantly more effective than no treatment (with average glucose levels of 9.4 and 8.9 millimoles per liter and time-in-range values of 61% and 67%, respectively), suggesting that while metformin has some glucose-lowering activity in HNF4A-MODY, sulfonylureas remain substantially more effective.[16]

However, metformin may have a limited role in combination therapy with sulfonylureas or other agents in selected HNF4A-MODY patients, particularly those with elevated body mass index or concurrent insulin resistance. Additionally, metformin may provide metabolic benefits beyond glucose lowering, such as modest weight loss and potential cardiovascular benefits, that could be adjunctive to primary glycemic management with secretagogues.

### Thiazolidinediones: Limited Evidence in HNF4A-MODY

Thiazolidinediones (TZDs) are a class of oral antidiabetic agents that work by activating peroxisome proliferator-activated receptor gamma (PPAR-gamma), thereby enhancing insulin sensitivity and reducing hepatic glucose production. Examples include pioglitazone and rosiglitazone. While TZDs have an established role in type 2 diabetes management, their utility in HNF4A-MODY is limited due to the fundamental pathophysiology of the disease. Because the primary deficit in HNF4A-MODY is impaired insulin secretion rather than insulin resistance, agents that enhance insulin sensitivity alone are unlikely to produce substantial glycemic benefits.

However, in one reported case of HNF4A-MODY with a de novo variant, the patient's treatment regimen was modified to include metformin combined with semaglutide (a GLP-1 RA) with eventual discontinuation of insulin glargine, while thiazolidinedione therapy was discontinued during the optimization process.[30] This suggests that while TZDs were part of the initial management strategy, they were not retained as essential to the regimen when more effective agents became available.

In general, thiazolidinediones are not recommended as first-line or even adjunctive agents for HNF4A-MODY management based on current evidence. However, they might be considered in specific circumstances such as concurrent type 2 diabetes-like features or in patients with metabolic syndrome features accompanying their monogenic diabetes.

### Alpha-Glucosidase Inhibitors: Theoretical Benefit for Postprandial Hyperglycemia

Alpha-glucosidase inhibitors such as acarbose work by reversibly inhibiting intestinal alpha-glucosidases, enzymes responsible for the metabolism of complex carbohydrates into absorbable monosaccharide units.[29] This action results in a diminished and delayed rise in blood glucose following meals, potentially reducing postprandial hyperglycemia. Given that HNF4A-MODY patients characteristically develop marked hyperglycemia after oral glucose loads, alpha-glucosidase inhibitors might theoretically provide benefit, particularly when combined with dietary modifications emphasizing complex carbohydrates.

However, the current evidence base for alpha-glucosidase inhibitor use in HNF4A-MODY is limited. These agents have not been included in systematic reviews or clinical guidelines as standard therapy for MODY management, and their use appears to be limited to occasional case reports or off-label use. The most commonly reported adverse effects of acarbose include abdominal pain, diarrhea, and flatulence, which tend to diminish over time but may limit patient acceptability and compliance.[29] Given the superior efficacy and better-established track record of sulfonylureas and emerging evidence for GLP-1 RAs, alpha-glucosidase inhibitors are unlikely to play a significant role in HNF4A-MODY management.

## Treatment Considerations in Special Populations

### Management During Pregnancy

Management of HNF4A-MODY during pregnancy presents unique challenges due to the effects of sulfonylureas on fetal development and growth. Glibenclamide, a longer-acting sulfonylurea, crosses the placenta and its use in pregnancy is associated with increased birth weight and neonatal hypoglycemia, particularly when used in later pregnancy.[22] However, optimal management of HNF4A-MODY in pregnancy requires excellent glycemic control in the first trimester to minimize the risk of fetal malformations, while avoiding the negative impact of glibenclamide on fetal weight gain in the third trimester.[22]

Current recommendations suggest two main treatment options for HNF1A/HNF4A-MODY in pregnancy:[22] (1) discontinue sulfonylureas before conception and transfer to insulin therapy (at the risk of short-term deterioration of glycemic control), or (2) treat with glibenclamide in the first trimester and transfer to insulin in the second trimester, but only if the patient has good glycemic control at conception (HbA1c less than 48 millimoles per mole or 6.5%).[22] The second option should only be considered for women with good pre-pregnancy glycemic control, as the deterioration in control upon discontinuing sulfonylureas can be marked. Delivery should be considered between 37 and 38 weeks plus 6 days, in line with management of other pregnancies involving pre-existing diabetes.[22] Glibenclamide can be resumed post-delivery and during breastfeeding, with potential transfer to an alternative sulfonylurea after weaning.[22]

### Neonatal Presentations of HNF4A-MODY

Interestingly, some patients with HNF4A-MODY present with a biphasic clinical course, beginning with neonatal hyperinsulinemic hypoglycemia that later transitions to diabetes in early adulthood. One reported case describes a 15-month-old girl who presented with transient hypoglycemia responsive to diazoxide and chlorothiazide treatment; genetic testing identified a heterozygous pathogenic variant in the HNF4A gene (c.932G>A, p.Arg311His).[17] This case highlights that genetic testing for persistent neonatal hypoglycemia is critical to identify monogenic causes and enable early diagnosis and personalized treatment strategies. Additionally, it emphasizes the importance of long-term follow-up and tailored management to address the evolving clinical manifestations of the disease as patients transition from neonatal hyperinsulinism to early-onset diabetes later in life.[17]

## Contraindications and Adverse Event Considerations

### Metformin: Generally Not Contraindicated but Limited Efficacy

Metformin is not absolutely contraindicated in HNF4A-MODY, but its limited efficacy as monotherapy makes it unsuitable as first-line therapy. However, metformin may be considered as an adjunctive agent in combination with other drugs, particularly in patients with elevated body mass index or evidence of insulin resistance. Renal function should be monitored regularly in any patient taking metformin, particularly those developing diabetic nephropathy.

### Thiazolidinediones: Potential Concerns with Fluid Retention and Cardiac Effects

Thiazolidinediones carry several potential contraindications and adverse effects that should be considered before prescribing in HNF4A-MODY patients. Patients with preexisting edema or concomitant insulin therapy are at higher risk of edema and should start on the lowest available dose.[19][38] Thiazolidinediones are associated with an increased risk of congestive heart failure, particularly in patients with diastolic dysfunction or a history of heart failure, and should be used with caution in such patients.[19][38] Pioglitazone has been associated with an increased risk of bladder cancer in some studies, though this risk varies in a duration-dependent and dose-dependent fashion.[19][38]

Given these considerations and the limited evidence for efficacy in MODY, thiazolidinediones should generally not be used as first-line or even second-line agents for HNF4A-MODY management. If thiazolidinediones are considered for adjunctive use in specific circumstances, careful patient selection and monitoring are essential.

### GLP-1 Receptor Agonists and Dual GIP/GLP-1 Agonists: Generally Well-Tolerated

GLP-1 receptor agonists and dual GIP/GLP-1 receptor agonists are generally well-tolerated with a favorable safety profile in HNF4A-MODY and other MODY populations. The most commonly reported adverse effects are gastrointestinal in nature, including nausea, diarrhea, and vomiting, which tend to diminish over time as patients develop tolerance.[2][3][9][27] These gastrointestinal effects can occasionally lead to treatment discontinuation, though this is typically a minority of patients.

An important safety advantage of GLP-1 RAs and dual GIP/GLP-1 RAs is that hypoglycemia is rarely associated with their use as monotherapy due to the glucose-dependent nature of their insulin secretory effects.[2] This contrasts favorably with sulfonylureas and meglitinides, which can cause hypoglycemia even when plasma glucose is not elevated. However, hypoglycemia risk increases significantly if GLP-1 RAs are combined with insulin or other secretagogues.[2] Additionally, these agents are generally associated with weight loss rather than weight gain, providing metabolic advantages over secretagogue therapy.[3][9][27]

### Sulfonylureas: Risk of Hypoglycemia and Weight Gain

The primary safety concern with sulfonylurea therapy in HNF4A-MODY relates to the high risk of hypoglycemia due to the exaggerated insulin secretory response these patients manifest. This is the fundamental reason for starting at one-quarter of the usual adult starting dose and titrating slowly based on blood glucose response.[1][1][1][1] Additionally, sulfonylureas are associated with weight gain, which can compound metabolic dysfunction over time.[1][1][1][1] Long-term use of sulfonylureas has not been associated with pancreatic damage or harm to general health, though the medications may lose efficacy over time in some patients.[21]

## Combination Therapy Approaches

### Sulfonylurea Combined with GLP-1 Receptor Agonists

Recent evidence suggests that combining sulfonylureas with GLP-1 receptor agonists or dual GIP/GLP-1 receptor agonists may offer synergistic or additive glycemic benefits while allowing for dose reductions of each agent, potentially reducing overall adverse effect burden. A trial with exogenous gastric inhibitory polypeptide (GIP) and GLP-1 infusions showed potentiation of the sulfonylurea-induced insulin secretion in 10 HNF1A-MODY cases, providing preclinical support for combination incretin therapy with secretagogues.[1][1][1]

Multiple case series have documented successful combination therapy. In one case, a patient with HNF4A-MODY was treated with tirzepatide while discontinuing glipizide, with remarkable improvements in glycemic control and body weight.[3] In another case series, several MODY patients were treated with GLP-1 RAs or dual GIP/GLP-1 RAs, with many able to discontinue sulfonylureas entirely or reduce doses substantially while maintaining or improving glycemic control.[9][27] These observations suggest that a stepwise approach might involve starting with sulfonylurea therapy, then adding or switching to GLP-1-based therapy if glycemic targets are not achieved or if adverse effects such as hypoglycemia become problematic.

### Sulfonylurea Combined with Dipeptidyl Peptidase-4 Inhibitors

An ongoing randomized, double-blinded, placebo-controlled crossover trial examined the combination of the sulfonylurea glimepiride and the DPP-4 inhibitor linagliptin versus glimepiride monotherapy in MODY patients.[1][1][1] This combination approach is based on the concept that combining direct insulin secretagogues with agents that enhance endogenous incretin signaling might provide additive benefits. However, results from this trial have not yet been published in the peer-reviewed literature, limiting current evidence for formal recommendations regarding this combination.

### Triple Combination Therapy

In one reported case of HNF4A-MODY with a de novo variant, the patient was eventually treated with a combination of metformin, semaglutide (GLP-1 RA), and initially insulin glargine, which was subsequently discontinued as glycemic control improved.[30] This case suggests that in complex clinical scenarios, particularly when genetic diagnosis is delayed or disease management has been suboptimal, combination therapy with multiple agents acting through different mechanisms may be necessary.

## Comparison with Other MODY Subtypes: Precision Medicine Considerations

While this report focuses specifically on HNF4A-MODY, it is important to acknowledge that different MODY subtypes have markedly different treatment responses and prognoses, illustrating the principle of precision medicine in diabetes management. **GCK-MODY (MODY type 2)** is characterized by stable, mild fasting hyperglycemia that typically does not require pharmacological treatment, with generally excellent long-term prognosis and minimal diabetes-related complications.[1][1][1][4] **HNF1A-MODY (MODY type 3)** is phenotypically very similar to HNF4A-MODY in terms of sulfonylurea responsiveness, but differs in penetrance and age of onset, with presentation typically between ages 21 to 26 years and 63% of carriers developing diabetes by age 25.[33][47]

**HNF1B-MODY (MODY type 5)** is characterized by associated renal disease and urogenital tract abnormalities, and patients generally respond poorly to sulfonylureas, requiring insulin therapy in most cases.[1][1][1][33][33][33] **PDX1-MODY (MODY type 4)** involves mutations in the insulin promoter factor affecting pancreatic development, with evidence suggesting that DPP-4 inhibitors targeting the incretin pathway may be particularly beneficial.[10] **NEUROD1-MODY (MODY type 6)** typically requires insulin therapy as beta-cell dysfunction is often more severe.[1][1][1]

This heterogeneity underscores the critical importance of accurate genetic diagnosis to guide appropriate therapeutic strategy. Patients with misdiagnosed MODY (particularly HNF1A-MODY or HNF4A-MODY diagnosed as type 1 diabetes) often receive prolonged, unnecessary insulin therapy when sulfonylurea treatment would be more effective, safer, and provide better quality of life. Conversely, patients with HNF1B-MODY inappropriately treated with sulfonylureas will experience therapeutic failure, whereas those with GCK-MODY will receive unnecessary medication when diet alone typically suffices.

## Emerging Research Directions and Unmet Needs

### Long-Term Outcomes with Novel Incretin-Based Therapies

While recent case series and small clinical trials provide encouraging evidence for GLP-1 receptor agonists and dual GIP/GLP-1 receptor agonists in HNF4A-MODY management, robust long-term outcome data are lacking. Future research should include prospective randomized controlled trials comparing these agents with sulfonylureas as first-line therapy, with endpoints including glycemic control, quality of life, hypoglycemia frequency, weight change, and long-term complications. Additionally, head-to-head comparisons between different GLP-1 RAs and between GLP-1 RAs and dual GIP/GLP-1 RAs would help establish optimal therapeutic sequencing.

### Genotype-Specific Treatment Optimization

Different mutations within the HNF4A gene may result in varying degrees of functional impairment, potentially influencing drug responsiveness. Future research should investigate whether specific HNF4A mutations predict differential responses to various therapeutic agents, enabling truly personalized medicine approaches. For example, mutations affecting DNA binding versus those affecting protein stability might differentially influence the degree of beta-cell dysfunction and consequently the efficacy of various drugs.

### Beta-Cell Regeneration and Preservation

While current pharmacological approaches focus on enhancing residual insulin secretion from remaining functional beta cells, research into beta-cell preservation and potential regeneration remains an important frontier. Understanding whether GLP-1 receptor signaling in HNF4A-MODY patients promotes beta-cell survival and adaptation (similar to mechanisms described in other diabetes populations) could provide mechanistic insights into the apparent long-term benefits of these agents.

### Combination of Secretagogues with GLP-1-Based Therapies

As clinical experience accumulates with combination therapy approaches, systematic evaluation of optimal drug combinations, dosing strategies, and patient selection criteria is needed. Research should determine whether certain patient phenotypes (e.g., those with obesity, poor metabolic control, or frequent hypoglycemia on sulfonylureas) preferentially benefit from specific combination approaches.

## Conclusion

HNF4A-MODY represents a unique form of monogenic diabetes with remarkable therapeutic implications. Sulfonylureas remain the gold-standard first-line pharmacological treatment, with well-established efficacy in bypassing the glucose-sensing deficit and restoring insulin secretion through direct stimulation of pancreatic beta-cell potassium channels. Meglitinides offer an alternative secretagogue approach with potentially lower hypoglycemia risk. Emerging evidence from multiple case series and small clinical trials supports the efficacy and safety of glucagon-like peptide-1 receptor agonists and dual GIP/GLP-1 receptor agonists as either second-line agents in patients with suboptimal control on sulfonylureas or, in selected cases, as first-line alternatives, particularly in patients with obesity or high hypoglycemia risk. Dipeptidyl peptidase-4 inhibitors may offer benefits in specific MODY subtypes, particularly PDX1-MODY with documented incretin pathway defects, though evidence in HNF4A-MODY specifically is limited. Metformin, thiazolidinediones, and alpha-glucosidase inhibitors have limited utility in HNF4A-MODY due to the pathophysiologic emphasis on insulin secretion defects rather than insulin resistance.

Accurate diagnosis through genetic testing is essential to avoid prolonged misclassification as type 1 diabetes and inappropriate insulin therapy. The management approach should be individualized based on disease severity, patient characteristics, response to initial therapy, and adverse effect profile. Future research should focus on long-term outcomes with novel incretin-based therapies, genotype-specific treatment optimization, and identification of optimal combination therapy regimens. Through precision medicine approaches informed by genetic diagnosis and understanding of pathophysiology, patients with HNF4A-MODY can achieve excellent glycemic control while minimizing adverse effects and maximizing quality of life.