---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T23:13:28.571860'
end_time: '2026-04-04T23:16:41.846470'
duration_seconds: 193.27
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: sickle cell disease
  mondo_id: MONDO:0011382
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
- **Disease Name:** sickle cell disease
- **MONDO ID:** MONDO:0011382

## Research Objectives

Please provide a comprehensive research report on **drug therapies for sickle cell disease**.
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

# Drug Therapies for Sickle Cell Disease: A Comprehensive Clinical and Translational Review

This comprehensive research report summarizes the current landscape of pharmaceutical and cellular therapeutic interventions for sickle cell disease, encompassing approved medications, investigational agents in clinical trials, drug repurposing candidates, and emerging combination therapy approaches. The field has experienced remarkable progress since 2023, with multiple disease-modifying agents achieving regulatory approval, including the first FDA-approved gene therapies utilizing CRISPR-Cas9 technology. However, recent safety concerns have also prompted market withdrawals of previously approved therapeutics, necessitating careful reassessment of benefit-risk profiles. This report synthesizes evidence from clinical trials, regulatory approvals, and translational research to provide a current understanding of pharmacologic options for sickle cell disease management across different patient populations and disease manifestations.

## Approved Pharmacologic Therapies for Sickle Cell Disease

### Hydroxyurea: The Foundational Disease-Modifying Agent

**Hydroxyurea** remains the most extensively studied and widely utilized disease-modifying therapy for sickle cell disease, having established itself as the standard of care since its landmark clinical evaluation over four decades ago[2]. Hydroxyurea functions through multiple complementary mechanisms of action that extend well beyond its initial characterization as a simple ribonucleotide reductase inhibitor. The drug exerts its primary effect through inhibition of ribonucleotide reductase (RR), specifically the M2 subunit of this enzyme, which reduces intracellular deoxynucleotide triphosphate pools and acts as an S-phase-specific agent with inhibition of DNA synthesis[2]. Critically, this inhibition is transient, occurring during the period when hydroxyurea is present in the cell and reversing rapidly upon drug clearance due to spontaneous regeneration of the active RR enzyme. As a result, once-daily dosing in sickle cell disease causes intermittent cytotoxic suppression of erythroid progenitors and cell stress signaling, which then affects erythropoiesis kinetics and physiology, leading to recruitment of erythroid progenitors with increased fetal hemoglobin (HbF) levels[2].

The clinical efficacy of hydroxyurea was demonstrated convincingly in the landmark Multicenter Study of Hydroxycarbamide in Patients with Sickle Cell Anemia (MSH), completed in 1995, which established that hydroxycarbamide produces a decrease in vaso-occlusive crisis (VOC) pain episodes in approximately 50% of adult patients[22]. The mechanism by which hydroxyurea reduces VOC frequency involves multiple pathways beyond HbF induction. The drug's cytotoxic effects reduce marrow production of neutrophils, reticulocytes, and platelets—a particularly important finding because elevated white blood cell counts have been associated with both morbidity and mortality in sickle cell disease[2]. By lowering white blood cell counts and reducing surface expression of adhesion receptors on both neutrophils and reticulocytes, hydroxyurea addresses a fundamental pathophysiologic mechanism of vaso-occlusion[2]. Additional benefits include salutary effects on circulating erythrocytes, including macrocytosis, increased mean corpuscular hemoglobin, better cellular hydration, improved targeting within vascular tissue, less hemolysis, and fewer sickled forms[2].

Clinical outcomes with hydroxyurea therapy demonstrate significant benefits across multiple disease manifestations. The drug decreases the rate of acute chest syndrome (ACS) episodes and blood transfusions by approximately 50% in adults[2], and these benefits extend to the pediatric population. The Baby HUG study confirmed the efficacy of hydroxyurea in treating children, and the drug is now more widely used in pediatric sickle cell clinics, even for very young children[24]. Recent studies in sub-Saharan Africa have established the feasibility of widespread use of hydroxyurea and its potential benefits for resource-limited communities[24].

Regarding long-term safety, based on tens of thousands of cumulative exposure years, hydroxyurea treatment at therapeutic doses in young patients with sickle cell disease does not appear to confer an increased risk of malignancy[2]. Importantly, there are no teratogenic effects of hydroxyurea, a concern that was previously questioned. A 2009 analysis by Ballas and colleagues reported on pregnancy outcomes in patients with sickle cell disease receiving hydroxyurea during a controlled trial, which failed to show evidence of increased teratogenic risk[2]. Concurrent use of folic acid decreases the risk of neural tube defects. The most common side effects include neutropenia, bone marrow suppression, elevation of hepatic enzymes, anorexia, nausea, vomiting, and infertility concerns, though the latter has not been substantiated[2].

The National Institutes of Health Consensus Conference concluded that "the risks of hydroxyurea are acceptable compared with the risks of untreated sickle cell disease"[2], a statement that remains valid in contemporary practice. Despite the development of newer agents, there is increasing recognition that more attention should be devoted to optimizing hydroxyurea use, particularly in resource-limited settings and in patients inadequately responding to or intolerant of newer therapies.

### L-Glutamine: Reducing Oxidative Stress

**L-glutamine** (Endari) received FDA approval on July 7, 2017, for oral administration to reduce the acute complications of sickle cell disease in adult and pediatric patients 5 years and older[5]. The approval was based on data from a randomized, double-blind, placebo-controlled, multi-center clinical trial (NCT01179217) enrolling 230 patients aged 5 to 58 years with sickle cell anemia or sickle β0-thalassemia who had experienced two or more painful crises within the 12 months prior to enrollment. Eligible patients stabilized on hydroxyurea for at least 3 months continued their therapy throughout the study[5].

The mechanism by which L-glutamine exerts its therapeutic effect involves reduction of oxidative stress in red blood cells and other tissues. L-glutamine serves as a substrate for the synthesis of glutathione, a critical antioxidant that protects cells from oxidative damage. In sickle cell disease, where chronic hemolysis and vaso-occlusion generate substantial oxidative stress, enhancing glutathione synthesis represents a rational therapeutic approach. Efficacy was demonstrated by a reduction in the number of sickle cell crises through week 48 among patients who received L-glutamine compared to those receiving placebo, with a sickle cell crisis defined as an emergency room/medical facility visit for sickle cell disease-related pain treated with a parenteral narcotic or parenteral ketorolac[5].

Over the 48-week study period, patients receiving L-glutamine had a median of 3 sickle cell crises compared with a median of 4 crises for those receiving placebo[5]. Treatment with L-glutamine also resulted in fewer hospitalizations due to sickle cell pain, fewer cumulative hospital days, and a lower incidence of acute chest syndrome. The most common adverse reactions occurring in greater than 10% of patients treated with L-glutamine were constipation, nausea, headache, abdominal pain, cough, pain in extremity, back pain, and chest pain[5]. Treatment discontinuation due to adverse reactions was reported in only 2.7% of patients receiving L-glutamine, indicating good tolerability.

The recommended dose of L-glutamine is 10 to 30 grams per day based on body weight, taken orally twice daily[5]. Each dose should be mixed in 240 mL of cold or room temperature beverage or 4 to 6 oz of food before ingestion. L-glutamine received Orphan Drug Designation from the FDA prior to approval, facilitating the development pathway for this relatively inexpensive and orally bioavailable agent[5].

### Voxelotor: A Recently Withdrawn Hemoglobin Polymerization Inhibitor

**Voxelotor** (Oxbryta, GBT440) represented an innovative approach to sickle cell disease by targeting the fundamental molecular event underlying disease pathophysiology—hemoglobin polymerization—rather than attempting to increase fetal hemoglobin levels or modulate inflammatory adhesion molecules. The drug functions as an allosteric modulator of hemoglobin that increases hemoglobin's oxygen affinity, thereby reducing the concentration of deoxy-hemoglobin S (deoxy-HbS) available for polymerization[3]. Since neither hemoglobin A (HbA), fetal hemoglobin (HbF), nor oxygenated HbS participate in polymerization interactions, a reduction in deoxy-HbS concentration via an increase in oxygen affinity can alter the kinetics of polymerization and provide a sufficiently prolonged delay time for red blood cells to pass through hypoxic environments in distal capillaries and arterioles without sickling prior to reoxygenation in the lungs[3].

The preclinical evidence supporting voxelotor's efficacy was compelling. In vitro studies demonstrated that the polymerization delay time as measured by optical density increased from 9 minutes to 18-22 minutes in the presence of 20-30% modified HbS, equivalent to the known effect of the same percentage of HbF on delay time[3]. Sickling was similarly inhibited by 30% hemoglobin occupancy with voxelotor, with no increase in sickle cells observed after 20 minutes of deoxygenation to 40 mmHg (approximately equivalent to venous blood oxygen tension in vivo), in contrast to a greater than 6-fold increase in sickling in untreated samples at the same oxygen tension[3]. These in vitro studies also found prolonged red blood cell half-life (indicating reduced hemolysis), improved deformability, and decreased whole blood viscosity in deoxygenated samples incubated with voxelotor[3].

Clinical trials demonstrated an average improvement in hemoglobin of greater than 1 g/dL, as well as statistically significant improvements in established markers of hemolysis[3]. However, in September 2024, Pfizer voluntarily withdrew voxelotor from national and global markets, ceasing distribution and discontinuing all active clinical trials and expanded access programs due to safety concerns[45][48]. Pfizer's decision was based on the totality of clinical data indicating that the overall benefit of voxelotor no longer outweighed the risk in the approved sickle cell patient population, with data suggesting an imbalance in vaso-occlusive crises and fatal events[45].

The safety signal emerged from three reports linking voxelotor to increases in pain and deaths. First, a clinical research study of children with sickle cell disease and higher risk of stroke (GBT440-032) had eight deaths in the voxelotor group compared to two deaths in the group without voxelotor, among 236 children aged 2-15 years from Egypt, Ghana, Kenya, Nigeria, Oman, Saudi Arabia, the USA, and the United Kingdom[45]. Second, another clinical research study of adolescents and adults with sickle cell disease and leg ulcers (GBT440-042) had eight deaths on voxelotor among 88 patients at least 12 years of age enrolled in Brazil, Kenya, and Nigeria[45]. Third, monitoring reports from people taking voxelotor as prescribed medication outside of clinical trials also indicated safety concerns[45].

Analysis of the fatal cases revealed that most occurred in the context of serious infections: of the eight deaths in the GBT440-032 study, three involved fatal malaria and two involved sepsis[45]. Of the eight deaths in the GBT440-042 study, malaria was identified as either the cause or a contributing factor in four cases[45]. These findings raised questions about potential immunosuppressive effects of voxelotor, which had been observed in animal studies and a decrease in white blood cell counts in clinical studies[45]. The European Medicines Agency had previously raised concerns about these possible immunosuppressive effects at the time of market authorization, noting that given the immunosuppressive effects in animal studies and the decrease in white blood cell counts in clinical studies, the emerging safety data required further review to determine impact on the benefit-risk balance[45].

Clinicians are now advised that the FDA received questions about whether to allow some individuals living with sickle cell disease to continue voxelotor on a "compassionate use" basis, but no such authorization has been granted to date[45]. For patients who had been doing well on voxelotor, there is concern about what happens upon discontinuation. One report describes that transitioning from full-dose voxelotor to complete cessation led to intense hemolysis and severe sickle cell problems within three days that injured the kidneys and other organs and required hospitalization[45].

### Crizanlizumab: P-Selectin Inhibition with Emerging Questions

**Crizanlizumab** (Adakveo) is a humanized monoclonal IgG2 antibody directed against P-selectin, which is used to prevent painful, vaso-occlusive crises in patients with sickle cell disease[4]. Sickle cell disease is characterized by an inherited mutation in the β globin gene that creates hemoglobin S (HbS), which is prone to aggregation with deoxygenation, resulting in deformation and sickling of red blood cells, hemolytic anemia, and recurrent painful crises involving different organs and tissues[4]. The binding of crizanlizumab to P-selectin inhibits its attachment to its glycoprotein ligand, thereby inhibiting the adhesion of sickled red cells to endothelium, a critical step in the vaso-occlusive crises of sickle cell disease[4]. Notably, crizanlizumab does not prevent sickling of red cells, increase hemoglobin levels, or change the oxygen-binding characteristics of hemoglobin; rather, it inhibits the aggregation and binding of sickled red cells to platelets, leukocytes, and endothelial cells, which mediates the vascular occlusions underlying painful crises[4].

In preregistration randomized, placebo-controlled trials, 48 weeks of crizanlizumab therapy resulted in a decrease in the number of painful crises and both duration and numbers of hospitalizations[4]. Crizanlizumab was approved in the United States in 2019 as therapy for prevention of painful crises in sickle cell disease in adults and children above the age of 16 years[4]. The drug is generally well tolerated and has not been associated with serum aminotransferase elevations during therapy or with instances of clinically apparent liver injury[4].

However, recent data have raised questions about crizanlizumab's efficacy. In 2025, the phase III STAND trial of crizanlizumab was published, revealing no difference between the treatment and placebo groups[6][6]. This finding suggests that the initial promise of P-selectin inhibition for preventing vaso-occlusive crises may not have translated into sustained clinical benefit, though the specific reasons for this discordance between earlier trials and the STAND trial require further investigation.

## Gene and Cellular Therapies: Revolutionary Approaches to Cure

### Casgevy: CRISPR-Cas9 Gene Editing

**Casgevy** (exagamglogene autotemcel) represents a landmark therapeutic advance as the first FDA-approved therapy utilizing CRISPR/Cas9, a type of genome editing technology, for the treatment of sickle cell disease[1][13][1]. Approved in 2023, Casgevy is specifically indicated for the treatment of sickle cell disease in patients 12 years of age and older with recurrent vaso-occlusive crises. The therapeutic approach involves modification of patients' hematopoietic (blood) stem cells by genome editing using CRISPR/Cas9 technology[1]. CRISPR/Cas9 can be directed to cut DNA in targeted areas, enabling the ability to accurately edit (remove, add, or replace) DNA where it was cut. The modified blood stem cells are transplanted back into the patient where they engraft (attach and multiply) within the bone marrow and increase the production of fetal hemoglobin (HbF), a type of hemoglobin that facilitates oxygen delivery[1]. In patients with sickle cell disease, increased levels of HbF prevent the sickling of red blood cells[1].

The clinical development of Casgevy involved extensive evaluation. The safety and effectiveness of Casgevy were evaluated in an ongoing single-arm, multi-center trial in adult and adolescent patients with sickle cell disease. Patients included in the trial had a history of at least two protocol-defined severe VOCs during each of the two years prior to screening. The primary efficacy outcome was freedom from severe VOC episodes for at least 12 consecutive months during the 24-month follow-up period[1]. A total of 44 patients received Casgevy, and of the 31 patients with sufficient follow-up time to be evaluable, 29 (93.5%) achieved this outcome[1]. All treated patients achieved successful engraftment with no patients experiencing graft failure or graft rejection[1].

Prior to treatment with Casgevy, patients' own stem cells are collected, and then the patient must undergo myeloablative conditioning (high-dose chemotherapy), a process that removes cells from the bone marrow so they can be replaced with the modified cells[1]. The modified stem cells are then delivered to the patient as a one-time, single-dose infusion as part of a hematopoietic blood stem cell transplant[1]. The most common side effects were low levels of platelets and white blood cells, mouth sores, nausea, musculoskeletal pain, abdominal pain, vomiting, febrile neutropenia (fever and low white blood cell count), headache, and itching[1]. Both the Casgevy application and that of Lyfgenia received Priority Review, Orphan Drug, Fast Track, and Regenerative Medicine Advanced Therapy designations[1][1].

More recent data from the phase 3 CLIMB SCD-121 study demonstrates the remarkable efficacy of Casgevy in reducing vaso-occlusive crises[13]. Treatment with exagamglogene autotemcel (exa-cel) eliminated vaso-occlusive crises in 97% of patients with sickle cell disease for a period of 12 months or more[13]. A total of 44 patients received exa-cel, and the median follow-up was 19.3 months. Of the 30 patients who had sufficient follow-up to be evaluated, 29 (97%; 95% confidence interval [CI], 83 to 100) were free from vaso-occlusive crises for at least 12 consecutive months, and all 30 (100%; 95% CI, 88 to 100) were free from hospitalizations for vaso-occlusive crises for at least 12 consecutive months[13]. The safety profile of exa-cel was generally consistent with that of myeloablative busulfan conditioning and autologous HSPC transplantation, with no cancers occurring[13].

### Lyfgenia: Lentiviral Vector Gene Therapy

**Lyfgenia** is a cell-based gene therapy approved alongside Casgevy for the treatment of sickle cell disease. Lyfgenia uses a lentiviral vector (gene delivery vehicle) for genetic modification and is approved for the treatment of patients 12 years of age and older with sickle cell disease and a history of vaso-occlusive events[1][1]. With Lyfgenia, the patient's blood stem cells are genetically modified to produce HbAT87Q, a gene-therapy-derived hemoglobin that functions similarly to hemoglobin A, which is the normal adult hemoglobin produced in persons not affected by sickle cell disease[1]. Red blood cells containing HbAT87Q have a lower risk of sickling and occluding blood flow[1]. Like Casgevy, the modified stem cells are delivered to the patient as a one-time, single-dose infusion as part of a hematopoietic blood stem cell transplant[1].

The safety and effectiveness of Lyfgenia is based on the analysis of data from a single-arm, 24-month multicenter study in patients with sickle cell disease and history of vaso-occlusive events between the ages of 12 and 50 years[1]. Effectiveness was evaluated based on complete resolution of VOEs (VOE-CR) between 6 and 18 months after infusion with Lyfgenia. Twenty-eight (88%) of 32 patients achieved VOE-CR during this time period[1][1]. Both products are made from the patients' own blood stem cells, which are modified and given back as a one-time, single-dose infusion. Prior to treatment, a patient's own stem cells are collected, and then the patient must undergo myeloablative conditioning (high-dose chemotherapy)[1]. Patients who received Casgevy or Lyfgenia are followed in a long-term study to evaluate each product's safety and effectiveness[1].

### ARU-1801: Modified Gamma-Globin Gene Therapy with Reduced-Intensity Conditioning

**ARU-1801** represents an alternative gene therapy approach designed to address some of the limitations of myeloablative approaches[14]. ARU-1801 is a gene therapy that uses a modified γ-globin lentiviral vector to produce HbF G16D within autologous CD34+ hematopoietic stem cells. Preclinical studies in sickle cell disease mice have shown that the G16D mutation enables γ-globin G16D to bind α-globin with higher affinity; lentiviral transfer of γ-globin G16D resulted in 1.5-2x more HbF per vector copy number compared to analogous wild-type γ-globin vector[14]. Early studies also suggested that HbF G16D may be more potent for anti-sickling than HbF, lowering reticulocyte counts in sickle cell disease mice to a greater extent at similar protein levels[14].

ARU-1801 with reduced-intensity conditioning (RIC) could lessen toxicities and resource utilization relative to myeloablative approaches, potentially allowing expanded access to gene therapy for a broader group of sickle cell disease patients. Updated data from patients in the ongoing Phase 1/2 study (NCT02186418) indicate that prior to ARU-1801 drug product infusion, all patients received a single intravenous dose of RIC melphalan (140 mg/m²)[14]. As of July 28, 2021, four patients (mean age, 26 years [range 19-35]) had been treated with ARU-1801 gene therapy for sickle cell disease with three patients followed for 12 months or more post-transplant[14].

The safety profile appears favorable. Transient neutropenia and thrombocytopenia were the predominant adverse events, lasting a median of seven days each, with no other serious adverse events related to chemotherapy or ARU-1801 reported to date[14]. Clinical efficacy is notable: at 36 months post-transplant, Patient 1 has shown stable HbF expression at 27% with 64% F-cells and marked improvements in sickle cell disease manifestations, including 93% fewer annualized VOEs compared to the two years prior to treatment[14]. Patient 2 has maintained 14% HbF and 37% F-cells at 36 months and saw 85% fewer annualized VOEs, despite lower engraftment due to renal hyperfiltration at the time of conditioning[14]. Patient 3, who received ARU-1801 manufactured with process improvements, has maintained 36% HbF at month 15 with pancellular distribution (96% F-cells) and has had no VOEs since administration, representing 100% reduction from baseline[14]. The amelioration of sickle cell disease phenotype and engraftment of ARU-1801 gene-modified hematopoietic stem and progenitor cells is possible with a single RIC dose of melphalan, suggesting this approach may be a promising alternative to myeloablative transplants for achieving durable responses with a favorable safety profile[14].

## Investigational Pipeline Therapies

### NDec: Oral DNA Methyltransferase Inhibition

**NDec** (modified-release decitabine and tetrahydrouridine) is an investigational treatment for sickle cell disease utilizing a novel approach to fetal hemoglobin induction[15][46][49]. Sickle cell disease is driven by the polymerization of mutated hemoglobin (HbS) in red blood cells, while fetal hemoglobin (HbF) mitigates the effects of HbS polymerization. However, the HbF gene expression is silenced in infancy by DNA methyltransferase 1 (DNMT1)[46]. Decitabine induces HbF expression by direct inhibition of DNMT1, offering an alternative to the indirect effects of hydroxyurea on HbF induction, which operates through inducing bone marrow stress[46]. NDec is an innovative combination treatment with decitabine as the active component and tetrahydrouridine as a pharmacokinetic enhancer[46].

A previous trial in high-risk patients with sickle cell disease reported that oral decitabine (0.16 mg/kg) and THU versus placebo significantly increased total hemoglobin, HbF, and the proportion of HbF-enriched red blood cells (%F-cells) and improved red blood cell health markers, without triggering grade 3 or higher non-hematologic toxicity[46]. The ASCENT1 trial (NCT05405114) aims to study the efficacy and safety of once- or twice-weekly NDec versus placebo in patients with sickle cell disease to provide proof of concept and determine the optimal dose for future trials[46][49].

The study design is notable for its engagement with patient communities. ASCENT1 is a randomized, placebo-controlled phase 2 trial with an additional exploratory open-label hydroxyurea block[46]. The trial population includes all sickle cell disease genotypes in patients at least 18 years of age with 2-10 documented vaso-occlusive crises within 12 months before screening and hemoglobin levels of 5.0-10.5 g/dL[49]. Participants randomized to NDec in the hydroxyurea-active block will discontinue hydroxyurea during a 4-week washout period[46].

The primary endpoint is change in total hemoglobin from baseline to week 24[46]. Secondary efficacy endpoints include change in HbF (g/dL and %HbF of total Hb), change in %F-cells of total red blood cells, and change in hemolysis markers from baseline to week 24[46]. Secondary efficacy endpoints also include numbers of vaso-occlusive crises, acute chest syndrome, and transfused red blood cell units from baseline to week 48[46]. The trial consists of a main and extension phase, each lasting 24 weeks, with NDec administered orally with meals according to body weight intervals to attain a decitabine dose level of 0.16-0.25 mg/kg and THU dose level of 8-12.5 mg/kg[46][49].

### Panobinostat: Histone Deacetylase Inhibition

**Panobinostat** (LBH589) is a histone deacetylase inhibitor being investigated as a potential agent to induce fetal hemoglobin expression in sickle cell disease patients who have failed to respond to or are intolerant of hydroxyurea therapy[10]. A Phase I dose-escalation study was designed to determine the maximum tolerated dose (MTD) and dose-limiting toxicities of panobinostat as a single agent and to characterize the safety and tolerability of panobinostat in adult patients with sickle cell disease[10]. The treatment phase consists of 12 weeks of duration, with subjects assigned to specified dose levels and dosing schedules remaining with the assigned regimen if tolerated throughout the 12-week period[10]. All subjects take study drug thrice weekly (Monday, Wednesday, and Friday) throughout the duration of the 12-week treatment period[10].

The primary outcome measure is determination of the safety and dose-limiting toxicities of escalating doses of oral panobinostat in sickle cell disease[10]. Secondary outcome measures include determination of the effect of escalating doses of panobinostat on overall HbF percentage and F cells, change in total hemoglobin, effect on serum inflammation markers and cytokines, and effect on quality of life as measured by the Adult Sickle Cell Quality of Life (ASCQMe) questionnaire[10]. Additionally, the study aims to define mechanisms of effect of panobinostat, including HbF induction and anti-inflammatory effects, and to discover biomarkers of treatment response[10].

### SGLT2 and SGLT1/2 Inhibitors: Novel Anti-Inflammatory Approach

Recent preclinical investigation has revealed that **sodium-glucose co-transporter inhibitors** may have therapeutic potential in sickle cell disease through mechanisms extending beyond their glucose-lowering effects[28]. Sodium-glucose co-transporter 2 inhibitors (SGLT2i) are widely used to treat patients with type 2 diabetes and exhibit beneficial cardiovascular effects beyond glucose lowering. Investigation of their potential to alleviate vaso-occlusive events and organ damage in sickle cell disease mice revealed significant benefits[28].

Intravital and immunofluorescence microscopy demonstrated that 4-day oral administration of dapagliflozin (DAPA) or sotagliflozin (SOTA) significantly reduces neutrophil adhesion and transmigration in cremaster venules, with SOTA showing greater inhibition, and downregulates E-selectin and intercellular adhesion molecule-1 (ICAM-1) expression in cremaster venules of TNF-α-challenged sickle cell disease mice[28]. Intriguingly, only SOTA improves mouse survival acutely[28]. Similar inhibitory effects on neutrophil recruitment are observed in sickle cell disease mice subjected to hypoxia-reoxygenation[28]. Flow chamber assays indicate that neither drug directly affects neutrophil or endothelial cell adhesive function[28].

When administered for 4 months, DAPA or SOTA mitigates neutrophil recruitment and enhances microcirculation in cremaster venules of TNF-α-challenged sickle cell disease mice, while only SOTA confers a survival benefit[28]. Both drugs reduce leukocyte infiltration in the liver or lungs, suggesting their ability to protect against organ damage[28]. Multiplex analysis shows that DAPA and SOTA lower plasma levels of soluble P-selectin, ICAM-1, S100A8/A9, and pro-inflammatory cytokines in sickle cell disease mice[28]. Co-administration with hydroxyurea for 4 months does not enhance these effects, suggesting that SGLT inhibitors may function through distinct anti-inflammatory pathways complementary to hydroxyurea's mechanisms[28].

### GLP-1 Agonists: Emerging Anti-Inflammatory Strategy

**GLP-1 agonists**, a class of medications developed for type 2 diabetes management, are being investigated for potential benefits in sickle cell disease based on their anti-inflammatory properties[27]. Preliminary evidence suggests an association between GLP-1 agonist use and improved survival, as well as reduced sickle cell crisis and cardiopulmonary complications in patients with sickle cell disease[27]. These findings, while preliminary, suggest that the inflammatory modulation achieved through GLP-1 receptor agonism may benefit the chronic inflammatory state characteristic of sickle cell disease.

## Drug Repurposing: Evidence-Based Candidates for Sickle Cell Disease

### Statins: Immunomodulation and Hemoglobin F Induction

A genomics-driven drug repurposing analysis identified multiple approved medications with potential for sickle cell disease management, with **simvastatin** and other statins emerging as particularly promising candidates[11]. Analysis identified 78 approved medications with potential for repurposing in sickle cell disease; this list was narrowed to 21 candidates based on safety profiles and interactions with key genetic pathways[11]. Among these, simvastatin, allopurinol, omalizumab, canakinumab, and etanercept were suggested as the most promising agents[11].

Simvastatin was recommended as the ideal drug for repurposing in sickle cell disease based on robust evidence[11]. In vitro studies underscore simvastatin's potential, showing a 1.9-fold increase in fetal hemoglobin expression and a 30-35% reduction in irreversibly sickled cells under hypoxic conditions[11]. Based on this robust body of evidence and specific criteria, simvastatin is recommended as an ideal drug for repurposing in sickle cell disease, although other statins such as atorvastatin, pravastatin, fluvastatin, rosuvastatin, and pitavastatin may also offer benefits depending on individual patient factors[11]. The most frequently detected therapeutic class among repurposing candidates was immunosuppressants (n = 12), including adalimumab, canakinumab, infliximab, omalizumab, tocilizumab, etanercept, rilonacept, peginterferon alfa-2a and 2b, azathioprine, glatiramer, and anakinra[11].

### Canakinumab and Etanercept: Cytokine-Directed Immunomodulation

**Canakinumab** and **etanercept** were suggested as favorable candidates for drug repurposing in sickle cell disease management[11]. Canakinumab can selectively target interleukin-1β (IL-1β), a cytokine with a central role in the inflammatory process, and may contribute to modulating disease pathways in sickle cell disease[11]. The rationale for these agents stems from the central role of chronic inflammation in sickle cell disease pathophysiology, where IL-1β and TNF-α are upregulated and contribute to vaso-occlusive crises, hemolysis, and chronic organ damage.

### Allopurinol: Targeting NOTCH4 Pathways

**Allopurinol**, an antihyperuricemic agent traditionally used in gout management, was identified as a promising repurposing candidate for sickle cell disease because it targets the NOTCH4 gene, which is involved in critical sickle cell disease pathological pathways[11]. The identification of allopurinol represents an unconventional approach to sickle cell disease management that leverages genomic pathway analysis to identify unexpected therapeutic targets.

### Tocilizumab: IL-6 Inhibition for Hyperhemolysis and Acute Chest Syndrome

**Tocilizumab**, a humanized monoclonal antibody against the interleukin-6 (IL-6) receptor, is being investigated for multiple sickle cell disease-related complications[39][42]. Tocilizumab was effectively used off-label in four young adults with sickle cell disease who developed hyperhemolysis syndrome as a complication of blood transfusion, stopping the premature destruction of red blood cells[42]. While tocilizumab is not approved for sickle cell disease or hyperhemolysis syndrome, it is generally widely available and should be considered a suitable and cost-effective alternative to currently available options[42].

Hyperhemolysis syndrome is a rare but serious complication that can follow a blood transfusion in which both the patient's own and transfused red blood cells are rapidly destroyed, leading to severe anemia[42]. The exact cause of hyperhemolysis syndrome is unclear but may involve antibodies triggering the complement pathway or activation of macrophages—immune cells that can destroy red blood cells[42]. Tocilizumab works by blocking the receptor for interleukin-6, a molecule that drives inflammation[42]. Because IL-6 activates macrophages, blocking its signaling with tocilizumab may be of therapeutic value for hyperhemolysis syndrome[42].

Case analysis of tocilizumab use showed that in all four patients, tocilizumab was effective in rapidly stopping hemolysis[42]. The two men subsequently received blood transfusions without any signs or symptoms of hemolysis but died due to problems unrelated to hyperhemolysis syndrome[42]. The two women required no further blood transfusions and remained alive after hospital stays of 11 and 43 days, respectively[42]. Researchers suggest that tocilizumab could be considered as an alternative to eculizumab in uncomplicated hyperhemolysis syndrome in sickle cell disease where there is evidence of an inflammatory response, indicated by fever or macrophage activation[42].

Additionally, a phase IIA trial is administering low-dose tocilizumab to approximately 70 adult and adolescent patients with sickle cell disease admitted with acute chest syndrome, a life-threatening condition similar to pneumonia but unique to sickle cell disease[39]. Acute chest syndrome is not entirely understood, and there are no curative treatments available. Recent data have suggested that increased interleukin-6 is a component of the inflammation in acute chest syndrome. The study is expected to demonstrate that tocilizumab will improve overall clinical outcomes, including oxygen levels, inflammation, and pain[39].

## Symptom-Specific and Supportive Pharmacotherapies

### Pain Management in Vaso-Occlusive Crises

#### Opioids: The Cornerstone of Acute Pain Management

Opioids represent the mainstay in the treatment of moderate to severe pain in sickle cell disease[34][50]. Currently available opioids traditionally provide their analgesic action through the mu opioid receptor, although other agents such as agonist-antagonists are utilized in this population[50]. The advantages of opioids include their potent centrally mediated analgesic action, the availability of many routes for delivery, a variety of available agents, and a lack of a ceiling effect, allowing for continued drug titration if there is lack of analgesia at lower doses[50].

**Diamorphine** (heroin) has replaced pethidine as the analgesic of first choice for acute pain in sickle cell disease[34][34]. Among approximately 800 adults with hemoglobinopathy receiving treatment in major centers, only a few individuals still receive pethidine[34]. There are several reasons for preferring diamorphine: the pethidine metabolite is excitatory to the nervous system and causes seizures, while diamorphine has a longer duration of action and is mass-for-mass a more potent analgesic[34]. Whereas diamorphine is soluble enough to be given subcutaneously, pethidine must be injected into muscle, and repeated intramuscular injections of pethidine cause muscle fibrosis and contractures with progressive reduction in absorption and need for escalating doses[34]. The standard dosing interval for morphine injections and rapid-release preparations is 4-6 hours, though some individuals become so tolerant to opioids that doses are needed 2-hourly[34][34].

Patient-controlled analgesia (PCA) is reported to be as safe and effective as intermittent opioid injections[34] and is particularly well tolerated in children over the age of 7, allowing for the child to control titration of analgesia and reducing the delay inherent in nurse-delivered analgesia[50]. Children and adolescents with sickle cell disease hospitalized for pain should receive PCA with both continuous infusion and bolus dosing[50]. When acute pain begins to resolve, the dose should be tailed off gradually rather than stopped abruptly to avoid withdrawal symptoms that can mimic those of sickle cell crisis[34].

#### NSAIDs: Anti-inflammatory Adjunctive Therapy with Important Cautions

**Nonsteroidal anti-inflammatory drugs** (NSAIDs) have been commonly used to treat pain in sickle cell disease as part of combination analgesic regimens[17][34][38]. The American Society of Hematology recommends that a short course (5-7 days) of NSAIDs be given alongside opioids to help manage acute pain in adults and children with sickle cell disease[17]. By combining analgesics with different mechanisms of action, such as acetaminophen or diclofenac, the dose of opioids can be kept to a minimum[34].

However, NSAIDs are associated with renal, gastrointestinal, and cardiovascular toxicities that warrant careful consideration[17][38]. The nephropathy can be worsened by NSAIDs, so treatment with these agents should be stopped after a week at the most[34]. NSAIDs use should be individualized based on potential side effects and patient risk factors, with the lowest effective dose prescribed with proper monitoring in patients with sickle cell disease[38]. Due to the risk of damage to the kidneys, heart, and digestive tract, caution is necessary if using NSAIDs in people with underlying health problems affecting these organs[17].

#### Acetaminophen and Adjuvant Analgesics

**Acetaminophen**, while a relatively weak analgesic, can be effective for some pain episodes in sickle cell disease[50]. The effects of acetaminophen are mostly central and it does not have much anti-inflammatory action. While the adverse effects of acetaminophen are limited in sickle cell disease compared to NSAIDs, patients should be reminded to avoid exceeding recommended daily doses as this can result in hepatic toxicity[50]. The use of combination products which contain acetaminophen along with an opioid are especially problematic and can lead to inadvertent overdose in this population[50].

Patients with sickle cell disease demonstrate aspects of both neuropathic and nociceptive pain. In addition, it is likely that recurrent episodes of severe pain lead to augmentation of pain processing and centrally mediated pain. These types of pain may respond to **adjuvant analgesics** such as antidepressants (amitriptyline and duloxetine) or anticonvulsants (gabapentin and pregabalin)[50]. Sickle cell patients with acute and chronic pain also develop sleep disorders, which can lead to increased pain complaints and decreased pain coping ability. A variety of sleep medications have been used in this population including zolpidem, trazodone, melatonin, and amitriptyline, though none of these drugs have been well studied in sickle cell disease[50].

### Pulmonary Hypertension and Priapism Management

#### Phosphodiesterase Type 5 Inhibitors

**Sildenafil** and **tadalafil**, phosphodiesterase type 5 (PDE-5) inhibitors, have demonstrated utility in managing both pulmonary hypertension and priapism in sickle cell disease through mechanisms involving nitric oxide pathway modulation[16][23][40][43]. The phosphodiesterase 5 inhibitor sildenafil has been shown to improve pulmonary hemodynamics and functional capacity in several forms of pulmonary arterial hypertension[16]. Inhibition of cyclic guanosine monophosphate (GMP) degradation by phosphodiesterase 5 increases nitric oxide (NO)-mediated pulmonary vasodilation[16].

In patients with sickle cell disease and mild-to-moderate pulmonary hypertension, chronic therapy with sildenafil improved the estimated pulmonary arterial systolic pressure and exercise capacity[16]. These effects were not related to changes in hemoglobin-oxygen carrying capacity, since sickle cell disease therapy was intensified prior to initiation of sildenafil and hemoglobin and fetal hemoglobin levels remained unchanged throughout the study[16].

For priapism, a randomized, double-blind, placebo-controlled clinical trial assessed sildenafil 50 mg daily for prevention of recurrent ischemic priapism associated with sickle cell disease[40]. Thirteen patients with sickle cell disease reporting priapism recurrences at least twice weekly were randomized to receive sildenafil 50 mg or placebo daily for 8 weeks, followed by open-label use of sildenafil for an additional 8 weeks[40]. Priapism frequency reduction by 50% did not differ between sildenafil and placebo groups by intention-to-treat or per-protocol analyses. However, during open-label assessment, 5 of 8 patients (62.5%) by intention-to-treat analysis and 2 of 3 patients (66.7%) by per-protocol analysis met the primary efficacy outcome[40]. Major priapism episodes were decreased 4-fold in patients monitored "on-treatment"[40].

Real-world effectiveness data demonstrate more substantial benefits. A retrospective chart review of patients with recurrent ischemic priapism started on regimented phosphodiesterase type 5 inhibitor therapy evaluated 24 evaluable patients (42 were initiated on therapy)[43]. Treatment decreased emergency department visits per month by 4.4-fold (p<0.001), reduced priapism duration tiers (p<0.001), and reduced priapism frequency tiers (p<0.001)[43]. Of 24 patients, 22 (92%) reported improvement in priapism outcomes, with 9 of whom reporting resolution of recurrent ischemic priapism episodes[43]. The median length of regimented phosphodiesterase type 5 inhibitor use was 3 months[43].

For those men receiving tadalafil daily, there was improvement in the rate of priapism in 71.4% (5 of 7) with self-report of improved erectile function[23]. Proposed mechanisms of action include hydroxyurea as a nitric oxide donor (improving phosphodiesterase-5 expression) and as an inducer of hemoglobin F production, reducing polymerization, sickling, and hemolysis[23]. A phase 2 trial in Nigeria (PIN trial; NCT05142254) is currently recruiting participants to assess the potential effect of moderate-dose hydroxyurea combined with tadalafil versus moderate-dose hydroxyurea with placebo in preventing priapism recurrences in adults with sickle cell anemia[23].

### Adjunctive Agents for Hemolysis and Complications

#### Magnesium Supplementation

**Magnesium** is known to widen blood vessels and, when regularly administered, improves the amount of liquid in red blood cells and can help stop their shape deforming. A systematic review included five studies with a total of 386 people with sickle cell disease aged between 4 and 53 years. Two studies (306 people) compared intravenous magnesium to placebo in people admitted to hospital as an emergency because of pain lasting until they were discharged (less than 4 weeks). Two of the three longer-term studies compared oral magnesium pidolate with placebo and the third study compared hydroxyurea and magnesium pidolate.

Oral magnesium pidolate, given over a longer period, did not reduce the severity of painful episodes and had no measurable effect on properties of sickled red cells. Oral magnesium appeared to be safe and well-tolerated with only mild side effects (diarrhea and headache). Intravenous magnesium can cause mild to moderate side effects after administration such as nausea, vomiting, feeling of warmth, and low blood pressure.

#### Omega-3 Fatty Acid Supplementation

**Omega-3 (n-3) fatty acids** show promise as a complementary approach to sickle cell disease management. Blood cell aggregation and adherence to vascular endothelium and inflammation play a central role in vaso-occlusive crisis in sickle cell disease, and the antiaggregatory, antiadhesive, antiinflammatory, and vasodilatory omega-3 fatty acids (DHA and EPA) are significantly reduced in patients with the disease. A randomized, placebo-controlled, double-blind trial investigated the therapeutic potential of omega-3 fatty acids for patients with homozygous sickle cell disease.

Omega-3 treatment reduced the median rate of clinical vaso-occlusive events (0 compared with 1.0 per year, P<0.0001), severe anemia (3.2% compared with 16.4%; P<0.05), blood transfusion (4.5% compared with 16.4%; P<0.05), white blood cell count (14.4 ± 3.3 compared with 15.6 ± 4.0 ×10³/μL; P<0.05), and the odds ratio of the inability to attend school at least once during the study period because of illness related to the disease to 0.4 (95% CI: 0.2, 0.9; P<0.05). A systematic review and meta-analysis support omega-3 fatty acid supplementation as a safe and effective complementary strategy to reduce painful vaso-occlusive crises in sickle cell disease.

#### Finerenone for Chronic Kidney Disease

**Finerenone**, a nonsteroidal mineralocorticoid receptor (MR) antagonist, has demonstrated effectiveness in kidney and cardiovascular protection in chronic kidney disease patients[7][26]. Endothelial dysfunction related to chronic hemolysis and the relative kidney hypoxia caused by vaso-occluded sickle red blood cells are probably key factors for the development of kidney complications in sickle cell disease[7]. The FIDELIO-DKD study demonstrated that patients with chronic kidney disease and type 2 diabetes treated with finerenone manifested a lower risk of a primary outcome event (kidney failure, a sustained decrease of ≥40% in the estimated glomerular filtration rate from baseline, or death from renal causes) than patients in the comparator arm receiving placebo[26]. While not yet specifically approved for sickle cell disease, finerenone offers a promising therapeutic avenue for managing chronic kidney disease complications, particularly in sickle cell disease patients with progressive renal dysfunction.

## Failed and Withdrawn Therapeutics: Important Clinical Lessons

### Poloxamer 188: Lack of Clinical Benefit

**Poloxamer 188** was evaluated as a potential agent to reduce the duration and severity of vaso-occlusive episodes[29]. A phase 3, randomized, double-blind, placebo-controlled, multicenter, international trial conducted from May 2013 to February 2016 included 388 individuals with sickle cell disease aged 4 to 65 years with acute moderate to severe pain typical of vaso-occlusive episodes requiring hospitalization[29]. A 1-hour 100-mg/kg loading dose of poloxamer 188 was followed by a 12- to 48-hour 30-mg/kg/h continuous infusion, or placebo[29].

There was no significant difference between the groups for the mean time to last dose of parenteral opioids (81.8 hours for the poloxamer 188 group versus 77.8 hours for the placebo group; difference, 4.0 hours [95% CI, -7.8 to 15.7])[29]. Based on a significant interaction of age and treatment, there was a treatment difference in time from randomization to last administration of parenteral opioids for participants younger than 16 years (88.7 hours in the poloxamer 188 group versus 71.9 hours in the placebo group), but this suggested potential harm rather than benefit[29]. These findings do not support the use of poloxamer 188 for vaso-occlusive episodes[29].

### Ticagrelor: Platelet Inhibition Insufficient for VOC Prevention

**Ticagrelor**, a reversible P2Y₁₂ inhibitor, was evaluated based on the understanding that platelets play a role in sickle cell disease-related thromboinflammation through platelet-platelet and platelet-neutrophil interactions[18]. The phase 3 HESTIA3 study assessed the efficacy and safety of ticagrelor versus placebo in preventing vaso-occlusive crises in pediatric patients with sickle cell disease[18]. Patients aged 2 to 17 years were randomly assigned 1:1 to receive weight-based doses of ticagrelor or matching placebo[18].

Despite successful inhibition of platelet activation (median platelet inhibition with ticagrelor at 6 months was 34.9% predose and 55.7% at 2 hours post-dose), ticagrelor did not decrease the frequency of vaso-occlusive crisis[18]. In fact, serious adverse events were reported in 44 patients (44%) in the ticagrelor group and 29 patients (32%) in the placebo group[18]. The most commonly reported serious adverse event by preferred term was sickle cell anemia with crisis, which was reported in 39 patients (39%) in the ticagrelor group and 24 patients (26%) in the placebo group[18]. These findings underscore the complexity of factors contributing to sickle cell disease-related vaso-occlusion and suggest that platelet inhibition alone is insufficient to prevent pain crises despite effective platelet function suppression[18].

## Combination Therapy Approaches

### Rationale for Combination Strategies

Now that multiple FDA-approved drugs are available in the United States as disease-modifying therapies in sickle cell disease, treatment strategies combining drugs targeting different disease pathophysiology are increasingly feasible[22]. The development of additional novel agents that further ameliorate clinical severity in an additive and/or synergistic manner when combined with hydroxyurea has ushered in an era of developing personalized combination drug regimens for individuals with sickle cell disease[22]. Over 30 treatment intervention trials are currently in progress investigating a wide range of agents acting by complementary mechanisms, providing the rationale for ushering in the age of effective and safe combination drug therapy for sickle cell disease[22].

### Established and Proposed Combinations

Proof of concept for combined therapies has been demonstrated in previous studies. Atweh and colleagues demonstrated that hydroxyurea and butyrate resulted in higher fetal hemoglobin levels for individuals failing treatment with butyrate alone[22]. These can be combination anti-switching therapies and/or combination of anti-switching and anti-sickling targeting one or more downstream effects[22].

The challenge for the future is how to design and conduct rational combination therapeutic regimens. These approaches will likely be guided by severity of clinical phenotype, principles of combination chemotherapy for neoplastic diseases (such as non-overlapping toxicities), and testing in preclinical animal models[22]. Oral decitabine and tetrahydrouridine newer regimens incorporating oral administration resulted in significant increases in F cells, fetal hemoglobin, and total hemoglobin in people with sickle cell disease[22].

Implementing combination therapy approaches with SGLT2 inhibitors and mineralocorticoid receptor antagonists, such as finerenone, offers a promising research avenue for treating heart failure with reduced ejection fraction and chronic kidney disease progression, based on their complementary modes of action and preclinical data supporting the efficacy of the approach in animal models[7]. The MIRACLE trial is underway in this setting[7]. Additionally, combination therapy with hydroxyurea and phosphodiesterase-5 inhibitors is being investigated for priapism prevention, based on proposed pharmacologic synergism between hydroxyurea as a nitric oxide donor and the phosphodiesterase-5 inhibitor in preventing priapism episodes[23].

## Curative Approaches: Hematopoietic Stem Cell Transplantation

### Allogeneic Transplantation: The Established Standard

A blood or marrow transplant (BMT) is one of the only cures for sickle cell disease and is also known as a bone marrow or blood stem cell transplant[12]. It replaces unhealthy blood-forming cells with healthy ones. The most common bone marrow transplant type for sickle cell disease is an allogeneic transplant, which uses healthy cells donated by a family member, an unrelated donor, or umbilical cord blood[12].

The transplant process begins with preparation involving chemotherapy (chemo) to destroy the unhealthy blood-forming cells[12]. Patients then receive healthy donor cells through an intravenous catheter, similar to a blood transfusion, which travel to the bones where they make new, healthy blood cells[12]. Recovery lasts for several months with close monitoring for complications like infections or side effects, and patients may need to stay at or near the transplant center[12].

A transplant doctor might recommend a transplant if the patient has experienced more than 3 severe pain crises in 2 years, a stroke or silent stroke, acute chest syndrome 2 or more times in the last 2 years, 8 or more red blood cell transfusions per year, high blood pressure in the lungs (pulmonary hypertension), or chronic pain lasting longer than 6 months[12]. While a transplant can stop further damage caused by sickle cell disease, it cannot fix existing organ damage or chronic pain[12].

### Complications and Long-Term Outcomes

Complications and risks of transplant for sickle cell disease include graft-versus-host disease (GVHD), in which the new donor cells attack the body such as skin, liver, and other organs[12]. Additionally, patients have a higher risk for infections while recovering from transplant, chemotherapy may damage organs like the heart, lungs, or kidneys, and patients may have extra bleeding while recovering from transplant[12].

Extended follow-up data from HLA-matched unrelated donor transplantation demonstrate that engraftment and cure are achievable with reduced-intensity conditioning regimens[41]. In a Blood and Marrow Transplant Clinical Trials Network phase II trial conducted between 2008 and 2014 of HLA-matched unrelated donor bone marrow transplantation for severe sickle cell disease in patients aged 3-19 years, the trial met the pre-specified primary endpoint of 75% 1-year event-free survival[41]. However, the incidence of 1-year acute and extensive chronic graft-versus-host disease were unacceptably high at 17% and 38%, respectively[41]. With median follow-up of 97 months, the 5- and 8-year probabilities of overall survival were 68% (95% CI 48-82%), and with a single secondary graft rejection 5 years after transplantation, the 5- and 8-year probabilities of event-free survival were 61% (95% CI 41-76%) and 57% (95% CI 37-73%), respectively[41].

Notably, there were no central nervous system, pulmonary, or vaso-occlusive events reported after successful donor engraftment[41]. No patient reported pulmonary, cardiac, hepatic, renal, or central nervous system toxicity beyond a year post-transplant[41]. Performance scores for 19 of 20 patients were reported as 90-100 (n=13) and 70-80 (n=6)[41].

Since completion of earlier trials and recognition of graft-versus-host disease-related complications, successful application of novel graft-versus-host disease prophylaxis including extended duration abatacept has been reported with reduced-intensity conditioning regimens in HLA-matched and minimally mismatched unrelated transplants[41]. These results encourage the recognition that outcomes from curative efforts are now approaching those previously described only after HLA-matched sibling donor transplantation and serve to expand curative options for sickle cell disease patients over a wide age and therapeutic range[41].

Excellent outcomes have been reported in HLA-matched related donor transplants, with Kaplan-Meier-based probability of overall survival of 100% in recent cohorts[44]. All patients were off immunosuppression at 1-year post-transplant[44].

## Contraindications and Adverse Event Considerations

### Disease-Specific Cautions with NSAIDs and Other Agents

While NSAIDs can be useful adjunctive agents in pain management, their use in sickle cell disease requires careful consideration of individual patient risk factors[17][38]. NSAIDs should be avoided in patients with underlying chronic kidney disease, as the nephropathy can be worsened by NSAIDs[34]. NSAIDs are also relatively contraindicated in patients with or at risk of developing peptic ulcer disease, liver or kidney disease, bleeding disorders, and cardiovascular disease[17][38].

Selective NSAIDs that block COX-2 but do not affect COX-1 are generally associated with a lower risk of digestive side effects, but their use has been linked to an increased risk of heart problems compared with nonselective NSAIDs[17]. The specific risks associated with NSAID use will vary depending on the specific medication, as well as the patient's genetic background and underlying health issues, and a physician should assess this when deciding if NSAIDs should be used to treat a specific patient[17][38].

NSAIDs are generally not recommended to be used during pregnancy, because they can cause problems with organ development in the fetus[17]. The U.S. Food and Drug Administration specifically recommends that NSAIDs be avoided by pregnant women from the 20th week of gestation onward due to the risk of serious kidney problems in the fetus, which can reduce the levels of amniotic fluid[17]. Pregnant women should therefore not take NSAIDs at 20 weeks or later into the pregnancy unless they have been specifically advised to do so by a healthcare professional[17].

### Antimalarial Drug Considerations in Endemic Areas

In sickle cell disease patients in malaria-endemic regions, antimalarial drug prophylaxis presents a therapeutic dilemma. A systematic review and meta-analysis of available literature determined the safety and effectiveness of antimalarial chemoprophylaxis used in sickle cell disease patients[31]. The data shows that the use of antimalarial drugs as prophylaxis in sickle cell disease patients results in reduction in malaria incidence; whereas the incidence of hospitalization, blood transfusion, vaso-occlusive crises, and mortality were not different between sickle cell disease patients on prophylaxis compared to those on placebo[31]. Taken individually, sulfadoxine-pyrimethamine and chloroquine seemed to provide better protection compared to the rest of the interventions[31].

Unexpectedly, the risk of vaso-occlusive crisis occurrence was more likely in proguanil, mefloquine-artesunate, and sulfadoxine-pyrimethamine-amodiaquine recipients compared to placebo, but less likely in those who received chloroquine and sulfadoxine-pyrimethamine[31]. The majority of studies provided information about adverse event occurrence, with reported adverse events mostly being minor (vomiting, body pain, weakness, pruritus, headache, and nausea), with the most commonly reported major adverse events being hospitalization, blood transfusion, vaso-occlusive crisis, and mortality[31]. Due to this, the use of anti-malarial monotherapies as prophylaxis in a group as vulnerable as sickle cell disease patients is of utmost concern, as the two drugs (chloroquine and sulfadoxine-pyrimethamine) that showed protective efficacy are no longer recommended for treatment because of widespread resistance[31].

## Emerging Perspectives on Therapy Selection and Future Directions

### Reassessment of Disease-Modifying Therapies

A significant clinical development has been the recent questioning of the efficacy of several previously approved therapies. The 2025 publication of the phase III STAND trial of crizanlizumab revealed no difference between the treatment and placebo groups, raising important questions about the actual benefit of P-selectin inhibition in a broader patient population[6][6]. This finding contrasts with the positive results from the preregistration trials that led to FDA approval and suggests that efficacy may not be sustained in larger, more diverse patient populations or with longer-term follow-up.

Additionally, the withdrawal of voxelotor in September 2024 represents a significant setback in the development of new sickle cell disease therapeutics[45][48]. The identification of excess deaths and vaso-occlusive crises in multiple clinical trials, particularly in populations in malaria-endemic regions, raises important questions about whether agents designed to increase hemoglobin oxygen affinity may have unintended consequences in certain patient populations. The association between voxelotor use and increased susceptibility to infections, including malaria and sepsis, suggests that hemoglobin-oxygen affinity modification may have immunosuppressive effects that were not adequately appreciated during drug development[45].

### Role of Hydroxyurea in Contemporary Practice

In light of these recent developments, there is increasing recognition that more emphasis should be placed on improving the utility of hydroxyurea in sickle cell disease[24]. Multiple studies have suggested that hydroxyurea reduces overall mortality and lengthens life span, though more rigorous controlled studies of these conclusions are still needed[24]. Recent studies in sub-Saharan Africa have established the feasibility of widespread use of hydroxyurea and its potential benefits for resource-limited communities, including in possible resistance to malaria[24]. These findings suggest that greater investment in optimizing hydroxyurea therapy worldwide and in expanding access to this relatively inexpensive oral agent may be more impactful than focusing resources on expensive gene and cell therapies with limited availability.

### Personalized Medicine Approaches

The increasing recognition of genetic heterogeneity in sickle cell disease outcomes and the variable response to disease-modifying therapies has prompted interest in developing personalized medicine approaches. The identification of genetic biomarkers predisposing patients at higher risk to develop adverse events may be considered when using non-aspirin NSAIDs[38]. Similarly, understanding the genetic and immunologic factors that predict response to hydroxyurea, novel agents, or combination therapies could allow for tailored treatment approaches that optimize efficacy while minimizing adverse effects.

## Conclusion

The therapeutic landscape for sickle cell disease has undergone remarkable transformation over the past three years, with the approval of gene therapies utilizing CRISPR-Cas9 technology marking a paradigm shift toward potentially curative approaches. Casgevy and Lyfgenia have demonstrated remarkable efficacy in eliminating vaso-occlusive crises in the majority of treated patients, representing genuine advances in the field. However, the complexity of sickle cell disease pathophysiology has been repeatedly demonstrated by unexpected clinical trial results and safety signals, most notably the withdrawal of voxelotor despite compelling preclinical rationale.

The current approved pharmacologic arsenal includes hydroxyurea, the foundational disease-modifying agent with decades of clinical experience and demonstrated mortality reduction; L-glutamine, which reduces oxidative stress; and crizanlizumab, though the recent STAND trial has questioned its sustained benefit. Multiple investigational agents targeting distinct pathophysiologic mechanisms are in various phases of clinical development, including oral DNA methyltransferase inhibitors, SGLT2 inhibitors, and GLP-1 agonists. Drug repurposing efforts have identified promising candidates, particularly statins and immunomodulatory agents, which may offer additional therapeutic options.

Symptom-specific therapies including phosphodiesterase-5 inhibitors for pulmonary hypertension and priapism, NSAIDs for inflammatory pain (with appropriate caution), and opioids for acute pain remain essential components of comprehensive sickle cell disease management. The emerging recognition of the need for combination therapy approaches, guided by principles developed in oncology and informed by preclinical model systems, represents an important evolution in treatment strategy.

Allogeneic hematopoietic stem cell transplantation remains the only established curative approach beyond gene therapy, with improving outcomes through refined conditioning regimens and graft-versus-host disease prophylaxis strategies. As the field progresses, the critical challenges ahead involve: optimizing access to curative therapies globally; developing rational combination drug strategies that address multiple disease mechanisms while minimizing overlapping toxicities; identifying biomarkers that predict response to specific therapies; and carefully evaluating the long-term safety and efficacy of novel agents in real-world populations, particularly in regions where infectious diseases remain significant comorbidities.

The dramatic evolution of sickle cell disease therapeutics over the past decade—from a disease managed primarily with supportive care and hydroxyurea to one with multiple disease-modifying options and genuine potential for cure—represents one of the most significant achievements in rare disease medicine. Yet this progress has also highlighted the importance of rigorous clinical evaluation, post-marketing surveillance, and honest reassessment of therapeutic approaches when evidence warrants. Future success will depend on balanced enthusiasm for novel approaches combined with evidence-based medicine, global health equity in access to therapies, and continued investigation of the fundamental biology underlying this complex genetic disorder.