---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:34:54.719950'
end_time: '2026-04-04T14:37:02.329471'
duration_seconds: 127.61
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: Fabry disease
  mondo_id: MONDO:0010526
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
- **Disease Name:** Fabry disease
- **MONDO ID:** MONDO:0010526

## Research Objectives

Please provide a comprehensive research report on **drug therapies for Fabry disease**.
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

# Comprehensive Analysis of Drug Therapies for Fabry Disease

Fabry disease, an X-linked lysosomal storage disorder caused by mutations in the *GLA* gene leading to deficiency of alpha-galactosidase A enzyme activity, has undergone significant therapeutic evolution over the past two decades[2][4][11]. The disease results in progressive lysosomal accumulation of globotriaosylceramide (Gb3) and its derivative lyso-Gb3 in tissues throughout the body, causing multisystemic manifestations affecting the kidney, heart, brain, and peripheral nervous system[11][21]. Over the past 25 years, therapeutic options have expanded from a single approved enzyme replacement therapy to include pharmacological chaperones, substrate reduction therapies in development, gene therapy approaches, and mRNA-based treatments, representing a paradigm shift from purely symptomatic management toward disease-modifying interventions aimed at preventing irreversible organ damage[46].

## Approved Drug Therapies: Enzyme Replacement Therapy

### Agalsidase Alfa (Replagal)

**Regulatory Status and Mechanism:** Agalsidase alfa received orphan designation from the FDA on June 22, 1998, followed by European marketing authorization under exceptional circumstances on August 3, 2001[22][40]. This recombinant human alpha-galactosidase A enzyme is produced in a human cell line to provide a human glycosylation profile that influences uptake by mannose-6-phosphate receptors on target cells[22]. The selection of the 0.2 mg/kg intravenous dose represents an attempt to temporarily saturate mannose-6-phosphate receptor capacity in the liver, allowing distribution of enzyme to other relevant organ tissues[22].

**Clinical Efficacy:** In males, agalsidase alfa treatment demonstrates the capacity to decrease plasma globotriaosylceramide (Gb3) and lyso-Gb3 levels, decrease Gb3 deposits in kidney endothelial cells, slow the decline of estimated glomerular filtration rate (eGFR), reduce or stabilize left ventricular mass, and improve nerve sensitivity, gastrointestinal symptoms, and pain[4]. The most compelling evidence comes from the Fabry Outcome Survey (FOS), which accumulated over 20 years of safety and efficacy data demonstrating that long-term treatment with agalsidase alfa in 1,864 adults with Fabry disease confirmed previously reported beneficial effects on renal function and cardiomyopathy, with annualized changes in eGFR remaining relatively stable in females and declining only slightly in males[48].

**Dosing and Administration:** The standard dosage is 0.2 mg/kg body weight administered intravenously over 40 minutes every other week[22]. Data indicates that at least 0.1 mg/kg is required to achieve a pharmacodynamic response[22]. Limited data exist on dosing in patients with advanced renal dysfunction (eGFR <60 mL/min), and no dose adjustment is recommended for patients on dialysis or post-kidney transplantation.

**Safety Profile:** Long-term safety monitoring through the FOS registry revealed that of 401 treated patients corresponding to 940 patient-years of exposure, only two serious adverse events were classified as possibly related to treatment[24]. The most common infusion-related reactions were mild and included rigors, pyrexia, flushing, headache, nausea, dyspnea, tremor, and pruritus[24]. Serious adverse events were predominantly attributed to underlying Fabry disease complications rather than treatment-related effects[24]. Five deaths were reported among treated patients, all attributed to complications of Fabry disease or concurrent diseases rather than to the medication itself[24].

### Agalsidase Beta (Fabrazyme)

**Regulatory Status:** Agalsidase beta received FDA approval on April 24, 2003, with seven years of orphan drug market exclusivity[40]. The European Commission granted marketing authorization on August 3, 2001[41]. Initially, only agalsidase beta was approved by the FDA in the United States, though regulatory efforts have since expanded approval to include alternative enzyme formulations[40].

**Mechanism and Clinical Characteristics:** Agalsidase beta is recombinant human alpha-galactosidase A enzyme administered at 1 mg/kg body weight intravenously every 2 weeks, representing a higher dose than agalsidase alfa[4][4]. Long-term clinical observational studies comparing the two enzyme replacement therapies report that agalsidase beta at higher doses appears slightly more effective than agalsidase alfa (0.2 mg/kg) regarding both cardiovascular and renal events, though no head-to-head randomized controlled trials have been conducted.

**Clinical Efficacy:** In males, agalsidase beta decreases plasma Gb3 and lyso-Gb3 and urinary Gb3 levels, decreases Gb3 deposits in different kidney cell types and endothelial cells in skin, slows the decline of eGFR, reduces or stabilizes left ventricular mass and wall thickness, improves nerve sensitivity, gastrointestinal symptoms, sweat function, pain, and quality of life[4]. A long-term analysis of patients with classic Fabry disease who participated in phase 3 clinical trials and the Fabry Registry (n=52) demonstrated that patients with low renal involvement at baseline had a mean annual eGFR decline of −1.89 mL/min/1.73 m².

**Treatment Switch Studies:** Patients previously treated with agalsidase alfa or migalastat who switched to agalsidase beta experienced significantly attenuated eGFR decline, with a reduction from −4.61 to −0.45 mL/min/1.73 m²/year in the post-switch period. Plasma lyso-Gb3 levels progressively reduced after switching, supporting the dose-dependent efficacy of agalsidase beta.

### Pegunigalsidase Alfa (Elfabrio)

**Regulatory Status and Mechanism:** Pegunigalsidase alfa is a novel PEGylated form of alpha-galactosidase A produced in a PlantCell Ex system using plant-based biologics technology[5][8]. The pegylation (polyethylene glycol modification) of the enzyme extends the half-life, allowing for more convenient dosing intervals and potentially reducing immunogenicity compared to non-pegylated formulations[5].

**Clinical Development and Efficacy:** In the BRIDGE phase 3 open-label switch-over study of 22 patients with Fabry disease who had been previously treated with agalsidase alfa for at least 2 years, pegunigalsidase alfa (1 mg/kg every 2 weeks) demonstrated superior renal outcomes[8]. Before switching to pegunigalsidase alfa, the mean annualized eGFR slope was −5.90 (1.34) mL/min/1.73 m²/year; 12 months post-switch, the mean eGFR slope improved to −1.19 (1.77) mL/min/1.73 m²/year, and mean plasma lyso-Gb3 reduced by 31%[8]. The therapy demonstrated a 50% reduction in Gb3 in kidney biopsy samples during the first 6 months of a clinical trial, with higher clearance in patients carrying classic mutations[5].

**Extended Dosing Intervals:** Notably, in March 2026, the European Commission approved an every-4-weeks dosing regimen of pegunigalsidase alfa at 2 mg/kg body weight for adults living with Fabry disease who are stable with prior enzyme replacement therapy, based on data from the BRIGHT study and its ongoing extension (CLI-06657AA1-03). This represents a meaningful advancement in reducing treatment burden, extending the infusion interval from every 2 weeks to every 4 weeks for eligible patients.

**Immunogenicity Considerations:** In the BRIDGE study, seven patients (35%) developed anti-drug antibodies at one or more study timepoints, with two having pre-existing antibodies at baseline[8]. Importantly, mean changes in eGFR slope for ADA-positive and ADA-negative patients were +5.47 and +4.29 mL/min/1.73 m²/year respectively, suggesting no negative impact of pegunigalsidase alfa anti-drug antibodies on eGFR slope[8].

**Safety Profile:** Pegunigalsidase alfa was well-tolerated, with 97% of treatment-emergent adverse events being mild or moderate in severity, and only 9% requiring discontinuation due to adverse events[8]. Five patients (23%) reported infusion-related reactions, a manageable profile for long-term therapy[8].

## Approved Drug Therapies: Pharmacological Chaperone Therapy

### Migalastat (Galafold)

**Regulatory Approval and Mechanism:** Migalastat, a small-molecule pharmacological chaperone, received FDA approval in August 2018 based on results from the Study to Evaluate the Efficacy, Safety and Pharmacodynamics of AT1001 in Patients With Fabry Disease and AT1001-Responsive GLA Mutations (FACETS), a phase III randomized, double-blind, placebo-controlled study with an open-label extension[5][9]. Unlike enzyme replacement therapies that provide exogenous enzyme, migalastat selectively and reversibly binds to the active site of amenable mutant forms of alpha-galactosidase A, stabilizing the enzyme, preventing its retention in the endoplasmic reticulum, and enabling its trafficking to lysosomes[4][7]. The mechanism fundamentally differs from ERT: migalastat acts as a strong competitive inhibitor of alpha-galactosidase A; however, at lower therapeutic doses, it increases enzymatic activity for amenable alpha-galactosidase A mutations[7].

**Patient Selection and Eligibility:** Migalastat is exclusively indicated for patients with migalastat-amenable *GLA* mutations—approximately 35-50% of the Fabry disease population[9][47]. Genetic testing for amenability is essential, as patients with non-amenable mutations will not respond to this therapy. A searchable database of amenable *GLA* variants is maintained by the manufacturer to facilitate clinical decision-making[47].

**Dosing:** The approved oral regimen is 123 mg once every other day[4][5]. Critically, migalastat administration requires specific fasting requirements: patients must not eat food or consume products containing caffeine for a minimum of 2 hours before and 2 hours after taking migalastat, providing a minimum 4-hour fasting window[26].

**Clinical Efficacy - FACETS Trial:** In the FACETS trial evaluating enzyme replacement therapy-naive patients with both migalastat-amenable and non-amenable *GLA* mutations, there was no significant difference between migalastat and placebo for the proportion of patients achieving a ≥50% reduction in the number of globotriaosylceramide inclusions in kidney interstitial capillaries at 6 months in the intent-to-treat population[9]. However, in the modified intent-to-treat population (patients with migalastat-amenable *GLA* mutations), migalastat treatment significantly reduced the mean number of Gb3 inclusions in kidney interstitial capillaries and plasma lyso-globotriaosylsphingosine levels at 6 months relative to placebo[9].

**Clinical Efficacy - ATTRACT Trial:** The ATTRACT trial in enzyme replacement therapy-experienced patients demonstrated that renal function was maintained during 18 months of migalastat or enzyme replacement therapy; however, migalastat significantly reduced cardiac mass compared with enzyme replacement therapy[9]. Long-term follow-up data showed notable reduction of mean left ventricular mass index relative to baseline at 18 or 24 months of migalastat therapy[7]. An open-label extension study by Feldt-Rasmussen et al. demonstrated notable reduction of left ventricular mass index, well tolerability of migalastat, and long-term stability of renal function after 30 months of migalastat 150 mg every other day in patients with Fabry disease and amenable alpha-galactosidase A variants with left ventricular hypertrophy[7].

**Safety and Tolerability:** Migalastat is generally well tolerated without severe side effects[7]. The most common adverse effects include headache, stuffy or runny nose, sore throat, urinary tract infection, nausea, and fever[26]. Clinical data proved that during migalastat therapy, left ventricular hypertrophy decreased and kidney function was stabilized in most patients[7].

**Treatment Switching:** Migalastat therapy is applicable to enzyme replacement therapy-naive as well as enzyme replacement therapy-experienced patients, with switching from enzyme replacement therapy to migalastat being feasible. In a study evaluating the switch from ERT to migalastat, kidney results demonstrated that migalastat has a similar effect to ERT, stabilizing renal function in terms of glomerular filtration rate loss and proteinuria[42]. Migalastat should not be administered concomitantly with enzyme replacement therapy[42].

## Investigational and Pipeline Drug Therapies

### Substrate Reduction Therapy: Glucosylceramide Synthase Inhibitors

**Therapeutic Rationale:** Substrate reduction therapy operates through a mechanistically distinct approach from enzyme replacement therapy[6]. Rather than attempting to replace deficient enzyme activity, substrate reduction therapies work by inhibiting glucosylceramide synthase, the enzyme involved in the production of complex glycosphingolipids including globotriaosylceramide[6]. By reducing the production of these harmful substances at the source, substrate reduction therapy addresses disease progression independently of alpha-galactosidase A activity levels[6].

**Lucerastat (GZ667161)**

Lucerastat is an investigational oral glucosylceramide synthase inhibitor being tested as a potential monotherapy for Fabry disease[5]. Preliminary data demonstrated reduction of Gb3 from superficial skin capillary endothelium and plasma lyso-Gb3 in treatment-naive Fabry patients[4]. When lucerastat was added to enzyme replacement therapy, it resulted in reduction of plasma Gb3, whereas no reduction was seen in patients under enzyme replacement therapy alone, with stabilization of renal and cardiac parameters at 12 weeks[4]. The combination approach merits investigation as substrate reduction therapy in addition to enzyme replacement therapy may provide a new form of combination therapy with potential complementary and additive benefits[5].

**Venglustat (GZ402671)**

Venglustat, an oral, brain-penetrant glucosylceramide synthase inhibitor developed by Sanofi Genzyme, represents a particularly promising substrate reduction therapy candidate due to its capacity to cross the blood-brain barrier[6]. This characteristic addresses a critical unmet medical need, as enzyme replacement therapies have limited ability to penetrate this barrier and therefore ineffectively manage neurological symptoms[6].

However, recent phase 3 data present a mixed clinical picture. The Peridot study assessed venglustat in 122 patients with Fabry disease but missed its primary endpoint of demonstrating superiority in patient-reported improvement of neuropathic pain in upper and lower extremities as well as abdominal pain compared to placebo[39]. Sanofi explained this failure by pointing to "reduction in neuropathic and abdominal pain observed in both study arms," suggesting a placebo effect or natural history effect[39].

In contrast, the Leap2Mono trial in type 3 Gaucher disease showed more promise. The 21 patients who received venglustat saw statistically significant improvements in neurological symptoms at week 52 as measured by scales for ataxia and neuropsychological status compared to the 22 patients receiving enzyme replacement therapy (p=0.007)[39]. When it came to non-neurological secondary endpoints like changes in spleen volume, liver volume, and hemoglobin level, venglustat performed as well as enzyme replacement therapy[39].

Despite the Fabry failure, Sanofi continues development of venglustat for cardiac indications. The company is still running a phase 3 study of venglustat on left ventricular mass index in patients with Fabry disease[39]. Venglustat was well tolerated in both studies, with the most common adverse events being headache, nausea, spleen enlargement (in GD3), and diarrhea[39].

### Gene Therapy Approaches

**FLT190 (AAV-Based Gene Therapy)**

FLT190 represents a recombinant adeno-associated viral (AAV) vector-based approach to Fabry disease therapy, currently in phase 2 evaluation through the MARVEL1 trial (NCT04040049)[16]. This represents a fundamentally different therapeutic paradigm: rather than chronically replacing deficient enzyme, gene therapy aims to achieve sustained enzyme production by delivering functional *GLA* gene sequences to target cells[16].

The MARVEL1 study is a multinational, open-label study assessing safety and efficacy of FLT190 administered as a single intravenous infusion in adult male participants with classical Fabry disease[16]. The study involves two parts: Part 1 focuses on previously treated patients with dose escalation, while Part 2 involves previously untreated patients with dose expansion[16]. Enrollment criteria specify adult males aged ≥18 years with confirmed classical Fabry disease, decreased plasma alpha-galactosidase activity at screening, one or more characteristic features of classic Fabry disease, estimated glomerular filtration rate ≥60 mL/min/1.73 m², and less than 500 mg/g urine protein-to-creatinine ratio[16].

A preclinical study of gene therapy using adeno-associated viral vector 9 (AAV-9) mediating widespread *GLA* expression demonstrated the ability to cross the blood-brain barrier and prevent glycosphingolipid accumulation when administered to both presymptomatic and symptomatic animals, suggesting potential benefit for neurological manifestations inadequately addressed by conventional therapies.

### mRNA-Based Therapy

**Systemic mRNA Therapy for Alpha-Galactosidase A Production**

Systemic mRNA therapy represents an emerging treatment modality capable of stimulating endogenous production of therapeutic alpha-galactosidase A enzyme[5][17]. Preclinical proof-of-concept studies demonstrate that intravenous administration of messenger RNA formulated in lipid nanoparticles can produce functional alpha-galactosidase A enzyme in target cells[17].

**Preclinical Evidence:** Single intravenous administration of human alpha-galactosidase A mRNA to *Gla*-deficient mice showed dose-dependent protein activity and substrate reduction, with long duration (up to 6 weeks) of substrate reductions in tissues and plasma observed after a single injection[17]. Repeat intravenous administration of human alpha-galactosidase A mRNA showed sustained pharmacodynamic response and efficacy in Fabry mice model[17]. Multiple administrations to non-human primates confirmed safety and translatability[17].

**Mechanism and Advantages:** The approach involves packaging mRNA into biodegradable lipid nanoparticles that, once administered intravenously, are taken up by the liver and translated into therapeutically active alpha-galactosidase A protein[17]. The protein is produced within the liver, then secreted into circulation, internalized systemically, and targeted to lysosomes via endocytosis[17]. Therapeutic proteins made from exogenously administered mRNA may mimic endogenous target proteins more closely than recombinant proteins manufactured from CHO, human, or plant cell lines[17].

**Tissue Distribution and Efficacy:** Heart and kidney of Fabry-affected individuals are generally the most affected organs, with cardiac lesions and eGFR decline representing clinical hallmarks of the disease. Data demonstrate that human alpha-galactosidase A produced in liver from administered mRNA distributes to heart, kidney, and spleen, as confirmed by tissue enzyme activity measurement, immunohistochemistry, and liquid chromatography-tandem mass spectrometry[17].

In Fabry mice, lyso-Gb3 levels were reduced by 80% in liver and spleen and greater than 50% in kidney and heart compared to untreated control animals following a single dose of mRNA therapy[17]. Importantly, enzymatic activity was detectable in plasma, heart, and liver tissues at day 28 after dosing in pilot duration studies, suggesting sustained therapeutic benefit[17].

**Clinical Translation Status:** While these preclinical studies demonstrate proof-of-concept, clinical development remains in early stages. The approach addresses a critical unmet need in Fabry disease management by avoiding the necessity for biweekly intravenous infusions and potentially providing more physiologic enzyme replacement than recombinant preparations[17].

### Other Investigational Approaches

**Modified Enzyme Replacement Therapies:** Beyond pegylation, other investigational approaches to enzyme delivery include modification of the enzyme to increase the duration of therapeutic plasma concentrations, alternative expression systems, and bioengineered variants designed to improve tissue penetration and reduce immunogenicity[5].

**Combination Approaches:** Evidence supports the complementary utility of combining substrate reduction therapy with enzyme replacement therapy. A preclinical study of Genz-682452 (a glucosylceramide synthase inhibitor) combined with enzyme therapy in Fabry mice demonstrated that mice treated with a combination of enzyme and Genz-682452 had the greatest reduction in Gb3 and lyso-Gb3[50]. Importantly, because Genz-682452, but not alpha-galactosidase A, can traverse the blood-brain barrier, levels of accumulated glycosphingolipids were reduced in the brain of drug-treated but not enzyme replacement therapy-treated mice[50]. These results suggest that combining substrate reduction and enzyme replacement may confer both complementary and additive therapeutic benefits in Fabry disease[50].

## Drug Repurposing and Off-Label Candidates

### Acetylsalicylic Acid (Aspirin) as Chaperone Potentiator

**Rationale and Mechanism:** Recent drug repositioning research has identified acetylsalicylic acid (aspirin) as a potential synergistic agent to enhance the stabilizing effects of pharmacological chaperones in Fabry disease[33]. The mechanistic basis involves acetylsalicylic acid modulating proteins involved in lysosomal trafficking and biogenesis. Specifically, proteins such as SNARE-associated protein Snapin, encoded by the *SNAPIN* gene, are strongly upregulated in acetylsalicylic acid-treated cells[33]. Snapin is a component necessary for lysosome-related organelle biogenesis and is heavily involved in intracellular vesicle trafficking and lysosome movement[33].

**Clinical Implications:** This drug repurposing approach suggests that acetylsalicylic acid can be used in synergy with pharmacological chaperones to prolong their stabilizing effects on alpha-galactosidase A[33]. While clinical trial data remain limited, this represents a potential low-cost adjunctive strategy to enhance existing chaperone therapy efficacy in appropriately selected patients.

### Galactose as Pharmacological Chaperone

**Therapeutic Mechanism:** Galactose is a low-affinity pharmacological chaperone that can prolong alpha-galactosidase A half-life in patient-derived cells treated with recombinant human alpha-galactosidase A[2]. Intriguingly, at low concentrations, galactose increases enzymatic activity, while at higher concentrations it acts as an inhibitor[2]. This concentration-dependent dual activity requires careful therapeutic optimization.

**Clinical Case Evidence:** A remarkable case report documented treatment of a Fabry disease patient carrying a cardiac variant (G328R) with infusions of galactose, leading to remarkable clinical improvement[2]. The patient progressed from NYHA functional class IV to class I, with cardiac transplantation no longer required due to this clinical improvement, and the patient returned to full-time work as a bus driver[2].

### Small Molecule Chaperones Beyond Migalastat

**1-Deoxygalactonojirimycin (DGJ):** An iminosugar analog synthesized in 1999, DGJ is a potent competitive inhibitor of alpha-galactosidase A that, at concentrations lower than those usually required for intracellular inhibition of the enzyme, effectively enhanced alpha-galactosidase A activity in Fabry lymphoblasts[2]. Currently being studied in phase 3 clinical trials as a chaperone therapeutic agent for Fabry disease, DGJ represents an alternative pharmacological chaperone approach to migalastat[27].

**Other Candidate Chaperones:** Compounds including ambroxol and pioglitazone have been shown in *in vitro* cell culture studies to augment lysosomal activity of alpha-galactosidase A in Fabry disease[29], suggesting potential for clinical evaluation.

## Contraindicated Medications in Fabry Disease

Certain medications are contraindicated in patients with Fabry disease due to their potential to inhibit residual alpha-galactosidase A enzyme activity or exacerbate disease manifestations[10][29][10]. These include:

**Amiodarone:** This antiarrhythmic agent should be avoided in Fabry patients because it may inhibit intracellular galactosidase activity[29]. Given that many Fabry patients develop cardiac arrhythmias and may be at risk for amiodarone therapy, alternative antiarrhythmic agents are preferable[29].

**Chloroquine:** Included in the list of medications contraindicated in Fabry disease[10][10].

**Benoquin:** Contraindicated in Fabry disease[10][10].

**Gentamicin:** This aminoglycoside antibiotic should not be co-administered with enzyme replacement therapy in Fabry disease[10][10].

The mechanistic basis for these contraindications relates to the critical dependence of Fabry disease management on maintaining whatever residual alpha-galactosidase A activity remains and preventing medication-induced further inhibition of enzyme function.

## Adverse Events and Complications

### Infusion-Related Reactions with Enzyme Replacement Therapy

Infusion-related reactions represent the most common treatment-related adverse events with enzyme replacement therapy. Overall, 51 patients (12.7% of patient cohorts) reported infusion-related reactions, with such reactions being much more frequent in male than female patients[24]. Because the same reaction recurred following several infusions in some patients, approximately 240 infusions were involved, representing about 1% of the estimated number of infusions administered to patient cohorts[24].

Fortunately, 41 patients (80.4%) experienced a limited number (1-5) of infusion-related reactions that did not recur following subsequent infusions[24]. The most frequent infusion-related symptoms were rigors, flushing, pyrexia, dyspnea, headache, and nausea[24]. In fewer than 15% of patients, the drug has been associated with mild acute infusion effects during or within 1 hour of infusion[24].

The most common symptoms are chills and facial flushing, typically transient[24]. In the majority of cases, infusion-related reactions are managed conservatively without stopping treatment. In fewer than 15% of cases requiring more severe and persistent interventions, the infusion can be interrupted temporarily (5-10 minutes) until symptoms subside, then restarted[24].

As of March 2005, only three patients had experienced severe treatment-related infusion reactions with symptoms including pyrexia, rigors, urticaria, and dyspnea with bronchospasm[24]. Such reactions generally occur within the first 2-4 months of treatment initiation with enzyme replacement therapy. Only one patient discontinued enzyme replacement therapy due to recurrent infusion reactions[24].

### Anti-Drug Antibody Formation and Clinical Consequences

**Prevalence and Neutralizing Activity:** A critical adverse immunological effect of enzyme replacement therapy involves development of anti-drug antibodies. Approximately 73% of patients treated with agalsidase-beta and 24% of patients treated with agalsidase-alfa reportedly develop anti-drug antibodies[25]. Recent studies demonstrate that IgG antibodies, particularly IgG4 isotype, mediate neutralizing activity that attenuates therapy efficacy[25]. Once neutralizing anti-drug antibodies occur, their formation appears to be irreversible, with the majority of affected patients remaining neutralizing anti-drug antibody-positive over 10 years[25].

**Effect on Treatment Efficacy:** Studies of patients with neutralizing anti-drug antibodies demonstrate that these antibodies can significantly reduce the therapeutic effectiveness of enzyme replacement therapy. A model explaining this mechanism proposes that during infusions in patients with anti-drug antibodies, the infused recombinant enzyme is directly inactivated (neutralized) by antibodies in the plasma[25]. Additionally, binding of anti-drug antibodies to enzyme leads to activation of macrophages that internalize enzyme-antibody complexes, decreasing cellular uptake of free enzyme[25].

**Dose-Dependent Overcoming of Antibody Inhibition:** Evidence suggests that increasing enzyme replacement therapy doses can partially overcome antibody-mediated inhibition. Studies show that in patients with anti-drug antibodies switched from agalsidase-beta 0.2 to 1.0 mg/kg every 2 weeks, plasma lyso-Gb3 and Gb3 significantly declined after 1 year, possibly due to supersaturation of antibodies[25]. A study comparing high-dose agalsidase-beta (1.0 mg/kg) with standard-dose agalsidase-alfa (0.2 mg/kg) in anti-drug antibody-positive patients demonstrated improved biochemical response with higher enzyme doses[25].

**Risk Factors for Anti-Drug Antibody Development:** A predictive model identified that nonsense and frameshift mutations in the *GLA* gene, higher plasma lyso-Gb3 at baseline, and agalsidase beta as first treatment are significantly associated with anti-drug antibody development. Notably, patients with completely absent alpha-galactosidase A enzyme activity (zero residual activity) demonstrate higher rates of anti-drug antibody development than those with residual enzyme activity, as absence of residual enzymatic activity is considered causative of lack of immune tolerance[24].

### Potential Strategies to Prevent Anti-Drug Antibody Formation

Evidence from other lysosomal storage disorders suggests potential approaches. In hemophilia patients, starting treatment with lower-dosed prophylactic recombinant factor at regular intervals was associated with a 60% lower risk of anti-drug antibody development compared to high-dose on-demand regimens. Immunosuppressive therapy administered to kidney or heart transplant recipients demonstrated efficacy in preventing de novo anti-drug antibody formation in enzyme replacement therapy-naive patients, with higher-dose immunosuppressive medications reducing antibody levels[46]. However, this protection is not durable, and these drugs' side effects must be considered[46].

## Combination Therapies and Treatment Sequencing

### Enzyme Replacement Therapy Plus Substrate Reduction Therapy

**Preclinical and Early Clinical Evidence:** Accumulating evidence supports complementary utility of combining enzyme replacement therapy with substrate reduction therapy. In Fabry mice, the combination approach achieved the greatest reduction in both Gb3 and lyso-Gb3[50]. Critically, substrate reduction therapy agents that cross the blood-brain barrier (unlike enzyme replacement therapy) achieved significant Gb3 reduction in brain tissue[50]. When treatment was initiated early in life, substrate reduction therapy was particularly effective at limiting substrate accumulation in the intestines, addressing gastrointestinal manifestations inadequately managed by enzyme replacement therapy alone[50].

**Clinical Trial Data:** In one small clinical series, lucerastat added to enzyme replacement therapy resulted in reduction of plasma Gb3, whereas no reduction was seen in patients under enzyme replacement therapy alone, with stabilization of renal and cardiac parameters at 12 weeks[4].

### Enzyme Replacement Therapy Combined with Pharmacological Chaperones

**Mechanistic Rationale:** Combining enzyme replacement therapy with low-affinity pharmacological chaperones represents a potential optimization strategy. Preliminary results demonstrate that galactose, a low-affinity pharmacological chaperone, can prolong recombinant human alpha-galactosidase A half-life in patient-derived cells[2]. This combination approach theoretically allows reduction of recombinant enzyme dosage requirements while maintaining therapeutic efficacy through enhanced enzyme stability.

**Current Clinical Context:** However, it is important to note that migalastat (the only clinically available pharmacological chaperone) should not be administered concomitantly with enzyme replacement therapy[42]. This important clinical caveat reflects potential pharmacokinetic or pharmacodynamic interactions that have not been fully characterized.

### Enzyme Replacement Therapy with Antihypertensive and Renoprotective Agents

**Angiotensin-Converting Enzyme Inhibitors and Angiotensin Receptor Blockers:** In conjunction with enzyme replacement therapy, ACE inhibitors or angiotensin receptor blockers can reduce proteinuria and stabilize renal function in Fabry patients[44]. Recent recommendations for Fabry patients receiving enzyme replacement therapy include ACE inhibitor or angiotensin receptor blocker treatment to reduce urinary protein excretion to <0.5 g/24 hours, with the goal of reducing kidney function loss to less than −1 mL/min/1.73 m² per year[44]. These agents are considered foundational supportive therapy rather than disease-modifying treatment but are essential components of comprehensive management.

### Pain Management Adjunctive to Disease-Specific Therapy

**Neuropathic Pain Agents:** Tricyclic antidepressants, serotonin and norepinephrine reuptake inhibitors (such as duloxetine and venlafaxine), carbamazepine, gabapentin, and pregabalin are considered first-line neuropathic pain agents[28]. Lidocaine patches, topical capsaicin (8%) patches, and tramadol are considered second-line options, with strong opioids as third-line options[28]. Importantly, analgesic drugs only provide symptomatic treatment and must be combined with disease-specific enzyme replacement therapy or chaperone therapy to address underlying pathology[28].

## Supportive and Symptomatic Treatments

### Pain Management Strategies

Comprehensive pain management in Fabry disease requires individualized strategies combining disease-specific treatment with enzyme replacement therapy, adjunctive symptomatic pain management with analgesics, and lifestyle modifications[28][28]. While enzyme replacement therapy has demonstrated capacity to reduce overall pain scores and pain intensity in patients with Fabry disease, pain does not always completely resolve, explaining the necessity for adjunctive medications[28].

Lifestyle modifications including use of air conditioning to avoid overheating, removing shoes and socks during pain attacks, rapid treatment of fever or infections, and maintaining good hydration help avoid pain triggers[28]. In terms of small nerve fiber sensory function, a study of 22 patients demonstrated improved thermal perception and vibration detection thresholds with agalsidase beta[28].

### Gastrointestinal Symptom Management

Gastrointestinal symptoms including postprandial abdominal pain, diarrhea, nausea, bloating, and vomiting are typical for Fabry patients, particularly those with classical phenotype[31]. While enzyme replacement therapy addresses underlying disease, symptomatic management is necessary. Patients with acute diarrhea can be treated with classical antidiarrheal medication such as loperamide[31]. Dietary modifications including smaller, more frequent meals, avoiding fried or greasy foods, and maintaining adequate hydration help prevent symptoms.

### Cardiovascular Management

Amiodarone, often used for arrhythmia management, should be avoided as it may inhibit intracellular galactosidase activity[29]. Alternative antiarrhythmic agents and devices (pacemakers, implantable cardioverter-defibrillators) may be necessary for cardiac management. In patients with significant symptomatic heart failure and intractable arrhythmia despite optimal therapy, heart transplantation may be a viable option[43], though the progressive multiorgan nature of Fabry disease complicates transplant outcomes compared to non-Fabry cardiomyopathy patients.

## Recent Clinical Developments and Regulatory Updates

### Pegunigalsidase Alfa Four-Week Dosing Approval

In a significant clinical advancement for April 2026, the European Commission approved an every-4-weeks dosing regimen of pegunigalsidase alfa at 2 mg/kg body weight for adults with Fabry disease who are stable on enzyme replacement therapy. This decision follows a positive opinion from the EMA Committee for Medicinal Products for Human Use and represents a meaningful reduction in treatment burden, extending the infusion interval from every 2 weeks to every 4 weeks for eligible patients. The approval is informed by results from the BRIGHT study (formally PB-102-F50) and its ongoing extension study, designed to assess adverse event profile, efficacy, and pharmacokinetics of this alternative dosing regimen.

### Venglustat Mixed Phase 3 Results

In February 2026, Sanofi released phase 3 results for venglustat in Fabry disease showing mixed outcomes[39]. The Peridot study in 122 Fabry patients missed its primary endpoint of demonstrating superiority in patient-reported neuropathic pain compared to placebo, though the company noted that pain reduction was observed in both study arms, suggesting natural history or placebo effects[39]. Despite the Fabry setback, Sanofi continues development of venglustat for cardiac indications with an ongoing phase 3 study of its effect on left ventricular mass index[39]. The company remains focused on rare disease research given the demonstrated efficacy of venglustat in type 3 Gaucher disease[39].

## Conclusion

Fabry disease therapeutics has evolved dramatically from a single enzyme replacement therapy approved in 2001 to a diverse portfolio of disease-modifying approaches including multiple enzyme formulations with varying immunogenicity profiles, a pharmacological chaperone with patient-specific applicability, substrate reduction therapies in late-stage development, gene therapy approaches under clinical investigation, and novel mRNA-based technologies in preclinical validation. Current evidence supports early diagnosis and treatment initiation to prevent irreversible organ damage, with therapy selection informed by specific *GLA* mutations, residual enzyme activity, and disease phenotype. Emerging data suggesting synergistic benefits of combination approaches (enzyme replacement therapy plus substrate reduction therapy) and novel administration strategies (extended-interval dosing for pegylated enzyme) continue to expand clinical options and reduce treatment burden. However, challenges including anti-drug antibody formation, limited effectiveness of current therapies in preventing certain disease manifestations (particularly cerebrovascular events), and disease heterogeneity necessitate ongoing clinical research and development of next-generation therapeutic approaches to achieve comprehensive disease modification in all patients.