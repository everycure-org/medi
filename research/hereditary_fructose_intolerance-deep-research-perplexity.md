---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:30:38.671604'
end_time: '2026-04-04T14:32:38.007219'
duration_seconds: 119.34
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: hereditary fructose intolerance
  mondo_id: MONDO:0009249
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 37
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** hereditary fructose intolerance
- **MONDO ID:** MONDO:0009249

## Research Objectives

Please provide a comprehensive research report on **drug therapies for hereditary fructose intolerance**.
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

# Drug Therapies for Hereditary Fructose Intolerance: A Comprehensive Research Report

Hereditary fructose intolerance (HFI) is a rare autosomal recessive metabolic disorder caused by mutations in the *ALDOB* gene encoding aldolase B, an enzyme essential for fructose metabolism[1][3]. With an estimated prevalence of 1 in 20,000 to 1 in 60,000 individuals globally[1][3], HFI presents a significant clinical challenge because the disease currently lacks any FDA-approved pharmacological treatment, with dietary restriction remaining the exclusive therapeutic intervention[1][6]. This comprehensive review synthesizes current evidence on drug therapies for HFI, examining approved treatments, investigational compounds in clinical development, drug repurposing candidates, contraindicated agents, and emerging combination approaches. The landscape of HFI therapeutics is undergoing substantial transformation with the development of ketohexokinase (KHK) inhibitors, which represent a paradigm shift from purely dietary management toward targeted molecular intervention. This report evaluates the evidence strength and clinical utility of all therapeutic approaches currently available or under investigation for HFI patients.

## Pathophysiology and Therapeutic Rationale

### Molecular Basis of Fructose Metabolism in HFI

Understanding the biochemical foundation of HFI is essential for comprehending why certain drugs are contraindicated and why emerging therapies target specific enzymatic steps. When individuals without HFI consume fructose, the monosaccharide is absorbed from the intestine through glucose transport proteins (GLUT) 5 and 2, followed by hepatic metabolism predominantly occurring through three sequential enzymatic steps[3][3]. The first committed step involves phosphorylation of fructose by fructokinase (ketohexokinase, KHK) to form fructose-1-phosphate (F-1P), which is subsequently cleaved by aldolase B into glyceraldehyde and dihydroxyacetone phosphate (DHAP), intermediates that then enter the glycolytic pathway[3][3][3].

In HFI patients, the absence or severe deficiency of functional aldolase B results in the pathological accumulation of F-1P, the toxic metabolic intermediate that cascades into multiple biochemical derangements[1][3]. This accumulation triggers intracellular phosphate sequestration, depleting inorganic phosphate (Pi) and subsequently adenosine triphosphate (ATP), leading to a vicious cycle of metabolic dysfunction[1][3]. The depletion of cellular ATP provokes increased adenosine monophosphate (AMP) degradation and subsequent inosine monophosphate (IMP) accumulation, which generates elevated levels of urate responsible for hyperuricemia observed in acute HFI manifestations[3][3]. Furthermore, F-1P accumulation inhibits both glycogenolysis through impairment of glycogen phosphorylase and gluconeogenesis through inhibition of glucose-6-phosphate isomerase, creating a dual blockade that explains the profound hypoglycemia characteristic of fructose ingestion in HFI patients[3][3][3].

This pathophysiological understanding has catalyzed the rational development of therapeutic strategies that either prevent F-1P formation through KHK inhibition or ameliorate its downstream consequences. The recognition that preventing the initial phosphorylation step could theoretically eliminate all subsequent metabolic derangements has guided the development of investigational KHK inhibitors, representing a fundamental shift from symptom management toward etiological intervention.

## Approved Drug Therapies

### Dietary Management as the Current Standard of Care

The only currently approved therapeutic intervention for hereditary fructose intolerance is dietary restriction, representing a unique situation in modern medicine where nutritional intervention rather than pharmaceutical therapy constitutes the sole definitive treatment[1][3][6][1][1]. The cornerstone of HFI management requires the complete elimination of foods and medications containing fructose, sucrose (which is a disaccharide of fructose and glucose), and sorbitol (which is metabolically converted to fructose via the polyol pathway)[1][3][5][6][13][14][1]. This dietary approach is not merely a symptom management strategy but rather the only intervention capable of preventing the toxic accumulation of F-1P and its consequent metabolic disasters[1][3][6].

Patients diagnosed with HFI should adhere to a lifelong fructose-free diet and avoid medications containing fructose to prevent poisoning[1][1]. The implementation of this dietary restriction requires meticulous attention to food composition, as fructose appears not only in obvious sources such as fruits, honey, and high-fructose corn syrup but also in numerous processed foods, pharmaceutical syrups, and infant formulas[1][11][13][19][19]. The presence of fructose (or sucrose or sorbitol) in many common infant formulas and most over-the-counter baby medicines is poorly recognized by healthcare providers, creating a dangerous gap between clinical practice and disease management requirements[1][19][19].

Clinical practice guidelines emphasize that the optimal level of dietary restriction in people with HFI is yet to be definitively established, as some individuals can achieve sufficient intake reduction to normalize liver and kidney function while others may experience chronic, nonspecific symptoms despite strict dietary adherence[1][6][1]. This variability likely reflects differences in residual aldolase B enzyme activity, genetic background, and individual metabolic capacity to handle trace amounts of fructose. Serum carbohydrate-deficient transferrin (CDT) determination has emerged as a valuable monitoring tool to assess dietary compliance and fructose, sorbitol, and sucrose intake, with a normal CDT profile considered the desired therapeutic goal for HFI patients[1][37]. A linear correlation has been established between fructose intake and serum CDT levels, enabling personalized dietary therapy optimization[1].

For infants with HFI, dietary management begins with exclusive breastfeeding or the use of sucrose-free infant formulas, achieving rigid dietary restriction in early childhood[1][6][1]. Introducing solid foods is not discouraged per se, but careful avoidance of fructose-containing foods is recommended until 2 or 3 years of age when liberalization may become tolerable in some patients[1][6]. Radically eliminating fruits, honey, most vegetables, and other foods containing fructose is recommended, while sorbitol, which is present in medicines and sugar-free products, must also be excluded[1][1][3]. Glucose, maltose, and starch can substitute for sucrose in the diet[1]. The absence of specific evidence-based dietary guidelines arises from discrepancies in food composition tables detailing sugar content across different foods and geographic regions[1][6].

Patients with HFI who adhere strictly to a fructose-sucrose-sorbitol (FSS)-free diet have an excellent prognosis with a normal lifespan[3][3][3]. However, this favorable outcome requires absolute dietary compliance throughout the patient's entire lifetime, creating significant psychological, social, and practical burden for patients and families. The long-term follow-up of HFI patients is poorly documented, but recent studies have revealed concerning findings that challenge the assumption that dietary restriction alone prevents all disease progression. One Italian study with ten years of follow-up demonstrated that hepatic steatosis persisted in 93.8 percent of patients despite strict dietary adherence to less than 1.5 grams of fructose daily[3][8]. Additionally, this same study found that approximately 37.5 percent of dietary-compliant patients continued to have elevated serum transaminase levels, suggesting that dietary management alone may be insufficient to prevent long-term chronic liver manifestations in a subset of patients[3][8].

## Contraindicated Medications and Hazardous Agents

### Fructose-Containing Pharmaceuticals and Formulations

A critical aspect of HFI management involves recognizing and avoiding medications and formulations that contain fructose, sucrose, or sorbitol, as inadvertent exposure to these compounds through pharmaceutical preparations represents a significant source of morbidity and mortality[1][11][19][19]. The FDA has issued specific guidance recognizing that there have been deaths involving the administration of intravenous solutions containing "Invert Sugar" (a mixture of 50 percent glucose and 50 percent fructose) to patients with undiagnosed or incompletely managed HFI[11][21]. This tragic history underscores the critical importance of avoiding fructose-containing intravenous fluids, as well as fructose-containing infant formulas and pharmaceuticals[11][25].

The mechanism of contraindication is straightforward: fructose, regardless of its route of administration (oral, intravenous, or parenteral), rapidly undergoes phosphorylation by KHK to form the toxic F-1P intermediate in hepatocytes, enterocytes, and proximal tubular cells[1][6][6][25]. Even brief exposure to fructose through contaminated medications can precipitate acute life-threatening manifestations including severe hypoglycemia, hepatic failure, renal tubular dysfunction, and even death[1][19][19][25]. Invert sugar is particularly hazardous because its formulation as an intravenous nutritional supplement was previously used in some hospital settings without adequate warning labels regarding the absolute contraindication in HFI patients[11][21]. The FDA has issued specific guidance that all oral dosage form drugs and all parenteral drugs which contain fructose should clearly identify this ingredient as "fructose" on labels and labeling, with the words "Fructose/Dextrose" immediately following "Invert Sugar" whenever they appear[11].

Sorbitol-containing medications represent another major source of inadvertent fructose exposure, as sorbitol is rapidly metabolized to fructose via the polyol pathway through the action of sorbitol dehydrogenase[1][25][26]. Many sugar-free medications, particularly cough syrups, antacids, and laxatives, contain sorbitol as a sweetening agent, creating a substantial risk for unsuspecting HFI patients[1][5][25]. Sucralose, while technically not metabolized to fructose, is frequently found in medications alongside sucrose or fructose and should be avoided in HFI patients[25]. The polyol pathway activation following glucose consumption, which produces endogenous fructose, has been demonstrated to trigger HFI manifestations through the same F-1P accumulation mechanism as dietary fructose exposure[26].

### Specific Classes of Contraindicated Agents

The FDA explicitly recommends avoiding enteral or parenteral exposure to fructose, sorbitol, sucrose, sucralose, and polysorbate in HFI patients, including specific formulations such as fructose, fructose-containing oligosaccharides, high-fructose corn syrup, honey, agave syrup, inverted sugar, maple-flavored syrup, molasses, palm or coconut sugar, and sorghum[25]. Additionally, medicines and formulas in which fructose or sucrose may not be listed as a primary component need to be avoided, with examples including syrups, enema solutions, some immunoglobulin solutions, and many infant and pediatric nutritional drinks[25]. Case reports have documented acute liver failure in neonates with undiagnosed HFI due to fructose-containing infant formulas, underscoring the life-threatening potential of inadvertent pharmaceutical exposure[19][19].

During hospitalization, special caution is advised to avoid use of fructose-containing intravenous fluids, which represent a particular hazard during emergency situations when HFI diagnosis may not be immediately apparent[25]. The historical tragedy of deaths from invert sugar administration has led to heightened awareness among some practitioners, yet case reports continue to document near-fatal or fatal exposures from medications that contain hidden sources of fructose or sorbitol[19][19]. This ongoing risk necessitates comprehensive medication review by specialized dietitians and metabolic specialists before initiating any pharmaceutical therapy in HFI patients.

## Investigational and Pipeline Drug Therapies

### Ketohexokinase Inhibitors: The Primary Investigational Approach

The most advanced investigational therapeutic approach for hereditary fructose intolerance involves pharmacological inhibition of ketohexokinase (KHK), the first committed enzyme in fructose metabolism[1][2][6][6]. This strategy is based on compelling preclinical evidence demonstrating that blocking the initial phosphorylation step would prevent the accumulation of the toxic F-1P intermediate and thereby eliminate the cascading biochemical derangements responsible for HFI pathology[1][3][6][6]. In experimental models, almost all the metabolic abnormalities in ALDOB-knockout mice were ameliorated when supplemented with genetic or pharmacological KHK inhibition[1][3][3].

#### PF-06835919: The Lead Ketohexokinase Inhibitor

The most clinically advanced KHK inhibitor is PF-06835919 (developed by Pfizer), a reversible inhibitor of ketohexokinase that was originally developed as a treatment for non-alcoholic fatty liver disease (NAFLD)[2][6][7][6][31]. PF-06835919 has demonstrated safety and efficacy in multiple phase II clinical trials for NAFLD, showing significant reductions in hepatic steatosis and inflammatory markers[7][31]. The pharmacological inhibition of fructokinase in humans was first demonstrated in a Pfizer safety study of 16 subjects examining PF-06835919 tolerability[1]. While this initial study did not aim to evaluate the metabolic benefit of KHK inhibition but rather was designed to test safety and tolerability, it established the proof-of-concept that KHK inhibition was feasible and tolerated in humans[1][22].

In a phase 2a randomized double-blind placebo-controlled trial for NAFLD, PF-06835919 administration at 150 mg and 300 mg once daily for 16 weeks was generally safe and well tolerated and resulted in reductions in whole liver fat in participants with NAFLD and type 2 diabetes[7][29]. The primary endpoint of percentage change from baseline in whole liver fat at week 16 showed least squares mean reductions of -5.26 percent (placebo), -17.05 percent (150 mg PF-06835919), and -19.13 percent (300 mg PF-06835919), with the 300 mg dose reaching statistical significance versus placebo (P = 0.0288)[7][29]. Treatment-emergent adverse event incidence was similar across groups (40.7 percent, 45.5 percent, and 32.7 percent in the placebo, 150 mg, and 300 mg groups respectively), with no apparent dose-related trend[7][29]. These favorable safety findings in NAFLD patients provided the clinical justification for investigating PF-06835919 in HFI patients.

The hepatic metabolic effects of PF-06835919 were further demonstrated in a subsequent proof-of-concept study examining in vivo fructose metabolism using phosphorus-31 magnetic resonance spectroscopy[6]. A 60 gram oral fructose load did not elicit a hepatic phosphomonoester (PME) peak reflecting F-1P or a transient decrease in hepatic Pi concentrations after PF-06835919 treatment compared with placebo, indicating effective KHK inhibition[6]. There was no carry-over effect, confirming the specificity of the inhibitory effect[6].

#### Clinical Trial in HFI Patients (NCT06089265)

The most significant recent development in HFI pharmacotherapy is the initiation of a clinical trial specifically examining PF-06835919 in hereditary fructose intolerance patients[2][2]. This open-label pilot study (ClinicalTrials.gov identifier NCT06089265) is evaluating the effects of PF-06835919 on fructose tolerance and intrahepatic lipid content in patients with HFI[2][2]. The study design involves treatment of three adult patients with HFI with PF-06835919 at 300 mg once daily (administered as three tablets of 100 mg in the morning) for 9 days, with five adult healthy individuals included as reference controls who do not receive active treatment[2][2].

The principal investigator for this trial is Patrick Schrauwen, PhD, at Maastricht University[2]. The main study parameters and endpoints include intrahepatic lipid content assessed by proton magnetic resonance spectroscopy at baseline and study completion, intestinal fructose tolerance assessed via visual analog scale for abdominal pain (1-10) and nausea evaluation every 5 minutes following oral fructose challenge compared to glucose, hepatic fructose tolerance assessed via serum glucose and phosphate levels after oral fructose challenge compared to healthy individuals, and renal fructose tolerance assessed via urinary glucose, phosphate, pH, and amino acid levels compared to healthy individuals[2][2].

The study protocol involves gradual exposure of HFI patients to increasing doses of either oral fructose or glucose in a blinded fashion, with doses of 2.5, 5.0, and 7.5 grams of fructose controlled with matched glucose doses for sweetness intensity and dependent on tolerability[6]. Following 9 days of PF-06835919 pretreatment, patients were exposed to paired, single-blinded oral glucose and fructose tolerance tests alternating per day[6].

#### Early Clinical Experience with PF-06835919 in HFI

Preliminary clinical results from HFI patients treated with PF-06835919 have demonstrated promising efficacy and tolerability[6]. Three patients with HFI received PF-06835919 treatment followed by fructose tolerance testing[6]. Patient A reported no intestinal complaints after 2.5 grams of fructose or glucose equivalent, with urinary fructose already increased after the run-in phase of PF-06835919 and increasing further after the oral fructose load, indicative of effective KHK inhibition[6]. There were no signs of proximal tubular dysfunction, though slight decreases in serum phosphate and glucose and increases in uric acid were observed[6].

Patient B experienced gastroenteritis during the run-in phase, which in retrospective analysis was determined to have been present before PF-06835919 treatment[6]. Patient C exhibited no gastrointestinal symptoms and no signs of proximal tubular dysfunction upon 2.5 and 5 gram fructose challenges or glucose equivalents, with blood glucose, serum phosphate, and uric acid remaining fairly stable upon both fructose tests[6]. Notably, dose-dependent increases in urinary fructose excretion were observed across all patients, reflecting the inhibition of hepatic fructose phosphorylation[6].

In conclusion regarding PF-06835919 efficacy in HFI, the drug effectively suppressed hepatic fructose phosphorylation in participants with metabolic-associated steatohepatitis (MASLD), and PF-06835919 was well tolerated and improved fructose tolerance in patients with HFI[6]. The clinical outcomes warrant further study that combines clinical pretesting to assess individual safety with longer follow-up and clinically relevant endpoints[6].

### Alternative Ketohexokinase Inhibitors

Beyond PF-06835919, other KHK inhibitors have been identified and investigated, though none have reached the level of clinical development achieved by PF-06835919. Research on molecules inhibiting fructokinase to prevent F-1P accumulation is ongoing, with fructokinase deficiency (which results in essential fructosuria, a benign metabolic disorder not known to cause clinical symptoms) emphasizing the therapeutic potential of fructokinase inhibition[1]. From the pyridine molecule, experimental inhibitors like pyridine 12 emerged as safe inhibitors of fructokinase with good results in experiments with rats and was established as a possible therapeutic method[1]. From the pyrimidine molecule, a fructokinase inhibitor began to be formed for possible clinical use, and PF06835919 was discovered, representing the evolution of this drug development program[1].

#### Osthole: A Natural Ketohexokinase Inhibitor

Osthole, a natural compound with KHK inhibitory activity, has demonstrated promising therapeutic effects in experimental models of HFI and related metabolic diseases[15][3]. Treatment with osthole, a natural KHK inhibitor, showed similar amelioration of metabolic abnormalities in ALDOB-knockout mice as genetic KHK inhibition, and additionally osthole treatment inhibited de novo lipogenesis in ALDOB knockout mice[1][3][3]. In studies examining the prevention of heart damage induced by diet-induced metabolic syndrome, osthole demonstrated reversal of cardiac hypertrophy, local hypoxia, oxidative stress, and increased activity and expression of KHK in cardiac tissue associated with metabolic syndrome[15].

Osthole is a nutraceutical that ameliorates metabolic syndrome and cardiac alterations induced by a high sugar/high fat Western diet, reducing hypoxia, cardiac damage, oxidative stress, fibrosis, and activating Nrf2, effects that were partially mediated through the blockade of KHK-mediated fructose metabolism at the cardiac level[15]. The antioxidant effects of osthole include activation of transcription factor Nrf2 that led to activating the expression of antioxidant enzymes including superoxide dismutase, catalase, glutamate-cysteine ligase, and glutathione peroxidase[15]. While osthole shows preclinical promise, clinical development in HFI patients has not been reported, and its status remains that of a research compound rather than an investigational pharmaceutical agent in formal clinical trials.

## Drug Repurposing Candidates

### Acetyl-CoA Carboxylase Inhibitors and DGAT2 Inhibitors

While not developed specifically for HFI, inhibitors targeting enzymes involved in hepatic lipid synthesis have emerged as potential therapeutic candidates for managing the chronic hepatic steatosis and metabolic dysfunction that persist in HFI patients despite dietary compliance[17][23]. Acetyl-CoA carboxylase (ACC) and diacylglycerol acyltransferase 2 (DGAT2) each play important roles in hepatic steatosis, and independent inhibition of each of these steps has been shown to reduce hepatic steatosis[23]. ACC inhibition is associated with upregulation of sterol regulatory element-binding protein 1c (SREBP1c) activity but results in reduced steatosis in hepatocytes, while inhibition of DGAT2 down-regulates SREBP1c activity, which in turn reduces hepatic lipogenesis[23].

In addition to its effects on steatosis, ACC inhibition may have direct antifibrotic effects in hepatic stellate cells, the collagen-producing fibroblast population in the liver; in rodent models, ACC inhibition abrogated a metabolic switch necessary for induction of glycolysis and oxidative phosphorylation during hepatic stellate cell activation in vitro, thereby reducing hepatic fibrosis[23]. In clinical trials, liver-targeted ACC-inhibiting agents have been associated with potent reductions in hepatic steatosis, though with accompanying elevations in serum triglycerides[23].

A significant clinical trial examined the combination of DGAT2 and ACC inhibitors in patients with NAFLD[23]. In a 6-week phase IIa trial in patients with NAFLD, DGAT2 inhibitor 300 mg twice daily plus ACC inhibitor 15 mg twice daily reduced hepatic steatosis to a similar degree as ACC inhibitor alone and to a greater degree than DGAT2 inhibitor alone, as assessed by magnetic resonance imaging-proton density fat fraction (MRI-PDFF)[23]. Importantly, the combination approach avoided the expected ACC inhibitor-associated increases in serum triglycerides that occur with monotherapy, suggesting additive or synergistic beneficial effects[23].

Recent research has identified that in HFI patients despite fructose abstinence, there remains a risk for hepatic disease and hyperlipidemia, potentially explained by fructose-independent mechanisms involving impaired fatty acid oxidation and elevated de novo lipogenesis driven by increased hepatic carbohydrate response element binding protein (ChREBP) activation[17]. Treatment with ACC and DGAT2 inhibitors reduced hepatic lipids and plasma triglycerides in aldolase B-knockout rats, suggesting that these agents may have therapeutic potential in HFI patients with persistent hepatic steatosis and hyperlipidemia despite dietary compliance[17].

### Polyol Pathway Inhibitors

The discovery that endogenous fructose production via the polyol pathway (aldose reductase to sorbitol dehydrogenase pathway) contributes significantly to HFI pathology even in fructose-restricted patients has opened a new avenue for therapeutic intervention[26]. The polyol pathway can be activated by glucose consumption, sorbitol or ethanol exposure, and stressful circumstances such as sepsis or major surgery[26]. Blockade of the polyol pathway through aldose reductase inhibition (sorbinil was used experimentally) significantly improved metabolic dysfunction and thriving in aldolase B knockout mice and increased their tolerance to dietary triggers of endogenous fructose production[26].

In glucose-exposed aldolase B knockout mice, sorbinil treatment resulted in significantly lower hepatic levels of sorbitol and fructose, reflecting decreased activity through the polyol pathway, along with lower levels of F-1P and marked improvement in overall energy charge[26]. However, the relative lack of specificity and tolerability issues with early aldose reductase inhibitors have limited their clinical development, and no specific polyol pathway inhibitors have entered clinical trials for HFI[26].

## Adverse Events and Drug-Disease Associations

### Adverse Events Relevant to HFI Management

The careful surveillance for adverse events in HFI patients receiving any therapeutic intervention is complicated by the baseline metabolic abnormalities present even in well-managed patients. Some HFI patients on a strict fructose-sucrose-sorbitol elimination diet develop several nutritional deficiencies, especially vitamins, particularly vitamin C found predominantly in fruits and vitamin B complex[3]. This nutritional deficiency risk necessitates supplementation with sugar-free multivitamins to prevent micronutrient deficiencies, specifically water-soluble vitamins[25][25].

In the context of pharmacological therapies, the adverse event profile of PF-06835919 demonstrated in NAFLD and type 2 diabetes populations is reassuring for potential HFI applications. Treatment-emergent adverse event incidence was similar across treatment groups in the phase 2a trial (40.7 percent placebo, 45.5 percent for 150 mg, and 32.7 percent for 300 mg), with no apparent dose-related trend and no serious adverse events reported[7][29]. The most common adverse reactions including laboratory abnormalities in patients receiving related therapeutic agents were decreased estrone (in males), increased urate, back pain, decreased estradiol (in males), and arthralgia[9].

### Metabolic Syndrome and Hyperlipidemia as Complicating Factors

An emerging concern in HFI management is the development of metabolic dysfunction-associated steatohepatitis (MASH) and related metabolic abnormalities in HFI patients despite dietary compliance[17]. Recent studies have revealed that individuals with aldolase B deficiency are characterized by increased hepatic fat content and glucose intolerance compared with controls, extending previous experimental findings to the human situation[10]. Glucose excursions during an oral glucose load were higher in aldolase B-deficient patients, suggesting underlying glucose intolerance independent of fructose metabolism[10]. Hypoglycosylated transferrin, a surrogate marker for hepatic F-1P concentrations, was more abundant in aldolase B-deficient patients than in controls, confirming chronic fructose-1-phosphate accumulation even in dietary-compliant patients[10]. Plasma β-hydroxybutyrate, a biomarker of hepatic β-oxidation, was lower in aldolase B-deficient patients than controls, suggesting impaired fatty acid oxidation contributing to steatosis[10].

The fructose-independent pathology observed in some HFI patients suggests that the enzymatic defect results in altered hepatic metabolism beyond the direct effects of F-1P accumulation[17]. Aldolase B deletion caused hepatic steatosis, fibrosis, and stunted growth in rats weaned on low-fructose chow, recapitulating human HFI features[17]. Upon fasting in these animals, fructose-independent hepatic steatosis and hyperlipidemia developed due to impaired fatty acid oxidation and elevated de novo lipogenesis[17]. Transcriptional and metabolomic profiling revealed increased hepatic ChREBP activation in aldolase B-knockout rats due to glycolytic metabolite accumulation caused by impaired gluconeogenesis[17].

## Combination Therapies and Integrated Approaches

### Dietary Management Combined with Future Pharmacological Therapy

The future therapeutic approach for HFI will likely involve a combination of dietary restriction with pharmacological intervention targeting the underlying metabolic dysfunction. The current evidence supports continued strict dietary adherence as the foundational therapy for HFI, with pharmacological agents such as KHK inhibitors potentially augmenting dietary management rather than replacing it[2][6][6].

Current thinking suggests that for fructose-induced NAFLD (which shares mechanistic similarities with HFI), a change in lifestyle that promotes exercise and healthy eating may be more effective than pharmacological treatment with KHK inhibitors, unless this approach is used as an adjuvant at the beginning of treatment to speed up the body's response[8]. This perspective suggests that combination approaches involving lifestyle modification, dietary management, and targeted pharmacotherapy may offer the optimal therapeutic strategy for HFI patients.

### Management of Comorbid Hepatic Disease

For HFI patients with persistent hepatic steatosis and metabolic dysfunction despite dietary compliance, combination therapy targeting multiple pathways involved in hepatic lipid metabolism may be warranted[17][23]. The synergistic effects observed with ACC inhibitor plus DGAT2 inhibitor combinations in NAFLD patients suggest that similar approaches might benefit HFI patients with therapy-resistant steatohepatitis[23]. Such combination approaches would need careful investigation to ensure safety and efficacy in the HFI population.

### Emerging Endogenous Fructose Control Strategies

The recognition that endogenous fructose production via the polyol pathway contributes significantly to HFI pathology in some patients suggests that future combination therapies might include both KHK inhibitors (to prevent the metabolism of endogenous fructose) and polyol pathway inhibitors (to prevent the generation of endogenous fructose in the first place)[26]. While this combination has not been formally tested in HFI patients, the mechanistic rationale is compelling based on experimental evidence.

## Clinical Trial Landscape and Future Directions

### Current Clinical Trials in HFI

The primary active clinical trial investigating pharmacological therapy for HFI is the ketohexokinase inhibitor study (NCT06089265) examining PF-06835919 in HFI patients at Maastricht University[2][2]. This pilot study represents the first formal clinical investigation of a targeted pharmacological agent specifically designed for HFI and represents a significant milestone in the field.

### Historical and Completed Trials

A completed clinical trial examined metabolic response to a short-term fructose-enriched diet in carriers for hereditary fructose intolerance compared to controls (NCT03545581)[4]. This study provided mechanistic insights into the metabolic consequences of aldolase B deficiency in the heterozygous carrier state, demonstrating that even carriers exhibit subclinical metabolic abnormalities despite not manifesting HFI symptoms[4].

Another completed trial examined safety and pharmacodynamics of PF-06835919 in patients with non-alcoholic fatty liver disease and type 2 diabetes (NCT03256526)[31][35]. This double-blind, placebo-controlled phase 2a study established the safety profile and metabolic effects of KHK inhibition in a large patient population, providing the critical evidence needed to justify investigations in HFI patients[31][35].

### Future Research Priorities

Future clinical investigations should prioritize long-term safety and efficacy studies of KHK inhibitors in HFI patients, examining not only acute fructose tolerance but also long-term outcomes including hepatic fibrosis progression, renal function preservation, and quality of life measures. The integration of advanced biomarkers such as serum CDT, liver fibrosis scores, and metabolomic assessments will enable more precise monitoring of disease activity and therapeutic response. Additionally, investigations into combination therapies addressing both the proximal fructose metabolism block (via KHK inhibition) and the downstream hepatic lipid accumulation (via ACC and DGAT2 inhibitors) should be pursued to optimize treatment outcomes in HFI patients with persistent hepatic disease despite dietary compliance.

## Regulatory Approval Status and Drug Development Pathway

### Current Regulatory Status

As of April 2026, no pharmacological agents have received FDA, EMA, or other regulatory agency approval specifically for the treatment of hereditary fructose intolerance[1][6][6]. Dietary management remains the only approved therapeutic approach. PF-06835919 is not approved for any indication; while the compound has demonstrated safety and efficacy in phase 2 trials for non-alcoholic fatty liver disease and type 2 diabetes, it has not yet received regulatory approval for these indications[7][29][31]. The ongoing clinical trial for PF-06835919 in HFI represents the first formal regulatory pathway exploration for a targeted pharmacological agent in this disease.

### Regulatory Pathway Considerations

The approval pathway for future HFI therapeutics will likely involve careful consideration of the rare disease designation, potentially qualifying for orphan drug status in the United States and Europe. This designation could accelerate the development and approval process through reduced regulatory requirements and potential financial incentives. The demonstration that KHK inhibition can improve fructose tolerance in HFI patients without serious adverse events (as suggested by preliminary trial results) establishes proof-of-concept for this therapeutic approach and provides a strong rationale for expanded clinical development.

## Contraindications and Special Populations

### Absolute Contraindications

Fructose and fructose-containing formulations remain absolutely contraindicated in all HFI patients regardless of severity or residual enzyme activity[1][6][11][25]. This contraindication extends to sucrose, sorbitol, sucralose, invert sugar, and related compounds[1][6][25]. Additionally, agents that activate the polyol pathway (such as high-dose glucose in certain clinical contexts) may be contraindicated or require careful monitoring in HFI patients[26].

### Special Considerations for Pediatric Patients

Pediatric HFI patients require particular vigilance regarding medication selection, as many pediatric formulations contain fructose or sorbitol as sweetening or preservative agents[1][19][19][25]. The administration of any new medication should involve verification of its fructose and sorbitol content, with consultation with specialized metabolic pharmacists recommended[1][25]. During acute illness or hospitalization, particular caution must be exercised to ensure that intravenous fluids do not contain fructose or related compounds[25].

### Pregnant and Lactating Women with HFI

While no specific pharmacological therapies are currently approved for HFI, pregnant women with HFI should maintain strict dietary management and continue taking sugar-free vitamin supplements. Emerging therapies such as KHK inhibitors will require careful investigation regarding safety in pregnancy and lactation before recommendation in this population. Carrier screening for hereditary fructose intolerance is an important form of genetic testing for those who may be at risk of passing the condition to their baby[14].

## Mechanism of Action and Pharmacodynamics of Lead Investigational Agents

### PF-06835919 Mechanism and Pharmacodynamics

PF-06835919 is a reversible inhibitor of ketohexokinase (KHK, also termed fructokinase), the enzyme that catalyzes the first committed step in fructose metabolism[2][6][6][31]. By preventing the phosphorylation of fructose to form F-1P, PF-06835919 effectively blocks the initial step in the cascade of metabolic derangements characteristic of HFI[6][6]. The drug crosses the blood-brain barrier and hepatic membranes to access intracellular KHK, where it reversibly inhibits enzyme activity[31]. Pharmacodynamic studies using phosphorus-31 magnetic resonance spectroscopy have demonstrated that PF-06835919 effectively suppresses hepatic fructose phosphorylation in humans, preventing the characteristic phosphomonoester peak that reflects F-1P accumulation[6].

The reversibility of KHK inhibition by PF-06835919 represents an important safety feature, as it allows for titration of drug effect and prevents permanent blockade of fructose metabolism[2][6][6]. This is particularly relevant given that fructokinase deficiency (essential fructosuria), which results from genetic loss of KHK activity, is benign and asymptomatic, suggesting that even complete pharmacological KHK inhibition should be well-tolerated[1][4].

## Conclusions and Clinical Practice Implications

Hereditary fructose intolerance currently lacks any FDA-approved pharmacological therapy, with dietary restriction of fructose, sucrose, sorbitol, and related compounds remaining the sole definitive treatment. However, this therapeutic landscape is undergoing significant transformation with the development of ketohexokinase inhibitors, particularly PF-06835919, which represents the first targeted molecular intervention for this disease[2][6][6]. The preliminary clinical evidence supporting the safety and efficacy of PF-06835919 in improving fructose tolerance in HFI patients is encouraging and warrants continued clinical development[6].

The recognition that dietary management alone is insufficient to prevent long-term hepatic disease in a significant subset of HFI patients despite strict compliance has created an urgent clinical need for adjunctive pharmacological therapies[3][8]. Future therapeutic approaches will likely involve combination strategies integrating dietary management with targeted pharmacological agents such as KHK inhibitors and potentially agents addressing downstream metabolic consequences such as hepatic steatosis[17][23][26]. Additional research priorities include investigations into polyol pathway inhibition to prevent endogenous fructose production and studies of agents targeting impaired hepatic β-oxidation and elevated lipogenesis[17][26].

Clinicians managing HFI patients should remain vigilant regarding medication selection and should consult with specialized metabolic pharmacists before prescribing any new medications to ensure freedom from fructose, sucrose, and sorbitol content. As investigational pharmacological agents progress through clinical development and eventually receive regulatory approval, HFI management will transition from purely dietary intervention to integrated approaches combining dietary restriction with targeted molecular therapies, potentially offering improved long-term outcomes and quality of life for patients with this rare but serious metabolic disorder.