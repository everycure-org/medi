---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T10:20:38.819040'
end_time: '2026-04-04T10:23:02.637214'
duration_seconds: 143.82
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: hereditary neuropathy with liability to pressure palsies
  mondo_id: MONDO:0008087
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 44
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** hereditary neuropathy with liability to pressure palsies
- **MONDO ID:** MONDO:0008087

## Research Objectives

Please provide a comprehensive research report on **drug therapies for hereditary neuropathy with liability to pressure palsies**.
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

# Comprehensive Research Report on Drug Therapies for Hereditary Neuropathy with Liability to Pressure Palsies

Despite significant advances in understanding the molecular basis of hereditary neuropathy with liability to pressure palsies (HNPP), no disease-modifying treatments have yet been approved by major regulatory agencies, and current clinical management remains primarily symptomatic and supportive. HNPP, an autosomal dominant peripheral neuropathy caused by heterozygous deletion or point mutations in the peripheral myelin protein 22 (PMP22) gene, represents a significant therapeutic challenge due to the haploinsufficiency mechanism underlying the disorder. Current management focuses on preventive strategies to avoid nerve compression and trauma, alongside pharmacological approaches to manage neuropathic pain symptoms that increasingly are being recognized as a core manifestation of the disease. The therapeutic landscape for HNPP is rapidly evolving, with multiple investigational approaches targeting the molecular basis of PMP22 dosage imbalance, including small molecule compounds identified through high-throughput screening, gene therapy vectors, and signaling pathway modulators showing encouraging preclinical results. This report synthesizes current evidence regarding available treatments, drugs in clinical development, repurposing candidates with potential efficacy, critical contraindications due to neurotoxic properties, and emerging therapeutic strategies designed to address the root cause of this rare hereditary neuropathy.

## Current Clinical Status: The Absence of Disease-Modifying Therapies

### Fundamental Treatment Limitations

The treatment of HNPP presents a fundamental challenge grounded in the genetic pathophysiology of the disease. **No specific treatment for the underlying genetic or biochemical defect exists, and no special diet or vitamin regimen is known to alter the natural course of HNPP**, according to Thomas Bird, MD, writing for GeneReviews, a genetic resource page for clinicians[1]. This statement reflects the reality that despite decades of research and considerable understanding of the PMP22 gene's role in peripheral myelin maintenance, translating this knowledge into efficacious disease-modifying therapy has proven exceptionally difficult. The disease is characterized by recurrent acute sensory and motor neuropathy in single or multiple nerves, with the most common initial manifestation being acute onset of non-painful focal sensory and motor neuropathy in a single nerve, typically presenting in the second or third decade of life[1][1][1].

The challenge of treating HNPP is fundamentally different from treating CMT1A, which results from PMP22 duplication causing protein overproduction. In HNPP, heterozygous deletion of the 1.5 megabase region on chromosome 17p12 containing the PMP22 gene results in haploinsufficiency, where the remaining single functional copy of the gene produces insufficient PMP22 protein to maintain normal myelin structure and function. This dosage-sensitive situation means that therapies must precisely restore PMP22 levels to physiological ranges without exceeding them, as excessive PMP22 would paradoxically cause CMT1A-like disease. The recovery from acute episodes is usually complete, though incomplete recovery can occur, resulting in mild disability in most cases[1][1][1]. Some affected individuals also demonstrate a mild-to-moderate peripheral neuropathy independent of acute compression episodes, and neuropathic pain is increasingly recognized as a common manifestation requiring targeted treatment approaches[1][6][1].

## Symptomatic Pharmacological Therapies: Current Standard of Care

### Neuropathic Pain Management with Gabapentinoids

Given the absence of disease-modifying treatments, the pharmacological management of HNPP currently focuses on symptomatic treatment of pain and functional impairment. Pregabalin, branded as Lyrica, is among the most commonly used medications to treat peripheral neuropathy and specifically HNPP[2][2]. Pregabalin belongs to the class of gabapentinoids and acts on voltage-dependent calcium channels in presynaptic neurons, reducing the release of excitatory neurotransmitters such as glutamate, thereby decreasing pain signal transmission in the central nervous system[12]. The mechanism of pregabalin involves selective binding to the α2δ-1 subunit of calcium channels in the central nervous system, which contributes to its analgesic effect and reduces the release of other neurotransmitters involved in pain processing[12].

Recent meta-analytic evidence demonstrates that pregabalin shows superior efficacy compared to gabapentin in managing neuropathic pain[12]. In a comprehensive meta-analysis of 14 studies with 3,346 patients, pregabalin demonstrated significantly superior results compared to gabapentin on the Visual Analog Scale (VAS) at various time intervals up to 12-14 weeks, with a standardized mean difference of −0.47 (95% CI −0.74 to −0.19)[12]. Beyond pain reduction, pregabalin improved patient-reported outcomes, resulted in lower opioid consumption, and led to fewer adverse events compared to gabapentin[12]. The superior efficacy of pregabalin may be attributed to its higher affinity for calcium channels, resulting in more potent inhibition of neurotransmitter release, and its selective binding to specific calcium channel subunits enhances its analgesic potential[12]. In clinical practice, therapeutic doses typically range from 300 to 600 mg daily, with higher doses demonstrating greater effectiveness in pain reduction[12].

### Gabapentin as an Alternative Analgesic

While pregabalin demonstrates superior efficacy, gabapentin remains widely used for neuropathic pain management in HNPP patients. Gabapentin also acts on voltage-dependent calcium channels through the α2δ subunit, though with lower affinity than pregabalin[12]. In studies evaluating gabapentin's efficacy for neuropathic pain, patients treated with variable success have been reported, with evidence suggesting gabapentin is more effective, particularly at higher doses[12]. Patients with HNPP have been treated with gabapentin for pain control, though the efficacy appears more variable compared to pregabalin, and individual response rates differ substantially among patients[6].

### Tricyclic Antidepressants and Serotonin-Norepinephrine Reuptake Inhibitors

Tricyclic antidepressants have demonstrated utility in managing neuropathic pain associated with HNPP. In a case series of four HNPP patients presenting with pain as a primary manifestation, patients were treated with variable success using tricyclic antidepressants, with some responding favorably to these agents[6]. The mechanism of action involves inhibition of monoamine reuptake, increasing serotonin and norepinephrine availability in the central nervous system, which modulates pain perception[6].

Venlafaxine, a serotonin-norepinephrine reuptake inhibitor (SNRI), is commonly used off-label for conditions including complex pain syndromes and diabetic neuropathy[15]. Venlafaxine increases serotonin and norepinephrine levels in the central nervous system by blocking transport proteins and inhibiting their reuptake at the presynaptic terminal, leading to greater neurotransmitter availability at the synapse[15]. Notably, the American Academy of Neurology endorses venlafaxine for diabetic neuropathy[15], and this mechanism-based indication may extend to management of neuropathic pain in HNPP, though specific HNPP trials are lacking. Venlafaxine demonstrates enhanced expression of brain-derived neurotrophic factors and promotes neuroplasticity, ultimately decreasing neuroinflammation[15]. In the case series of HNPP patients with pain presentations, venlafaxine was among the medications used with variable success[6].

### Tramadol and Opioid Medications

For HNPP patients with refractory neuropathic pain inadequately controlled by first-line agents, tramadol and opioid medications may be considered. In the aforementioned case series of four HNPP patients presenting with pain, two patients with refractory symptoms required the use of narcotics, specifically methadone and oxycodone[6]. Tramadol, a weak opioid agonist with monoamine reuptake inhibition properties, was also utilized in this patient population[6]. The use of opioid medications in HNPP represents a last-line approach when neuropathic pain proves resistant to gabapentinoids and other adjunctive therapies. The complications of chronic opioid use, including tolerance, dependence, and potential for overdose, necessitate careful patient selection and monitoring.

### Topical Anesthetic Agents

The FDA-approved 5% lidocaine patch (Lidoderm) has emerged as an option for localized neuropathic pain, including postherpetic neuralgia and potentially other neuropathic pain conditions[28]. Topical lidocaine dampens peripheral nociceptor sensitization and central nervous system hyperexcitability and may benefit patients with localized neuropathic pain[28]. In a meta-analysis of topical lidocaine for postherpetic neuralgia, topical lidocaine relieved pain better than placebo (P = 0.003)[28]. While the Lidoderm 5% patch is the only topical anesthetic agent that has received FDA approval for treatment of a neuropathic pain condition, its role in HNPP specifically remains under-researched, though individual HNPP patients have been treated with this agent with variable success[6][28]. The advantage of topical agents lies in minimal systemic absorption and reduced risk of drug-drug interactions, making them particularly suitable for patients with multiple comorbidities.

### Physical and Occupational Therapy

Treatment of HNPP manifestations involves occupational therapy and physical therapy as needed to address issues with fine motor and gross motor skills, including activities of daily living[1][1][30]. Physiotherapy along with occupational therapy should be started at the onset of symptoms, as early intervention can be beneficial for maintaining functional capacity[34]. Bracing, such as with a wrist splint or ankle-foot orthosis, may be useful transiently or in some instances permanently to prevent nerve compression[30][30]. Special shoes with good ankle support may be needed, and protective pads at elbows or knees can prevent pressure and trauma to local nerves[30]. These supportive interventions aim to prevent triggering episodes of acute neuropathy and maintain optimal function.

## Emerging Investigational Therapies: Targeting PMP22 Dosage

### High-Throughput Screening and Small Molecule Discovery

A breakthrough in HNPP therapeutic development has emerged from high-throughput screening campaigns targeting PMP22 protein expression. Researchers at Vanderbilt University, in a cutting-edge drug discovery project funded by the Charcot-Marie-Tooth Research Foundation, screened a library of over 20,000 small molecules and identified several that showed promising activity warranting further investigation[4][4][4]. The researchers recognized that the fundamental problem in HNPP differs from CMT1A: while CMT1A results from PMP22 overproduction, HNPP results from underproduction of PMP22. Thus, identifying drugs that can restore PMP22 levels and function to healthy levels serves as a major therapeutic goal[4][4][4].

In the second year of this project, researchers conducted additional evaluations to further assess the potency and efficacy of these compounds in Schwann cells and developed and tested new versions to identify additional candidate compounds with greater effects on altering PMP22 production or trafficking[4][4][4]. These studies revealed three candidate compounds that significantly altered PMP22 levels. Of these three, two reduce PMP22 levels, highlighting their potential for CMT1A and CMT1E where PMP22 is overproduced or forms intracellular aggregates. Crucially for HNPP patients, **the third candidate increases PMP22 levels, making it a possible solution for HNPP, where PMP22 is underproduced**[4][4][4]. Importantly, all three candidate compounds demonstrated no signs of Schwann cell toxicity, making them strong prospects for further testing, which will include testing their ability to correct disease symptoms in tissue and mouse models[4][4][4].

Interestingly, the researchers identified that all three molecules do not directly bind to PMP22, meaning they regulate PMP22 levels indirectly, likely by interacting with another target that influences PMP22 expression[4][4][4]. This indirect mechanism suggests the compounds may avoid some of the off-target effects associated with direct PMP22 inhibition or activation. As the project moves forward, the team continues to study the molecular mechanisms behind these compounds, with plans to optimize their effectiveness and assess their ability to promote nerve cell myelination[4][4][4]. Additional screening efforts are also underway to discover more potential candidate compounds. This represents one of the most promising near-term approaches to developing disease-modifying therapy for HNPP.

### PAK1 Inhibition: A Novel Pathway Intervention

Recent mechanistic studies have identified a molecular pathway upstream of segmental demyelination in HNPP that offers therapeutic potential. Using an HNPP mouse model (Pmp22+/-), researchers identified a robust increase of F-actin in nerve regions where myelin junctions were disrupted, leading to increased myelin permeability[32]. These abnormalities were present long before segmental demyelination at the late phase of Pmp22+/- mice, suggesting that addressing junction disruption early in disease progression could prevent later pathological changes[32]. The increase of F-actin levels correlated with enhanced activity of p21-activated kinase 1 (PAK1), a molecule known to regulate actin polymerization[32].

The therapeutic significance of this finding became apparent when researchers treated HNPP mice with a PAK1 inhibitor. **This treatment completely prevented the progression of nerve conduction failure and HNPP pathology**[32]. This work offers a promising therapeutic approach for HNPP distinct from approaches targeting PMP22 levels directly. The discovery that myelin junction disruption takes place long before segmental demyelination provides a mechanism upstream to segmental demyelination, a pathological process relevant to many demyelinating diseases[32]. This finding suggests that therapeutic intervention targeting PAK1 activity early in disease progression could arrest the cascade leading to functional nerve conduction failure. Preclinical development of PAK1 inhibitors for HNPP represents an active area of investigation, though no clinical trials in HNPP have been initiated as of April 2026.

### YAP/TAZ Modulation: Targeting Schwann Cell Biology

Another emerging therapeutic approach involves modulation of the Hippo signaling pathway in Schwann cells. Recent research demonstrated that Yes-associated protein 1 (YAP) and transcriptional coactivator with PDZ-binding motif (TAZ) associate with TEAD1 transcription factors and positively regulate PMP22 expression through enhancer binding, making YAP and TAZ intriguing therapeutic targets to alter the expression of PMP22[33]. In a landmark study, researchers investigated novel targets for modulating PMP22 protein levels in HNPP by examining YAP activity. They found that genetic attenuation of the transcriptional coactivator YAP in Schwann cells reduced p-TAZ levels, increased TAZ activity, and increased PMP22 in peripheral nerves[33]. 

To test this therapeutic hypothesis, researchers ablated YAP alleles in Schwann cells of the Pmp22-haploinsufficient mouse model of HNPP and identified fewer tomacula (focal regions of hypermyelination characteristic of HNPP) on morphological assessment and improved nerve conduction in peripheral nerves[33]. While the decrease in the number of tomacula did not correspond to improvement of motor nerve conduction deficits in PMP22+/- mice with ablation of one or both YAP alleles when compared to PMP22+/- littermates at early timepoints, ablation of a single YAP allele partially rescued motor nerve conduction in PMP22+/- mice at 60 days of age[33]. Overall, the findings suggest that modulation of YAP activity may be beneficial to HNPP pathophysiology[33], representing a distinct therapeutic avenue potentially suitable for drug development. The identification that YAP effects are allele dosage-dependent and disease-context dependent indicates that therapeutic YAP inhibition must be carefully titrated to achieve maximal benefit without adverse effects.

### Gene Therapy Approaches

Gene-based therapies aim to address the primary genetic cause of HNPP by restoring gene function through viral vectors and other delivery mechanisms. In demyelinating HNPP caused by PMP22 deletion, several strategies aim to increase production of the deficient PMP22 protein. Loss-of-function demyelinating neuropathies such as HNPP are being addressed by Schwann-cell-targeted adeno-associated virus (AAV) mediated gene replacement, restoring myelination and nerve function in preclinical models[8]. One approach involves introduction of another copy of the PMP22 gene into the peripheral nerve by gene therapy[27]. An obstacle in this strategy is that increasing the dosage of PMP22 above a certain level will cause CMT1A-like disease, necessitating tightly controlled gene expression levels[27]. 

Several preclinical strategies have been explored, including AAV-mediated gene therapy delivering functional PMP22 to Schwann cells. These approaches face challenges related to achieving adequate transduction efficiency, ensuring tissue-specific delivery to peripheral nerves, and maintaining therapeutic gene expression levels within the narrow range necessary to avoid inducing CMT1A phenotype. The timing of intervention—during developmental myelination versus in mature Schwann cells—remains unclear, as does whether the myelin abnormalities or conduction block in HNPP arises from PMP22 deficiency during myelin development or is dependent upon ongoing depletion of PMP22 in mature Schwann cells[9]. Despite these challenges, gene therapy remains a promising long-term approach to addressing the root cause of HNPP.

## Drug Repurposing Candidates: Off-Label Applications

### Corticosteroids for Protracted Neurological Episodes

While not standard therapy, corticosteroid administration has demonstrated benefit in individual HNPP patients with protracted or incomplete recovery from acute episodes. Management of hereditary neuropathy with liability to pressure palsy is primarily conservative, aimed at preventing nerve injury by avoiding trauma or other potential aggravating factors[10][31]. No pharmacological treatment has been traditionally known to be beneficial. However, clinical case reports describe two adolescents, one with genetically confirmed HNPP and another with clinical picture suggestive of HNPP, who showed considerable improvement of their symptoms after receiving corticosteroid therapy[10][31]. Both individuals were symptomatic for at least five months before treatment, and following corticosteroids, both demonstrated rapid improvement leading to near-complete recovery of muscle power[10][31].

These observations suggest that corticosteroid therapy may be beneficial in individuals with HNPP who have a protracted or incomplete course of recovery, representing a departure from standard management algorithms[10][31]. Clinical improvement after corticosteroid therapy has been reported in some individuals with other hereditary neuropathies, supporting the biological plausibility of anti-inflammatory mechanisms providing benefit in genetically determined neuropathies[10][31]. However, the mechanism underlying potential corticosteroid benefit in HNPP remains unclear, and no randomized trials have been conducted. The use of corticosteroids must be weighed against potential adverse effects, particularly with prolonged use. Current evidence suggests corticosteroids might be considered in severe cases with incomplete spontaneous recovery, though they remain an off-label, investigational approach lacking robust clinical trial evidence.

### Vitamin C (Ascorbic Acid): Limited Evidence and Theoretical Concerns

Vitamin C (ascorbic acid) represents a unique case of a repurposing candidate with contradictory evidence and theoretical concerns specific to HNPP pathophysiology. There is little research showing specific effects of vitamin C on HNPP, though studies have revealed effects on the inherited condition Charcot-Marie-Tooth disease type 1A (CMT1A)[2][2]. In a 2004 study titled "Ascorbic Acid Treatment Corrects the Phenotype of a Mouse Model of Charcot-Marie-Tooth Disease," researchers claimed to see improvement from ascorbic acid, reporting that "Ascorbic acid treatment resulted in substantial amelioration of the CMT-1A phenotype, and reduced the expression of PMP22 to a level below what is necessary to induce the disease phenotype"[2][2]. The authors suggested that "As ascorbic acid has already been approved by the FDA for other clinical indications, it offers an immediate therapeutic possibility for patients with the disease"[2][2].

However, for HNPP patients, ascorbic acid presents a paradox. While high-dose vitamin C reduces PMP22 production (beneficial in CMT1A with PMP22 overproduction), this mechanism would likely have a reverse or harmful effect for those with HNPP, which involves haploinsufficiency with already reduced PMP22 gene dosage[2][2]. Furthermore, a later study concluded that "Findings of this study suggest that ascorbic acid is not efficacious in adults with CMT1A"[2][2]. Given the opposite dosage problem in HNPP compared to CMT1A, ascorbic acid supplementation offers no clear therapeutic rationale and could theoretically worsen HNPP pathophysiology by further reducing already-deficient PMP22 levels. As stated authoritatively in the literature, Thomas Bird, MD, writing for GeneReviews, emphasizes that "No specific treatment for the underlying genetic or biochemical defect exists and no special diet or vitamin regimen is known to alter the natural course of HNPP"[2][2]. The Charcot-Marie-Tooth Association advises that HNPP patients avoid consuming high doses of vitamin C, while noting no problems with regular doses (75-90 mg daily)[25].

## Contraindicated Drugs and Neurotoxic Agents

### Vincristine: Explicit Contraindication and Severe Neurotoxicity

The chemotherapy agent vincristine represents a critical contraindication in HNPP, particularly in demyelinating forms of CMT. The cancer drug vincristine, a known chemotherapy drug used to treat several types of cancer including acute leukemia, malignant lymphomas, and carcinomas, is contraindicated in HNPP[2][2]. The drug manufacturer Pfizer explicitly warns that Vincristine Sulfate injections should not be given to individuals with "the demyelinating form of Charcot-Marie-Tooth disease"[13][13]. Research demonstrates that Vincristine's neurotoxic effects can exacerbate nerve damage, potentially leading to severe and sometimes irreversible worsening of HNPP symptoms[13][13][25].

A landmark scientific reevaluation of neurotoxic drugs, conducted in 2022 by the Charcot-Marie-Tooth Association and published in the Journal of the Peripheral Nervous System, identified Vincristine as one of only two drugs with evidence-backed potential for significant harm in CMT/HNPP[13][13]. The potential risks of concern with Vincristine include research showing that Vincristine's neurotoxic effects can exacerbate nerve damage, leading to severe and sometimes irreversible worsening of CMT symptoms, with particular concern in the demyelinating forms[13][13]. Individuals with HNPP who develop malignancies requiring chemotherapy must work closely with both their oncologist and neurologist to identify alternative agents lacking neurotoxic properties toward peripheral nerves.

### Paclitaxel: Significant Neurotoxic Risk

Paclitaxel (Taxol), a widely used chemotherapy medication for treatment of multiple cancer types including ovarian, breast, lung, cervical, and pancreatic cancers, represents another critical contraindication in HNPP[2][2][13][13]. The Charcot-Marie-Tooth Association identified Paclitaxel as having known association with neurotoxicity and being of special concern to individuals with HNPP and their healthcare providers[13][13]. Drugs and medications such as Taxol that are known to cause nerve damage should be avoided in HNPP patients, according to findings by Vinay Chaudry, MD, in a 2003 study titled "Toxic Neuropathy in Patients With Pre-Existing Neuropathy"[2][2]. In this study, six patients with pre-existing neuropathy who received "non-toxic" dosages of known neurotoxic agents including Taxol had significantly worsened neuropathy[2][2]. Chaudry concluded that "functionally disabling toxic neuropathy can occur in patients with pre-existing neuropathy at standard doses"[2][2].

A 2017 study highlighted the potential for Paclitaxel to worsen peripheral neuropathy specifically in patients with CMT caused by mutations of the MFN2 gene (CMT2A, CMT2A2B, CMT2B4, and HMSN-6A), ARHGEF10 gene (CMT-ARHGEF10), and the PRX gene (CMT4F)[13][13]. The research suggests that Paclitaxel's neurotoxic effects can exacerbate nerve damage, leading to severe and sometimes irreversible worsening of symptoms[13][13]. The Charcot-Marie-Tooth Association has maintained a "Medical Alert" list of potentially neurotoxic medications, defining Taxol as a "definite high risk" to those with HNPP even if the individual may not present any symptoms[2][2]. As with Vincristine, the identification of Paclitaxel as a high-risk agent reflects findings from the 2022 systematic review published in the Journal of the Peripheral Nervous System[13][13].

### Fluoroquinolone Antibiotics: FDA Safety Warning

Fluoroquinolone antibiotics, a family of broad-spectrum systemic antibacterial agents widely used for treatment of respiratory and urinary tract infections, represent a significant risk in HNPP patients[2][2][26]. However, in 2016, the U.S. Food and Drug Administration advised that the serious side effects associated with fluoroquinolone antibacterial drugs generally outweigh the benefits for patients with neuropathic issues[2][2]. In a statement, the FDA declared: "An FDA safety review has shown that fluoroquinolones when used systemically (i.e. tablets, capsules, and injectable) are associated with disabling and potentially permanent serious side effects that can occur together. These side effects can involve the tendons, muscles, joints, nerves, and central nervous system"[2][2].

The neurotoxic effects of quinolones encompass antibiotic-associated encephalopathy, seizures, peripheral neuropathy, and exacerbation of myasthenia gravis[26]. Research indicates that quinolones exhibit both central and peripheral neurotoxicity[26]. Peripheral neuropathy is specifically linked to systemic exposure to quinolones. In a study by Morales et al. following 5,357 patients with incident peripheral neuropathy matched to 17,285 controls, those taking oral fluoroquinolones had significantly increased risks, with risks rising by 3% for each additional day of current exposure and persisting for up to 180 days after exposure[26]. The peripheral neuropathy reported with fluoroquinolone administration can be severe, debilitating, and permanent[2][2]. For this reason, physicians need to practice due diligence when prescribing not only antibiotics but any drug to HNPP patients[2][2]. The FDA safety review showing that fluoroquinolones when used systemically are associated with disabling and potentially permanent serious side effects that can occur together in multiple organ systems represents a compelling contraindication in HNPP patients[2][2][26].

### Other Neurotoxic Medications and Drug Interactions

While Vincristine and Paclitaxel represent the most serious contraindications with evidence-backed neurotoxic potential, other medications warrant caution in HNPP. The revised 2023 Charcot-Marie-Tooth Association neurotoxic drug list retained only Vincristine and Paclitaxel as having evidence-backed potential for harm[13][13]. However, general principles suggest HNPP patients should be carefully monitored for side effects when receiving any new medications. Severe side effects have been reported in patients with CMT1A who took Vincristine and developed limb paralysis[25]. While this specific evidence comes from CMT1A rather than HNPP, an HNPP animal model shows slower recovery from nerve damage, suggesting HNPP patients may be similarly or even more vulnerable to neurotoxic medication effects[25].

Additional medications and drug classes warrant consideration. Lithium, used in psychiatric treatment, has been associated with peripheral neuropathy secondary to lithium use even at therapeutic serum levels[22]. Most cases of lithium-induced neuropathy occur in association with lithium toxicity, but very few cases of peripheral nervous system damage secondary to lithium with normal serum lithium levels have been reported[22]. The potential mechanism of lithium-induced neuropathy involves intracellular accumulation of lithium and interference with the propagation of action potentials[22]. Systematic clinical search for symptoms and signs as well as electrophysiological search for subclinical neuropathy in HNPP patients on chronic lithium therapy would be prudent[22].

## Combination Therapy Approaches

### Rationale for Combination Therapy in Neuropathic Pain Management

Combination therapy approaches have demonstrated efficacy in managing neuropathic pain across various conditions and offer potential advantages for HNPP pain management. When combining gabapentinoids with opioids, the combination has been found to improve neuropathic pain in both cancer and non-cancer patients[12]. However, when combining gabapentin with opioids, dose adjustments of gabapentin may be necessary due to delayed renal elimination[12]. Furthermore, combining tricyclic antidepressants or gabapentinoids with opioids has demonstrated improved neuropathic pain control[12]. Pregabalin has shown to reduce opioid doses and the adverse effects associated with opioid use[12], suggesting that gabapentinoid-based combination regimens may reduce overall opioid requirements in HNPP patients with severe pain.

Beyond pain management, the concept of combination therapy extends to disease-modifying approaches. In CMT1A research, multiple therapeutic approaches have been explored in combination. For example, PXT3003, an investigational oral combination therapy designed to downregulate PMP22 expression, functions as a fixed-dose combination of baclofen, naltrexone, and sorbitol[11][19]. While PXT3003 is targeted toward CMT1A with PMP22 overproduction rather than HNPP with PMP22 underproduction, this represents an example of rationally designed combination therapy addressing multiple molecular pathways. The rationale for combination drug therapy often involves targeting parallel pathways or utilizing synergistic mechanisms to enhance therapeutic efficacy while potentially reducing required doses of individual agents.

### Pharmacological Synergies in Pain Management

Specific combination strategies show promise for HNPP pain management based on mechanistic considerations. The combination of a gabapentinoid (pregabalin or gabapentin) with a tricyclic antidepressant or SNRI addresses neuropathic pain through multiple mechanisms: gabapentinoids reduce excitatory neurotransmitter release via calcium channel modulation, while tricyclics and SNRIs enhance monoamine availability, with complementary effects on pain processing[12][15]. Clinical experience in HNPP patients demonstrates variable success with this combination approach, and systematic optimization of combination regimens remains an area for future clinical investigation[6].

For patients with inadequate response to first-line agents, combination therapy with topical lidocaine patches applied to areas of localized neuropathic pain may provide additive benefit through local anesthetic effects complementing systemic agents. The non-systemic nature of topical lidocaine minimizes drug-drug interactions and cumulative toxicity risks. Sequential or concurrent use of multiple pain management modalities—including physical therapy, occupational therapy, bracing strategies, and pharmacological agents—represents the current standard of care approach to HNPP management, though prospective studies evaluating specific combination regimens remain lacking.

## Diagnostic and Monitoring Considerations

### Electrophysiological Characteristics in Diagnosis

Accurate diagnosis of HNPP through electromyography (EMG) and nerve conduction studies is essential for appropriate treatment planning and prognosis. Among symptomatic patients, clinical manifestations include transient neurological symptoms triggered by traction or compression (57.8%), temperature changes (3.4%), or unclear causes (38.8%)[3]. EMG findings in HNPP revealed distinctive background polyneuropathy independent of superimposed entrapment neuropathies[3]. Among patients, 40.2% had prolonged distal sensory latency (DSL), 67% had decreased sensory nerve action potentials (SNAP), and 75.3% had reduced sensory nerve conduction velocity (SNCV), supporting the EMG characteristics of HNPP[3]. These electrophysiological findings provide objective markers for disease presence and severity that can guide therapeutic decisions and monitor response to emerging disease-modifying therapies as they become available.

### Genetic Testing for Disease Confirmation

The diagnosis of HNPP is established via genetic testing identifying the pathogenic variant in the PMP22 gene. Heterozygous PMP22 deletions and other PMP22 gene mutations are found in 77.4% and 22.6% of cases, respectively[3]. Single-nucleotide deletions and microdeletions within PMP22 represent 0.81% of cases each[3]. The increased diagnosis rate of HNPP is primarily based on recurrent single or multiple nerve injury in clinical practice, positive family history, EMG findings showing neurological changes beyond the lesion nerve involving a wider range, and genetic testing showing PMP22 heterozygosity deletion[3]. Genetic confirmation is particularly important because approximately 10-15% of patients with HNPP may have no obvious symptoms and are only diagnosed through family members' onset, medical treatment, and family genetic testing[3].

## Adverse Events and Disease Progression

### Clinical Outcomes and Spontaneous Recovery Patterns

Understanding the natural history and expected recovery patterns of HNPP is essential for distinguishing disease progression from treatment adverse effects. Full recovery over a period of days to months occurs in approximately 50% of episodes[1]. While incomplete recovery is common and often associated with frustration, when recovery is not complete, the resulting disability is typically mild[1][1][1][1]. Some affected individuals demonstrate a mild-to-moderate peripheral neuropathy independent of acute compression episodes. Among literature review findings, 17.7% of patients have signs of muscle atrophy of varying degrees, and 12.1% have characteristics of pes cavus (high arches), findings that must be differentiated from CMT1A[3]. Nine percent of patients show no neurologic symptoms, highlighting the variable penetrance and expressivity of HNPP[3].

Neuropathic pain associated with HNPP may present as a combination of true neuropathic pain (sharp, burning, tingling, highly sensitive to touch) and musculoskeletal pain. In a study examining pain as a presentation of HNPP, four HNPP patients presented with pain at clinical evaluation[6]. Three patients reported aching pain of muscles and joints diffusely in addition to transient paresthesias and dysesthesias in the arms and legs[6]. One of these patients had previously been diagnosed with fibromyalgia approximately 1.5 years prior to presentation, and two other patients were ultimately found to meet clinical criteria for fibromyalgia[6]. A fourth patient had prominent diffuse myalgic pain in the arms in addition to persistent paresthesias and dysesthesias in the hands[6]. This presentation pattern suggests that myalgic pain in HNPP patients may result from hypersensitivity of muscle nociceptors due to the underlying neuropathic condition and may precede classic transient paresthesias or weakness by several years[6].

### Management of Acute Episodes

Avoiding physical triggers represents the cornerstone of HNPP management to prevent episodes. HNPP patients are advised to avoid physical activities including compression (by sitting with legs crossed and putting pressure on the peroneal nerve or leaning on elbows against the ulnar nerve), prolonged stereotypic movements, and over-stretching of arms or legs[25]. However, a sedentary lifestyle is not advocated because it may lead to obesity and metabolic problems, necessitating that activities be tailored for individuals to have adequate exercise without triggering nerve symptoms[25]. Recovery time from episodes varies considerably, taking anywhere from hours to months to recover from an episode[25]. While most episodes are transient, some patients may experience permanent weakness[25].

Episodes may not have identifiable triggers in 38.8% of cases[3], presenting a clinical challenge for patient counseling and prevention strategies. Some patients may be asymptomatic, and HNPP may lead to severe limb paralysis when asymptomatic patients are challenged by strenuous physical activities such as running 10 miles daily with a 50-pound backpack[25]. An instructive case describes an asymptomatic woman who developed leg paralysis after prolonged labor lasting nine hours while sitting in birthing position[25]. These possible outcomes impose catastrophic risk in the fraction of patients with undiagnosed asymptomatic HNPP, emphasizing the importance of family screening and genetic counseling.

## Regulatory Status and Clinical Trial Landscape

### Current Clinical Trial Activity

As of April 2026, no randomized controlled trials specifically evaluating pharmacological interventions for HNPP have reached late-stage development or regulatory approval. The clinical trial landscape reflects the status of basic and preclinical research on HNPP-directed therapeutics. Preclinical studies with Vanderbilt's PMP22-targeting small molecules represent the most advanced investigational approach specific to HNPP mechanism of action, though these compounds remain in the discovery and preclinical development phase without announced clinical trial initiation.

Research efforts by the Charcot-Marie-Tooth Research Foundation have supported multiple investigational programs and managed 17 preclinical studies with biotech partners[29]. The foundation approved 10 new research awards to study gene therapy to restore nerve function, hearing loss and balance in CMT, repurposing of approved drugs to evaluate potential HNPP benefit, and improving drug delivery so treatments can reach nerve cells more effectively[29]. These initiatives reflect the commitment to developing targeted therapies for rare CMT subtypes including HNPP.

### Comparative Treatment Landscape with CMT1A

While HNPP remains without approved disease-modifying therapy, the related condition CMT1A has progressed further in therapeutic development. PXT3003, an oral combination therapy showing positive results both in non-clinical pharmacology and clinical studies for treatment of CMT1A, received positive topline results from Phase III trials[11][19]. In October 2018, PXT3003 completed an international Phase III trial in 323 patients 16 years and older for treatment of CMT1A, confirming excellent safety profile and demonstrating an encouraging efficacy profile[11]. The Phase III extension study continues, with the drug showing trends in multiple efficacy endpoints beyond stabilization, particularly the Overall Neuropathy Limitations Scale (ONLS)[11]. PXT3003 has been granted Orphan Drug Designation in both the United States and Europe and received Fast Track Designation from the FDA[11]. In March 2020, the United Kingdom's Medicines and Healthcare Products Regulatory Agency granted Promising Innovative Medicine (PIM) designation to PXT3003 for treatment of CMT1A[11].

While PXT3003 targets CMT1A with PMP22 overexpression, the approaches used in its development—modulating PMP22 expression through multiple mechanisms including cAMP pathway modulation and Pmp22 protein folding—may provide insights applicable to HNPP drug development. However, the inverse dosage problem in HNPP means that CMT1A therapeutic strategies cannot be directly translated and must be substantially modified.

### Other CMT Gene Therapies in Development

Several gene therapy approaches for CMT are in development that may eventually inform HNPP strategies. DTx Pharma's DTx-1252, acquired by Novartis in 2023, targets CMT1A through siRNA therapy designed to silence PMP22 overexpression[17]. This program demonstrates that gene therapy approaches targeting PMP22 are advancing toward clinical development, though DTx-1252 specifically targets CMT1A rather than HNPP. Applied Therapeutics' INSPIRE trial evaluates govorestat (AT-007), an aldose reductase inhibitor for SORD deficiency-related CMT, representing a distinct genetic mechanism but demonstrating the broader progress in bringing CMT therapeutics to clinical trials[18].

## Conclusion and Future Directions

Hereditary neuropathy with liability to pressure palsies remains a serious therapeutic challenge for which no disease-modifying treatments have yet been approved by major regulatory agencies. Current clinical management relies on symptomatic treatment of neuropathic pain using gabapentinoids, tricyclic antidepressants, SNRIs, and in refractory cases, opioid analgesics, along with supportive strategies including physical therapy, occupational therapy, orthotic devices, and crucially, avoidance of physical triggers that precipitate acute episodes. The identification of neuropathic pain as an increasingly recognized manifestation of HNPP underscores the importance of comprehensive pain management strategies tailored to individual patient presentations.

Significant therapeutic progress is anticipated in the near term, driven primarily by small molecule drug discovery campaigns identifying compounds that can selectively increase PMP22 expression in Schwann cells. The identification of three lead compounds from a screen of over 20,000 molecules represents genuine progress toward disease-modifying therapy. Parallel research on PAK1 inhibition and YAP/TAZ modulation in Schwann cells has identified upstream pathological mechanisms that may be amenable to pharmacological intervention. Gene therapy approaches using AAV vectors to deliver functional PMP22 to peripheral nerves remain in preclinical development but offer long-term therapeutic potential, particularly if delivery, transduction efficiency, and durability challenges can be addressed.

Critical contraindications must be rigorously communicated to all healthcare providers caring for HNPP patients. Vincristine and Paclitaxel represent severe neurotoxic risks in this population, with evidence demonstrating potential for permanent, irreversible worsening of neuropathic symptoms. Fluoroquinolone antibiotics pose significant risks of permanent peripheral neuropathy and should be avoided when alternative agents exist. These contraindications underscore the necessity of HNPP awareness among all medical specialties, particularly oncologists and infectious disease specialists who may prescribe these agents without awareness of their particular risks in this patient population.

The path from laboratory discovery to clinical reality for HNPP therapeutics requires several critical steps, including optimization of lead compounds for bioavailability and tissue-specific delivery, comprehensive preclinical safety assessments, investigational new drug applications, and ultimately rigorous clinical trials demonstrating efficacy and safety. The participation of HNPP patients in clinical research through registries such as the Global Registry for Inherited Neuropathies will be essential for advancing the field. With sustained research investment and the promising preclinical developments documented in this report, genuine therapeutic progress for HNPP appears achievable within the next several years, offering hope to the estimated three million individuals affected by Charcot-Marie-Tooth diseases worldwide and the fraction of that population suffering from HNPP specifically.