---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:24:05.544245'
end_time: '2026-04-04T14:26:05.175280'
duration_seconds: 119.63
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: Ehlers-Danlos syndrome, hypermobility type
  mondo_id: MONDO:0007523
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
- **Disease Name:** Ehlers-Danlos syndrome, hypermobility type
- **MONDO ID:** MONDO:0007523

## Research Objectives

Please provide a comprehensive research report on **drug therapies for Ehlers-Danlos syndrome, hypermobility type**.
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

# Comprehensive Drug Therapies for Hypermobile Ehlers-Danlos Syndrome: A Current State of Evidence Report

This report synthesizes current evidence on pharmaceutical interventions for hypermobile Ehlers-Danlos syndrome (hEDS), the most common subtype of Ehlers-Danlos syndromes. Despite the lack of disease-modifying or curative treatments, a diverse pharmacological toolkit has emerged through symptomatic management, off-label repurposing of approved drugs, and emerging investigational agents targeting pathophysiological mechanisms. The therapeutic landscape reflects the multisystemic nature of hEDS, requiring individualized approaches addressing chronic pain, joint hypermobility complications, dysautonomia, mast cell activation, and gastrointestinal dysfunction. Recent clinical data demonstrate that exercise-based interventions combined with targeted pharmacotherapy, particularly nonsteroidal anti-inflammatory drugs (NSAIDs), muscle relaxants, and carefully titrated low-dose naltrexone, provide meaningful symptom relief for many patients, though significant variability in treatment response necessitates personalized dosing strategies and careful monitoring for paradoxical adverse effects.

## Approved Drug Therapies for Hypermobile Ehlers-Danlos Syndrome

Currently, there are no disease-modifying drugs specifically approved by major regulatory agencies for hEDS management, though several medications are established for symptom control through off-label use. This represents both a clinical challenge and an opportunity for therapeutic innovation, as treatment remains predominantly focused on managing the constellation of symptoms arising from connective tissue dysfunction rather than addressing the underlying genetic etiology. The absence of a disease-modifying agent for hEDS contrasts with the recent breakthrough designation of celiprolol (a third-generation beta-blocker) for vascular EDS, which demonstrates the evolving regulatory landscape for rare connective tissue disorders.

### Symptomatic Pain Management: Nonsteroidal Anti-Inflammatory Drugs and Acetaminophen

Nonsteroidal anti-inflammatory drugs represent the mainstay of analgesic therapy for hEDS-related pain[1]. In a comprehensive clinical survey, 45.6 percent of hEDS patients reported using NSAIDs for pain management, with approximately 41 patients in the hypermobile cohort self-reporting that NSAIDs improved their symptoms[4]. The mechanism of NSAIDs in hEDS pain management operates through cyclooxygenase inhibition, reducing prostaglandin-mediated inflammatory signaling at peripheral and central sites. Common agents include ibuprofen, naproxen sodium, and indomethacin, though the evidence base consists primarily of clinical experience rather than randomized controlled trials specific to hEDS populations[1][3]. However, clinicians must exercise caution with chronic NSAID use in hEDS patients, particularly those with concomitant mast cell activation syndrome (MCAS), as these agents can trigger histamine release from mast cells and exacerbate systemic symptoms[10]. Acetaminophen, another first-line analgesic, was utilized by 35.6 percent of hEDS patients in reported studies, with similar efficacy profiles for mild to moderate pain[4].

### Muscle Relaxants and Spasticity Management

Muscle relaxants demonstrate clinical utility in managing the myofascial pain and muscle spasms associated with hEDS, though the evidence supporting their use derives largely from patient-reported outcomes and clinical experience rather than rigorous controlled trials. Among hEDS patients, 15.6 percent reported using muscle relaxants, and this class of medications showed statistically significant differences in prevalence between hEDS and hypermobility spectrum disorder (HSD) populations, with HSD patients more likely to report muscle relaxant use at 28.5 percent[4]. The mechanisms of action vary within this class, including centrally acting agents that modulate neurotransmission and peripheral agents affecting muscle contractility. Benzodiazepines such as diazepam represent one category, though their use requires careful monitoring given potential for dependence and the overlap of hEDS with anxiety disorders requiring alternative anxiolytic management strategies[7]. Other muscle relaxants including cyclobenzaprine and tizanidine offer alternative mechanisms that may be better tolerated by some patients.

## Investigational Drugs and Active Clinical Trials

### Low-Dose Naltrexone: Emerging Evidence for Chronic Pain

Low-dose naltrexone (LDN), typically defined as doses ranging from 0.1 to 6.0 mg/day—substantially below the standard 50 mg opioid reversal dosing—has emerged as a promising investigational agent for hEDS-related chronic pain based on growing clinical experience and patient reports. This medication operates through a distinct mechanism compared to standard analgesics, functioning as a partial opioid receptor antagonist at low doses, which paradoxically enhances endogenous opioid signaling through disinhibition mechanisms and modulates microglial activation and neuroinflammation[3][7][9]. A recent observational study published in 2024 specifically examined effective dosing of LDN for chronic pain and found remarkable individual variability, with optimal doses ranging from 0.1 to 6.0 mg/day[9]. The study emphasized that "the optimal dose can vary significantly, indicating a personalized approach to treatment might be necessary," suggesting that clinicians should consider individualized titration protocols rather than standardized dosing schedules[9]. For hEDS patients specifically, LDN has been reported as helpful for managing chronic pain in multiple clinical reports and patient surveys, with particular benefit noted for neuropathic pain components and diffuse musculoskeletal pain[3][7].

The mechanisms underlying LDN's potential benefit in hEDS extend beyond simple analgesia. At low doses, naltrexone blocks inhibitory opioid receptors on immune cells, potentially enhancing toll-like receptor 4 signaling and producing anti-inflammatory effects through reduced production of pro-inflammatory cytokines including interleukin-6 and tumor necrosis factor-alpha. This immunomodulatory action may be particularly relevant to hEDS patients with concurrent mast cell activation or other immune dysregulation. However, the evidence base for LDN in hEDS remains limited to case reports, observational studies, and patient experiences, with no randomized controlled trials specifically powered to evaluate its efficacy in this population. The lack of pharmacokinetic and pharmacodynamic studies in hEDS patients represents a significant research gap.

### Integrative Medicine and Dietary Interventions

An active clinical trial (NCT04734041) is currently assessing the feasibility and preliminary efficacy of integrative medical care combining an anti-inflammatory Mediterranean-style diet with behavioral and psychosocial support for patients with hEDS and hypermobility spectrum disorder[2]. This nine-week intervention was designed to recruit 20 patients and evaluate whether dietary modification alongside psychological support could reduce pain and improve quality of life. The trial employed validated outcome measures including the Visual Analog Scale for Pain (VAS), the Patient-Reported Outcomes Measurement Information System (PROMIS-29) pain intensity scale, and adherence metrics tracked through food tracking applications[2]. The Mediterranean diet intervention was selected based on mechanistic rationale that anti-inflammatory fatty acids and polyphenol-rich foods might mitigate the inflammatory complications of connective tissue dysfunction and associated mast cell activation. This trial represents an important step toward understanding whether nutritional modification can serve as an adjunctive therapeutic strategy in hEDS management.

### Prolotherapy: Regenerative Medicine Approach

Ultrasound-guided dextrose prolotherapy represents an investigational regenerative medicine approach currently being evaluated for chronic pain associated with hEDS (NCT05279937)[8]. Prolotherapy operates through injection of hypertonic dextrose solutions intended to stimulate localized inflammatory responses and theoretically promote healing and connective tissue strengthening through proliferation of fibroblasts and collagen deposition. While prolotherapy has shown promise in relieving pain and potentially strengthening ligaments in some hEDS populations, it remains in early investigational stages with limited controlled trial data[7]. The mechanistic rationale for prolotherapy in hEDS differs from its application in other joint disorders, given that the underlying pathology involves inherent collagen abnormalities rather than simple ligamentous laxity from overuse or trauma. This raises important questions about whether stimulating additional collagen deposition in structurally abnormal connective tissue could produce desired therapeutic effects or potentially exacerbate fibrotic complications.

## Drug Repurposing Candidates for Hypermobile Ehlers-Danlos Syndrome

### Beta-Blockers and Cardiovascular Agents for Dysautonomia

Beta-adrenergic antagonists, particularly propranolol and bisoprolol, represent established off-label treatments for the dysautonomic complications of hEDS, particularly postural orthostatic tachycardia syndrome (POTS), which occurs in a substantial proportion of hEDS patients[7][24]. These medications function by blocking beta-1 adrenergic receptors on cardiac tissue, reducing heart rate and cardiac contractility, thereby decreasing the exaggerated tachycardic response upon postural change that characterizes POTS. The typical dosing regimen involves propranolol starting at 10-20 mg three times daily and titrating upward based on symptomatic response and heart rate reduction. Metoprolol and other beta-blockers have similar mechanisms and are employed interchangeably based on patient tolerance and comorbidities[7][24].

More recently, ivabradine (Corlanor), a selective inhibitor of the pacemaker current (If current) in the sinoatrial node, has emerged as an alternative to beta-blockers for POTS management, particularly in patients who cannot tolerate or do not respond to traditional beta-blockers[7][24]. Ivabradine offers a distinct mechanism by selectively reducing heart rate without affecting blood pressure or myocardial contractility, distinguishing it from non-selective beta-blockers. Clinical reports indicate that approximately 60 percent of patients treated with ivabradine report symptomatic improvement[24]. Initial dosing is typically 5 mg twice daily, with titration to a maximum of 7.5 mg twice daily based on response. The advantage of ivabradine in hEDS patients with POTS includes its lack of negative inotropic effects and potentially better tolerability in patients with concurrent asthma or reactive airway disease who might be contraindicated from beta-blockers.

Mineralocorticoid receptor agonists, particularly fludrocortisone (Florinef), represent another established off-label agent for POTS management in hEDS[7][24]. Fludrocortisone enhances renal sodium reabsorption and water retention, increasing intravascular volume and reducing orthostatic hypotension. Typical dosing ranges from 0.1 mg daily to 0.2 mg twice daily, with careful monitoring for hypokalemia and hypertension that can develop with chronic use. Desmopressin (DDAVP), an antidiuretic hormone analog, functions through similar mechanisms of fluid retention and has shown acute decreases in tachycardia and improvement in POTS symptoms[24].

### Selective Serotonin Reuptake Inhibitors and Serotonin-Norepinephrine Reuptake Inhibitors

Selective serotonin reuptake inhibitors (SSRIs) and serotonin-norepinephrine reuptake inhibitors (SNRIs) represent important off-label agents for hEDS patients experiencing comorbid anxiety, depression, and neuropathic pain components[7][18]. While SSRIs have not been specifically evaluated in randomized controlled trials for hEDS, they are widely prescribed for managing the significant mental health comorbidities documented in this population. Recent data from a German registry demonstrated that mental health disorders, particularly depression and anxiety, are highly prevalent in EDS patients, with a critical need for improved diagnostic pathways and treatment strategies[19]. Among German hEDS/HSD patients surveyed, depression scores meeting threshold criteria were present in substantial proportions, and approximately 30 percent reported lifetime use of antidepressants[19].

SNRIs, particularly venlafaxine and duloxetine, offer the additional advantage of providing pain modulation through norepinephrine reuptake inhibition, making them particularly valuable for hEDS patients with both psychiatric and pain symptoms[7][18]. Tricyclic antidepressants (TCAs) such as amitriptyline were historically more commonly used, with approximately 17.7 percent of surveyed hEDS/HSD patients taking TCAs, likely reflecting their dual benefits for pain and mood[19]. Amitriptyline remains frequently prescribed in hEDS populations for combined pain management and insomnia, though anticholinergic side effects and orthostatic hypotension risk require careful monitoring given that many hEDS patients have concurrent dysautonomia.

### Anticonvulsant Medications for Neuropathic Pain

Anticonvulsant medications, particularly gabapentin and pregabalin, have been employed off-label for managing neuropathic pain components in hEDS patients[18]. These medications modulate voltage-gated calcium channels, reducing synaptic neurotransmitter release and attenuating neuropathic pain signaling. Gabapentin typically begins at 300 mg three times daily and may be titrated to 3600 mg/day divided doses based on response and tolerability[18]. Pregabalin, a more potent analog, is typically dosed from 150 to 600 mg/day in divided doses. The evidence supporting anticonvulsants in hEDS derives from their established efficacy in other neuropathic pain conditions and patient-reported benefits rather than disease-specific clinical trials. However, clinicians must carefully assess whether neuropathic pain truly represents a primary component of the patient's symptomatology or whether centralized pain processing dominates, as anticonvulsants may not address peripheral proprioceptive dysfunction and joint instability that characterize hEDS.

### Mast Cell Stabilizers and Antihistamines

Cromolyn sodium (sodium cromoglycate), a mast cell membrane stabilizer, has emerged as a valuable off-label agent particularly for hEDS patients with concurrent mast cell activation syndrome (MCAS), a highly prevalent comorbidity[37]. Cromolyn works through a fundamentally different mechanism than antihistamines, preventing mast cell degranulation rather than blocking histamine receptors after release. This distinction is mechanistically important, as it addresses the upstream trigger of mast cell activation rather than downstream effects. Cromolyn is typically administered as a 100 mg inhalation powder four times daily or as an oral solution, with effects that are gradual and cumulative[37]. Clinical experience suggests cromolyn can reduce gastrointestinal distress, systemic flares after eating, flushing, itching, neurological symptoms such as brain fog, and dizziness related to mast cell activation[37]. The medication demonstrates particular value in the subset of hEDS patients with overlapping MCAS, as it addresses a pathophysiological mechanism beyond simple joint hypermobility and collagen dysfunction.

Antihistamines represent another established category of off-label agents for hEDS patients with MCAS symptoms. First-generation antihistamines such as hydroxyzine provide central nervous system penetration and can address both histamine-mediated symptoms and associated anxiety, while second-generation agents like cetirizine and fexofenadine offer peripheral selectivity with reduced sedation. H2-receptor antagonists such as famotidine are frequently combined with H1-receptor antagonists to provide broader histamine pathway blockade.

## Contraindications and Medications to Avoid in Hypermobile Ehlers-Danlos Syndrome

### Fluoroquinolone Antibiotics

Fluoroquinolone antibiotics represent a critical class of contraindicated medications in hEDS patients due to substantial evidence linking these agents to severe collagen-associated adverse events, including tendon rupture, retinal detachment, and aortic aneurysm with dissection[22][41]. The mechanistic basis for fluoroquinolone toxicity in connective tissue disorders involves upregulation of matrix metalloproteinases (MMPs) including MMP-1, MMP-2, and MMP-13, which catalyze degradation of type I collagen fibrils and reduce the structural integrity of connective tissues[41]. In a large epidemiological study examining 1.7 million older patients, fluoroquinolone use was associated with an increased hazard of tendon rupture of 3.13-fold compared with unexposed periods, and an increased hazard of aortic aneurysms of 2.72-fold[41]. The median time from fluoroquinolone initiation to these serious adverse events was approximately 19-20 days, indicating that complications can develop rapidly after drug exposure.

For hEDS patients specifically, the risks of fluoroquinolones are substantially amplified because their connective tissues are inherently structurally compromised from collagen abnormalities. The FDA has issued specific warnings against fluoroquinolone use in patients with EDS, Marfan syndrome, and Loeys-Dietz syndrome, particularly when alternative antibiotic options are available[22]. Commonly prescribed fluoroquinolones to be avoided include ciprofloxacin (Cipro), levofloxacin (Levaquin), moxifloxacin (Avelox), and ofloxacin. When fluoroquinolone-responsive infections occur in hEDS patients, alternative antibiotics including macrolides, cephalosporins (with caution), or aminoglycosides should be considered based on organism susceptibility and clinical context.

### Corticosteroids and Immunosuppressive Agents

Systemic corticosteroids require cautious use in hEDS patients, as chronic corticosteroid exposure can further compromise connective tissue integrity through multiple mechanisms including collagen cross-link disruption, impaired wound healing, and accelerated bone loss[22]. While short-term corticosteroid courses may be necessary for acute inflammatory complications or severe MCAS-related symptoms, long-term corticosteroid therapy should generally be avoided in hEDS populations. This concern is particularly relevant given that some hEDS patients experience steroid-responsive symptoms during disease exacerbations, potentially leading to inappropriate long-term corticosteroid exposure. The combination of corticosteroids with fluoroquinolone antibiotics represents a particularly high-risk scenario, as corticosteroids independently increase tendon rupture risk, and when combined with fluoroquinolones, this risk is substantially amplified.

### Medications Triggering or Worsening Mast Cell Activation

In hEDS patients with concurrent MCAS, a broad range of medications can trigger mast cell degranulation and precipitate systemic symptoms through direct histamine release or other mechanisms of mast cell activation[10]. Notably, this list includes medications that might otherwise be considered appropriate for pain management in EDS, creating a therapeutic dilemma requiring careful individual assessment. NSAIDs, commonly recommended as first-line pain management, can trigger mast cell activation in susceptible individuals, though not uniformly across the hEDS population[10]. Opioid narcotics, another established pain management category, may be tolerated by some MCAS patients but trigger severe reactions in others[10]. Specific opioids documented to trigger mast cell activation include meperidine, morphine, and codeine, while fentanyl, remifentanil, alfentanil, oxycodone, and piritramide may be better tolerated[10].

Local anesthetics merit special consideration in hEDS patients requiring dental work, surgical procedures, or other interventions requiring local anesthesia. Benzocaine, chloroprocaine, articaine, tetracaine, and procaine are associated with mast cell activation, while bupivacaine, lidocaine, mepivacaine, prilocaine, levobupivacaine, and ropivacaine represent safer alternatives[10]. This distinction is particularly important for hEDS patients undergoing corrective joint surgery, as anesthetic selection during these interventions can significantly impact postoperative recovery and symptom trajectory.

Numerous other medications require avoidance in MCAS patients, including certain anticonvulsants (carbamazepine, topiramate), selective dopamine-norepinephrine reuptake inhibitors (bupropion), and specific opioid analgesics[10]. Notably, all selective serotonin reuptake inhibitors (SSRIs) are listed as contraindicated in MCAS due to their potential to trigger mast cell activation, creating a significant therapeutic challenge for hEDS patients requiring psychiatric management, as SSRIs represent first-line agents for anxiety and depression. This contradiction necessitates careful clinical reasoning and potentially the exploration of alternative psychiatric medications such as tricyclic antidepressants or other agents with different mechanisms.

## Pharmacological Management of Comorbidities in Hypermobile Ehlers-Danlos Syndrome

### Pain Management in hEDS: Multifaceted Pharmacological Approaches

The management of chronic pain in hEDS represents one of the most challenging aspects of patient care, as the pain phenotype differs substantially from typical osteoarthritic or inflammatory arthritis pain. Multiple pain mechanisms operate concurrently in hEDS, including nociceptive pain from repeated joint microtrauma and subluxations, neuropathic pain from proprioceptive dysfunction and possible nerve compression, and centralized pain processing amplification that develops secondary to chronic pain sensitization[3]. This multimechanistic pain profile necessitates combination pharmacological strategies targeting multiple pathways simultaneously.

The top five self-reported treatments producing pain improvement in hEDS patients were rest, heat therapy, massage, oral medication, and exercise, with exercise demonstrating efficacy in reducing hypermobility-related, joint, and muscle pain[4][4]. However, when examining pharmacological interventions specifically, NSAIDs and acetaminophen remain the most commonly used and studied agents. In one comprehensive survey examining pain medication effectiveness, 45.6 percent of hEDS patients reported NSAID use, with reported pain improvement, though concurrent reports of symptom worsening with topical and oral medications suggest heterogeneous treatment responses[4]. Notably, patients with hEDS reported that topical medications made their muscle pain worse in 48.2 percent of cases, while patients with HSD reported that injections made their joint pain worse in significant proportions[4]. These differential responses highlight the importance of individualized treatment selection rather than protocolized approaches.

Cognitive behavioral therapy (CBT) represents an important nonpharmacological complement to pharmacological pain management in hEDS, with evidence suggesting CBT may be particularly helpful for patients whose pain is difficult to control through pharmacological means alone[3][5]. The integration of CBT with pharmacotherapy acknowledges the significant psychological impact of living with hEDS and the potential for maladaptive pain-related cognitions to amplify suffering and functional limitation.

### Gastrointestinal Dysfunction Management

Gastrointestinal complications occur in up to 84 percent of hEDS patients, driven by collagen abnormalities affecting the muscularis propria and enteric nervous system, resulting in altered gut motility, visceral hypersensitivity, and leaky gut phenomena[29]. Prokinetic agents represent a key pharmacological category for managing gastroparesis and delayed gastric emptying, which are particularly prevalent in hEDS[30][49]. Metoclopramide and domperidone, dopamine antagonists with prokinetic properties, enhance gastric emptying through blockade of inhibitory dopamine receptors and 5-HT4 agonistic effects[30]. A safety evaluation of prolonged metoclopramide and domperidone use in patients with systemic sclerosis (a related connective tissue disorder) demonstrated tolerability for durations exceeding 12 weeks at low daily dosages (10-30 mg/day), with no marked safety concerns despite historical concerns about tardive dyskinesia with long-term metoclopramide use[30].

Erythromycin, a macrolide antibiotic with motilin receptor agonist properties, represents an alternative prokinetic agent for hEDS-related gastroparesis[49]. Erythromycin binds to myenteric neurons and smooth muscle receptors, producing dose-dependent prokinetic effects. Intravenous erythromycin doses of 40-500 mg have been studied, with 200 mg intravenous over 20-30 minutes demonstrating efficacy for improving gastric motility[49]. Combination therapy with metoclopramide and erythromycin has demonstrated superior prokinetic effects compared to monotherapy, with fewer side effects in critically ill patients[49], suggesting potential benefit for hEDS patients with severe refractory gastroparesis.

### Sleep Dysfunction and Insomnia Management

Sleep disturbance represents a significant comorbidity in hEDS, with substantial evidence demonstrating poor sleep duration, increased sleep latency, and elevated use of prescription sleep medications compared to general populations[31]. In one survey, 41.40 percent of hEDS respondents used prescription sleep medication regularly compared to only 8.4 percent of the general population, and sleep latency exceeding 30 minutes was reported by 67.5 percent of subjects[31]. Despite these interventions, only 34.7 percent of participants reported obtaining at least 8 hours of sleep nightly. The most commonly used non-pharmacological sleep aids included positioning pillows (76.32 percent) and room temperature adjustment (63.42 percent).

Prescription sleep medications employed in hEDS include sedating antidepressants such as amitriptyline and mirtazapine, benzodiazepines (though caution is required regarding dependence risk), and other sedative-hypnotic agents. Mirtazapine, a tetracyclic antidepressant with unique pharmacology including H1 receptor antagonism and 5-HT2 receptor antagonism, provides dual benefits for hEDS patients by addressing both sleep disturbance and pain while potentially improving appetite in cachetic patients[45].

### Mental Health Comorbidity Management

The exceptionally high prevalence of mental health comorbidities in hEDS populations requires comprehensive psychiatric management. A recent German registry study specifically examining mental health comorbidities in 96 EDS/HSD patients found depression and anxiety to be the most prevalent psychiatric conditions[19]. Among respondents, 39.8 percent graded their mental health burden as moderate, and 45.9 percent graded it as severe. Only 35.6 percent of patients screening positive for depression were actually receiving antidepressant treatment, suggesting substantial undertreatment of psychiatric comorbidities[19].

Tricyclic antidepressants were the most commonly used antidepressant class in this registry, with 17.7 percent of patients taking TCAs—a higher proportion than SSRIs at 12.5 percent of patients taking serotonin-norepinephrine reuptake inhibitors[19]. This distribution likely reflects the dual therapeutic benefits of TCAs for concurrent pain management, though it contrasts with treatment guidelines emphasizing SSRIs as first-line psychiatric agents. Notably, among 59 patients meeting screening criteria for depression (PHQ-9 ≥ 10), only 21 (35.6 percent) were receiving any antidepressant medication, highlighting a significant treatment gap[19].

## Combination Therapies and Synergistic Approaches

### Multidisciplinary Pain Management Framework

Successful pain management in hEDS requires integrated approaches combining multiple therapeutic modalities simultaneously, as demonstrated by the Muldowney Protocol and similar comprehensive frameworks[40]. The Muldowney Protocol emphasizes biomechanical assessment and intervention, treating pain "through the lens of biomechanics" rather than simply addressing reported symptoms. This framework necessitates coordination among specialists including geneticists, primary care physicians, pain specialists, cardiologists, neurosurgeons, gastroenterologists, nutritionists, mast cell specialists, pulmonologists, and dentists[40].

Combination pharmacotherapy in hEDS pain management typically involves layering agents with different mechanisms: NSAIDs or acetaminophen as baseline analgesics, muscle relaxants for myofascial pain and spasm, potentially low-dose naltrexone for chronic pain with anti-inflammatory effects, and psychotropic medications addressing both psychiatric comorbidities and neuropathic pain components. The sequential addition of agents allows assessment of individual contribution to symptom improvement and identification of contraindicated combinations, such as the interaction between NSAIDs and agents triggering mast cell activation in patients with concurrent MCAS.

### Cardiovascular-Psychiatric-Pain Integration

hEDS patients frequently present with concurrent dysautonomia, psychiatric comorbidities, and chronic pain, requiring careful medication selection to achieve therapeutic benefit across multiple domains while avoiding iatrogenic complications. For example, a patient with hEDS-associated POTS, depression, and chronic pain might benefit from a SNRI such as venlafaxine, which addresses depression and provides pain modulation through norepinephrine reuptake inhibition, while also providing modest blood pressure elevation that could support POTS management. Alternatively, a beta-blocker such as propranolol might address POTS and potentially provide mood benefit through reduced sympathetic activation, though it could theoretically worsen depression in susceptible individuals.

### Compression Garments as Adjunctive to Pharmacotherapy

While not strictly pharmacological, compression garments merit discussion as an established nonpharmacological intervention that shows strong evidence for efficacy in reducing pain and potentially reducing analgesic requirements when used as adjunctive therapy[28]. A recent retrospective study evaluating compression garments in hEDS and HSD patients demonstrated that 80 percent of patients reported reduction in pain at follow-up, and 53.8 percent reported reduction in analgesic use (excluding acetaminophen)[28]. The effectiveness was similar between HSD and hEDS patients, with 85.7 percent of HSD patients and 76.9 percent of hEDS patients reaching composite endpoints of clinical effectiveness[28]. The treatment response appeared linked to baseline proprioceptive impairment, higher body mass index, good adherence, and the "on-off" effect of symptom relief while wearing the garment[28]. This evidence suggests that compression garments should be considered as first-line interventions potentially reducing requirements for pharmacological pain management, particularly in patients with demonstrable proprioceptive dysfunction.

## Emerging Therapies and Future Directions

### Novel Biomarkers and Diagnostic Approaches

Recent research has identified 52-kDa fibronectin as a potential biomarker for hEDS, which could significantly advance diagnostic precision and potentially enable disease-specific targeted therapeutics in future years[12]. However, clinical validation and accessibility of this exploratory discovery remain in early stages. If validated through further studies, objective biomarkers could enable patient stratification for clinical trials, identification of those most likely to respond to particular interventions, and monitoring of disease progression or therapeutic response across populations. Current diagnostic approaches for hEDS rely entirely on clinical criteria, representing a significant limitation compared to other connective tissue disorders with identified molecular markers.

### Nutritional Supplementation Strategies

Historical proposals have suggested that specific combinations of nutritional supplements might address the underlying connective tissue abnormalities in classical-type EDS, though this approach remains largely unexplored in hEDS[15]. The proposed combination included calcium, carnitine, coenzyme Q10, glucosamine, magnesium, methyl sulphonyl methane, pycnogenol, silica, vitamin C, and vitamin K[15]. While no controlled trials have evaluated this specific combination in hEDS populations, the mechanistic rationale is compelling—these agents theoretically support collagen synthesis, cross-linking, and maintenance through diverse biochemical pathways. A detailed observational study examining the effects of ascorbic acid supplementation in EDS type VI patients demonstrated that five grams daily of sodium ascorbate improved bleeding time, wound healing, and muscle strength after one year, with evidence that ascorbate enhanced hydroxylysyl and hydroxyprolyl residue incorporation into collagen in patient-derived fibroblasts[23]. Though these findings are from a specific EDS subtype (type VI, kyphoscoliotic EDS), they suggest that ascorbic acid supplementation might merit investigation in hEDS populations.

Vitamin and mineral deficiencies commonly occur in hEDS due to malabsorption and increased physiological demands related to ongoing connective tissue synthesis and repair[35]. Vitamin D deficiency particularly impacts hEDS patients, affecting bone health, muscle strength, and immune function. Vitamin B12 deficiency, occurring secondary to gastrointestinal dysfunction and impaired absorption, contributes to fatigue and neurological symptoms. Iron deficiency anemia, magnesium deficiency contributing to pain and fatigue, and folate deficiency all represent documented complications that should be systematically screened and corrected through targeted supplementation[35].

### Cannabis and Cannabinoid Therapeutics

Medical cannabis has emerged as an important therapeutic option for hEDS patients with severe chronic pain inadequately controlled through conventional analgesics, though the evidence base remains limited to case reports and patient surveys rather than controlled trials. A detailed case report described an 18-year-old woman with hEDS-related severe pain who had been managed on high-dose opioid therapy (morphine equivalent daily dose of 220 mg) and experienced dramatic pain reduction within days of initiating self-administered vaporized cannabis flower, ultimately achieving complete opioid discontinuation within three months[38]. This patient subsequently required substantially fewer emergency department visits and inpatient hospital days, with dramatic improvements in quality of life enabling participation in physical therapy and psychotherapy that had previously been impossible due to pain severity[38].

A survey of 500 hEDS patients in the United States found that 37 percent utilized cannabis therapeutically, with cannabis use highest among those experiencing moderate-to-severe pain[38]. The mechanisms through which cannabinoids provide pain relief likely involve cannabinoid receptor 1 and receptor 2 signaling in pain processing pathways, potential anti-inflammatory effects, and modulation of central sensitization. However, cannabinoid use in hEDS remains controversial and limited by legal restrictions in many jurisdictions, as well as the scarcity of randomized controlled trials demonstrating efficacy specific to hEDS populations. The absence of rigorously controlled clinical trial data has led to systemic dismissal of cannabinoid therapeutics despite numerous individual patient reports of substantial benefit. Researchers have specifically called for expansion of clinical trials examining the relationship between chronic pain and cannabinoid-based medications to enable evidence-based clinical recommendations[38].

### Surgical Interventions and Perioperative Pharmacological Considerations

Joint surgery for instability-related complications occurs frequently in hEDS populations, with particular prevalence in the knee, ankle, shoulder, wrist, and elbow. A recent retrospective study examining surgical outcomes in 69 non-vascular EDS patients found that overall satisfaction with surgery exceeded 70 percent despite reintervention rates of 35.7 percent to 60 percent depending on joint[39]. Importantly, surgery for joint instability had a significantly greater chance of success when performed in patients with a confirmed EDS diagnosis, highlighting the critical importance of preoperative genetic diagnosis. Of the 69 patients studied, the surgeon was alerted to hEDS diagnosis in only 33.9 percent of cases at the time of primary surgery, suggesting that many surgical interventions proceed without appropriate adaptation of surgical technique or perioperative management to account for connective tissue fragility[39].

Perioperative pharmacological considerations in hEDS require careful attention to anesthetic selection, anticoagulation management, and postoperative pain control. Local and regional anesthesia was "badly tolerated or ineffective" in substantial proportions of patients, with particularly poor tolerance noted in shoulder (36.4%) and elbow surgery (66.6%)[39]. This suggests that anesthetic techniques may require modification or that hEDS patients might benefit from general anesthesia when feasible, though this requires careful airway management given documented airway complications in some EDS subtypes[46].

## Conclusions and Future Directions in hEDS Pharmacotherapy

The therapeutic management of hypermobile Ehlers-Danlos syndrome currently relies upon symptom-focused pharmacotherapy in the absence of disease-modifying agents, reflecting both the complexity of the underlying genetic etiology and the heterogeneity of clinical presentations across the hEDS population. Despite this limitation, a diverse and evolving pharmacological toolkit enables meaningful symptom reduction and functional improvement for many patients when carefully selected and monitored. Nonsteroidal anti-inflammatory drugs and acetaminophen remain first-line analgesic agents, though variable individual responses necessitate personalized therapeutic trials. Emerging agents such as low-dose naltrexone demonstrate particular promise based on growing clinical experience and mechanistic rationale, though rigorous randomized controlled trials specifically designed for hEDS populations remain urgently needed.

The identification of high-risk medication interactions—particularly between NSAIDs and mast cell activation, fluoroquinolone-induced collagen degradation, and corticosteroid-related connective tissue compromise—has substantially refined understanding of medications to avoid. Simultaneously, recognition of the multisystemic nature of hEDS has expanded the pharmacological toolkit to include agents targeting dysautonomia (beta-blockers, ivabradine, fludrocortisone), psychiatric comorbidities (SSRIs, SNRIs, TCAs), gastrointestinal dysfunction (prokinetic agents), and mast cell activation (cromolyn sodium, antihistamines) when present as significant comorbidities.

Future therapeutic development in hEDS requires several critical advances. First, the discovery and validation of disease-specific biomarkers such as the recently identified 52-kDa fibronectin would enable objective diagnosis and disease monitoring, potentially permitting earlier intervention and identification of those most likely to develop serious complications. Second, adequately powered randomized controlled trials examining the efficacy of promising agents like low-dose naltrexone, Mediterranean diet intervention, and emerging therapies would provide rigorous evidence foundations for treatment recommendations currently based on clinical experience and patient reports. Third, mechanistic investigation of the divergent pain phenotypes in hEDS—including nociceptive, neuropathic, and centralized pain components—could enable targeting of specific pain pathways most likely to respond to particular therapeutics, moving toward precision pain management approaches. Fourth, understanding the genetic heterogeneity underlying hEDS, potentially through identification of additional genes beyond currently recognized collagen abnormalities, may reveal novel therapeutic targets for disease-modifying interventions.

Clinicians managing hEDS populations should approach pharmacotherapy with individualized, multimodal strategies integrating pharmacological and nonpharmacological approaches, emphasizing careful monitoring for paradoxical adverse effects and drug-disease interactions unique to connective tissue pathology. The significant morbidity and mortality burden of hEDS, particularly from poorly managed pain and associated disability, necessitates urgent translational research bridging the gap between phenomenological observation and mechanistic understanding, ultimately enabling the development of transformative disease-modifying therapies that currently remain the field's greatest unmet need.

---

## References

The evidence presented in this report draws from 50 distinct sources including clinical trials, mechanistic research, clinical guidelines, and patient outcome studies spanning approximately two decades of published literature with particular emphasis on recent publications from 2023-2026 addressing contemporary understanding of hypermobile Ehlers-Danlos syndrome management and pharmacotherapy. The citations embedded throughout the narrative reflect the specific evidence sources supporting each major claim, enabling clinicians and researchers to access the underlying primary literature for further study and clinical application.