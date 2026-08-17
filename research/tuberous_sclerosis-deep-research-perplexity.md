---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T15:05:37.748201'
end_time: '2026-04-04T15:07:47.914124'
duration_seconds: 130.17
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: tuberous sclerosis
  mondo_id: MONDO:0001734
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 45
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** tuberous sclerosis
- **MONDO ID:** MONDO:0001734

## Research Objectives

Please provide a comprehensive research report on **drug therapies for tuberous sclerosis**.
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

# Comprehensive Research Report on Drug Therapies for Tuberous Sclerosis Complex

This comprehensive research report examines the landscape of pharmacological interventions for tuberous sclerosis complex (TSC), a rare genetic disorder affecting approximately fifty thousand individuals in the United States and nearly two million people globally[4][4]. The report synthesizes evidence on approved therapeutics, investigational agents in clinical development, drug repurposing strategies, and emerging treatment approaches, with particular emphasis on the mechanistic basis for therapeutic efficacy and recent clinical trial outcomes. TSC results from mutations in the TSC1 or TSC2 genes, leading to dysregulation of the mammalian target of rapamycin (mTOR) pathway and the development of benign tumors across multiple organ systems, including the brain, kidneys, skin, heart, and lungs, making it a prototypic condition for precision medicine application in rare genetic disorders[4][5][25].

## Molecular Pathophysiology and Therapeutic Target Identification

Understanding the molecular basis of TSC provides essential context for comprehending why specific pharmaceutical interventions have proven effective. The TSC1-TSC2 complex, along with its auxiliary subunit TBC1D7, normally functions to inhibit mTOR complex 1 (mTORC1), a key serine/threonine protein kinase crucial for cellular growth and metabolism[3][25]. When mutations in either the TSC1 or TSC2 genes disrupt this complex's function, the result is constitutive hyperactivation of mTORC1 signaling[2][3][25]. This pathological activation drives unchecked cell proliferation and survival, leading to the characteristic benign tumors and hamartomas that define the disease[2]. The discovery of this molecular mechanism was transformative for drug development, as it identified mTOR as a rational therapeutic target and prompted the clinical repurposing of mTOR inhibitors, compounds originally developed for transplant immunosuppression and cancer therapy[2][16].

The TSC1-TSC2 complex regulates mTORC1 activity by integrating signals from growth factors, amino acids, and energy status[25]. In normal cells, the complex acts as a central hub that coordinates these diverse inputs to maintain appropriate cellular growth and differentiation[25]. Beyond direct mTORC1 inhibition, the TSC1-TSC2 complex independently activates mTOR complex 2 (mTORC2), which is essential for AKT activation and promotes cell survival, growth, and proliferation through inhibition of downstream substrates like FOXO1/3A and GSK3β[25]. This multimodal regulatory function explains why mTOR inhibition, while effective, remains incomplete in addressing all aspects of TSC pathophysiology and why combination therapeutic approaches are increasingly being explored[31].

## Approved Pharmacological Therapies for Tuberous Sclerosis Complex

### mTOR Inhibitors: Everolimus

Everolimus (Afinitor, Votubia) represents a synthetic analogue of rapamycin that functions as an allosteric inhibitor of mTORC1[1][2][6]. The drug binds to FK506 binding protein 1A 12 kDa (FKBP12), and this complex then interacts with mTOR to inhibit mTORC1 signaling through an allosteric mechanism[5][6]. Importantly, everolimus and other rapalogs are not direct kinase inhibitors at pharmacologically achievable drug concentrations; rather, they selectively inhibit the kinase-independent functions of mTORC1[2][6]. The molecular mechanism involves inhibition of downstream effectors S6K1 and 4E-BP1, with resultant suppression of cap-dependent translation, transcription, and cell cycle progression[6].

Regulatory approvals for everolimus in TSC have occurred sequentially across multiple disease manifestations. On April 10, 2018, the FDA approved everolimus tablets for oral suspension (Afinitor Disperz, manufactured by Novartis Pharmaceuticals Corporation) for the adjunctive treatment of adult and pediatric patients aged two years and older with tuberous sclerosis complex-associated partial-onset seizures[1]. This approval was based on the EXIST-3 trial (NCT01713946), a randomized, double-blind, multicenter study enrolling 366 patients with TSC-associated partial-onset seizures who demonstrated inadequate seizure control despite receiving at least two sequential antiepileptic drug regimens[1]. The trial demonstrated statistically significant reductions in seizures for the low-trough everolimus arm (29.3% reduction, 95% CI 18.8-41.9, p=0.003) and the high-trough arm (39.6% reduction, 95% CI 35-48.7, p<0.001) compared with placebo (14.9% reduction)[1]. The proportion of patients achieving at least fifty percent reduction in seizure frequency was higher in both everolimus arms (28.2% and 40.0%, respectively) compared with placebo[1].

Beyond seizure management, everolimus is also approved by regulatory authorities for two additional TSC manifestations. The European Medicines Agency (EMA) granted conditional marketing authorization for Votubia on September 2, 2011, for the treatment of subependymal giant cell astrocytomas (SEGAs) and renal angiomyolipomas in TSC patients[21]. This conditional authorization was converted to full marketing authorization on November 16, 2015, reflecting accumulated long-term efficacy and safety data[21]. The FDA approved everolimus for SEGA treatment in TSC patients based on the EXIST-1 trial[15], which demonstrated that thirty-five percent of patients receiving everolimus achieved at least a fifty percent reduction in SEGA volume compared with zero percent in the placebo group (difference 35, 95% CI 15-52, p<0.0001)[15]. For renal angiomyolipomas, everolimus demonstrated similar efficacy, with 73.2% of eligible patients achieving target lesion response[15].

The pharmacokinetics and therapeutic drug monitoring of everolimus requires careful attention in clinical practice. Patients are initiated at starting doses of three to six mg/m² daily (adjusted based on age and concurrent CYP3A4/P-glycoprotein inducer use), with subsequent titration to achieve target blood trough concentrations of five to fifteen ng/mL[1][5]. This precision dosing approach reflects the narrow therapeutic window for optimal efficacy and tolerability and necessitates regular therapeutic drug monitoring through tandem mass spectrometry[1][5]. The recommended maintenance dose is typically five mg/m² orally once daily with dose adjustments in five mg increments to maintain therapeutic trough levels[1]. In patients with severe hepatic impairment or concurrent P-glycoprotein and moderate CYP3A4 inhibitors, dose reduction is necessary, while concurrent P-glycoprotein and strong CYP3A4 inducers may require dose increases[1].

Long-term efficacy data for everolimus in TSC-associated epilepsy demonstrate sustained benefit over extended treatment periods. An analysis of the first prospective human clinical trial for TSC patients with medically refractory epilepsy treated with everolimus reported outcomes over four years, with the study providing Class IV evidence that prolonged everolimus treatment up to four years effectively treats refractory epilepsy in TSC patients[13][13]. More than eighty percent of individuals with TSC develop epilepsy, typically within the first year of life, with an estimated one-third developing drug-resistant seizures that fail to respond to conventional anticonvulsant medications[13][13]. The long-term extension analysis demonstrated that improved seizure control was maintained for four years in the majority of patients, with seizure frequency reduction persisting at rates comparable to initial treatment phases[13][13].

### Vigabatrin for Infantile Spasms

Vigabatrin (VGB) represents the first-line pharmacological treatment for infantile epileptic spasm syndrome (IESS) in TSC patients, having been approved by the FDA in 2009 for the add-on treatment of drug-resistant complex focal seizures and as treatment for infantile spasms[8][12]. The mechanism of action involves irreversible inhibition of GABA transaminase, the enzyme responsible for GABA catabolism, thereby increasing gamma-aminobutyric acid concentrations in the central nervous system[12][12]. Additionally, vigabatrin inhibits mTOR pathway activity, providing a dual mechanism that may further account for its particular efficacy in TSC-associated seizures[5]. The drug has the distinctive property of producing substantially elevated cerebral GABA concentrations, which enhances inhibitory neurotransmission and suppresses seizure activity[17].

A systematic review examining efficacy and safety data for vigabatrin in TSC patients with infantile epileptic spasm syndrome identified seventeen relevant studies, comprising three randomized controlled trials and fourteen observational studies[8]. The analysis yielded an overall response rate of 67% (231 of 343 responders), with a spasm-free rate restricted to randomized controlled trials of 88% (29 of 33 subjects)[8]. Although all analyzed studies reported beneficial effects of vigabatrin in TSC patients with IESS, with notably higher response rates compared to non-TSC subjects with IESS, the systematic review concluded that low heterogeneity and high level of evidence did not guarantee sufficient strength for definitive therapeutic recommendations[8].

The EPISTOP trial (Long-Term, Prospective Study Evaluating Clinical and Molecular Biomarkers of Epileptogenesis in a Genetic Model of Epilepsy-Tuberous Sclerosis Complex) completed recently and provided landmark evidence for preventive vigabatrin treatment in TSC infants[3][5][42]. This multicenter study enrolled ninety-four infants with TSC without prior seizure history, who were followed with monthly video electroencephalography and received vigabatrin either as conventional antiepileptic treatment initiated after the first electrographic or clinical seizure or preventively when epileptiform EEG activity was detected before clinical seizures[42]. The results demonstrated that preventive treatment with vigabatrin significantly extended the time to first clinical seizure: in the randomized controlled trial component, this interval was 364 days (95% CI 223-535) with preventive treatment versus 124 days (95% CI 33-149) with conventional treatment[42]. Pooled analysis at twenty-four months showed preventive treatment reduced the risk of clinical seizures (odds ratio 0.21, p=0.032), drug-resistant epilepsy (odds ratio 0.23, p=0.022), and infantile spasms (odds ratio approaching zero, p<0.001)[42]. Critically, no adverse events related to preventive treatment were noted, establishing safety for this preventive approach[42].

However, vigabatrin carries an important limitation due to potential irreversible peripheral visual field defects that may occur with prolonged use at higher doses[11]. While previous studies in TSC animal models suggested that taurine deficiency might underlie vigabatrin-related retinal toxicity, a clinical study failed to find evidence supporting this hypothesis[11]. Despite the absence of formal recommendations for taurine supplementation in children receiving vigabatrin, some clinical centers provide supplementation given the absence of known contraindications[11]. The recommended preventive dosing for vigabatrin in TSC infants is one hundred mg/kg/day based on EPISTOP trial findings[11], with escalation to 150 mg/kg/day if clinical seizures occur[11]. Higher doses of vigabatrin are associated with lower relapse rates, suggesting a dose-response relationship[11].

### Cannabidiol for TSC-Associated Seizures

Cannabidiol (CBD, marketed as Epidiolex in the United States and Epidyolex in the European Union) represents a purified, non-intoxicating cannabinoid that has recently been approved for TSC-associated seizures in multiple regulatory jurisdictions[3][23]. The drug received FDA approval as an adjunctive treatment for TSC-associated seizures in patients one year of age and older[7], while the EMA approved it for patients two years of age and older[23]. Unlike tetrahydrocannabinol (THC), the primary psychoactive cannabinoid, CBD does not produce intoxication and appears to function through multiple mechanisms including modulation of the mTOR pathway, GABA receptor function, and other neurotransmitter systems[3][23].

The regulatory approvals for cannabidiol in TSC-associated seizures were based on randomized, double-blind, placebo-controlled clinical trials demonstrating significant efficacy in reducing seizure frequency. In the pivotal trial (GWPCARE6, NCT02544763), patients receiving adjunctive cannabidiol achieved substantially greater median percentage reductions in seizure frequency compared to placebo[23][45]. Long-term, open-label extension data for cannabidiol in TSC-associated seizures demonstrated sustained efficacy through forty-eight weeks of treatment[3][45]. Median percentage reductions in seizure frequency across twelve-week windows ranged from 54% to 68% over the forty-eight week observation period[45]. Seizure responder rates (defined as achieving at least a fifty percent reduction in seizure frequency) ranged from 53% to 61% across twelve-week windows, with 29% to 45% achieving at least seventy-five percent seizure reduction and 6% to 11% achieving complete seizure freedom[45]. Critically, eighty-seven percent of patients and caregivers reported subjective global improvement in quality of life and epilepsy control at week twenty-six of the extension phase[45].

The adverse effect profile of cannabidiol is generally mild to moderate. The most frequent adverse events reported in long-term extension data were diarrhea (42% of patients), seizure (22%, likely baseline disease manifestation), and decreased appetite (20%)[45]. Elevated liver transaminases occurred in seventeen patients (9%), with twelve of these patients concurrently taking valproate, suggesting a potential pharmacokinetic interaction[45]. Despite these side effects, only six percent of patients permanently discontinued treatment due to adverse events[45]. The major adverse effects identified in regulatory trial populations were mild to moderate gastrointestinal upset (diarrhea and decreased appetite) and somnolence, occurring in 91% of patients receiving less than twenty-five mg/kg/day and nearly all patients receiving greater than twenty-five mg/kg/day[11]. Important drug-drug interactions exist between cannabidiol and concurrent antiseizure medications; cannabidiol affects the metabolism of several medications and can exacerbate their side effects, particularly for valproic acid and clobazam, often necessitating dose adjustments of concurrent medications[11].

### Sirolimus for Lymphangioleiomyomatosis and Other TSC Manifestations

Sirolimus (also known as rapamycin), a macrolide produced by Streptomyces hygroscopicus, functions as an mTOR inhibitor similar to everolimus but with distinct pharmacokinetic and clinical profiles[2][10]. The FDA approved sirolimus for the treatment of lymphangioleiomyomatosis (LAM), a progressive lung disease that occurs sporadically or in combination with TSC and predominantly affects women of reproductive age[10]. The drug binds to FK-binding protein-12 (FKBP-12) to form a complex that inhibits mTORC1 activation through an allosteric mechanism[10]. In LAM pathophysiology, LAM cells contain inactivating mutations in tuberous sclerosis proteins, resulting in mTORC1 pathway-driven cellular proliferation, migration, and survival, with elaboration of lymphangiogenic growth factors such as vascular endothelial growth factor-D promoting lymphatic spread and cystic lung destruction[10].

The landmark Multicenter International LAM Efficacy of Sirolimus (MILES) Trial demonstrated that sirolimus stabilized lung function, reduced serum VEGF-D concentrations, and improved functional performance and quality of life compared to placebo over one year, although benefits waned when the drug was discontinued in the second year[10]. This finding established that continuous therapy may be required for sustained benefit and prompted a paradigm shift in understanding TSC manifestations as requiring long-term disease-modifying therapy rather than temporary symptomatic treatment[10]. In another phase 3 trial of LAM patients, sirolimus improved lung function, quality of life, and functional performance[10]. These collective findings led to FDA approval of sirolimus for drug treatment of LAM in 2015[10].

Beyond LAM treatment, sirolimus and everolimus have demonstrated broad efficacy across multiple TSC manifestations. Double-blind, placebo-controlled clinical trials have demonstrated the effectiveness of mTOR inhibitors in treating TSC-related brain and kidney tumors such as SEGAs and angiomyolipomas that are not candidates for surgery[5]. Both sirolimus and everolimus can reduce TSC-related lesions including kidney angiomyolipomas, SEGAs, and facial angiofibromas in both human patients and animal models[5]. Regarding cardiac rhabdomyomas, mTOR inhibitors are considered temporary and safe treatment for symptomatic cardiac rhabdomyomas in children with TSC, especially for high-risk or inoperable tumors, though high-quality randomized trials are needed to further validate these effects[5].

A notable emerging application involves preventive mTOR inhibitor treatment in very young TSC patients before manifestation of clinical disease. A phase 1 clinical trial (STOP2A: Stopping TSC Onset and Progression 2: Epilepsy Prevention in TSC Infants) prospectively evaluated the safety and potential efficacy of preventive sirolimus in infants with TSC without prior seizures[16][16]. The trial treated five patients until twelve months of age, with enrolled infants required to be younger than six months at enrollment with no history of seizures and no clinical indication for sirolimus treatment[16]. Results demonstrated that sirolimus was both safe and well tolerated by infants with TSC in the first year of life[16]. Additionally, preliminary work suggested a favorable efficacy profile compared to previous TSC cohorts not exposed to early sirolimus treatment[16]. These encouraging findings have prompted expansion to a larger prospective phase 1/2b multicenter, placebo-controlled clinical trial (TSC-STEPS, NCT05104983) to confirm and extend these results[16].

Low-dose sirolimus regimens have been explored in LAM patients with the goal of reducing adverse effects while maintaining efficacy. Low-dose sirolimus treatment with trough levels below five ng/mL was recently shown to improve lung function in sixteen patients with LAM, including nine without chylous effusion and seven with chylothorax with documented resolution of the effusion[10]. Several studies have demonstrated trends in improvement of FEV₁ (forced expiratory volume in one second) and disease progression after low-dose sirolimus treatment[10]. In a prospective LAM national cohort, Bee and colleagues demonstrated that lower serum sirolimus levels were associated with fewer adverse effects but not necessarily with lower efficacy in FEV₁ decline[10].

## Anti-Seizure Medications for TSC-Associated Epilepsy

### Conventional Antiepileptic Drugs and Treatment Algorithms

Beyond mTOR inhibitors and cannabidiol, conventional antiepileptic drugs (AEDs) remain essential components of TSC-associated epilepsy management. The choice of specific AED is based on individual seizure type, epilepsy syndrome, involved organ systems, age of the patient, and side-effect profiles[12]. The TSC Consensus Meeting for SEGA and Epilepsy Management in 2012 recommended vigabatrin as the first drug of choice for infantile spasms and focal seizures secondary to TSC[12]. Other commonly used AEDs include lamotrigine, levetiracetam, clobazam, valproic acid, topiramate, oxcarbazepine, and carbamazepine[12][12].

Lamotrigine acts by blocking sodium channels and has demonstrated efficacy in focal seizures associated with TSC[12]. Levetiracetam employs a distinct mechanism involving action on neurotransmitter release, synaptic vesicle protein 2 (SV2), and calcium signaling, making it useful for multiple seizure types in TSC[12][12]. Clobazam allosterically activates the GABA receptor and binds less to subunits that mediate sedative effects compared to other benzodiazepines[12][12]. Topiramate inhibits GABA transaminase and increases cerebral GABA concentrations, with studies demonstrating that single doses increase cerebral GABA concentrations acutely by approximately seventy percent compared to baseline[17].

Certain drugs require careful consideration or avoidance in TSC patients due to risk of seizure exacerbation. Carbamazepine, oxcarbazepine, and phenytoin may cause exacerbation of seizures, particularly in younger children and infants, and can precipitate or aggravate infantile spasms[12][12]. Consequently, oxcarbazepam use requires caution in children younger than two years of age given concerns that it could result in recurrence of infantile spasms[11]. Despite these risks, oxcarbazepine has been studied in TSC populations; clinical experience with treatment of epilepsy in twenty-eight individuals with TSC using oxcarbazepine showed that thirty-six percent of patients became seizure-free and twenty-one percent experienced greater than fifty percent seizure reduction (total responders thirty-six percent)[22]. However, oxcarbazepine appeared less effective than lamotrigine based on comparative data[22].

### GABA Modulation and Neuroactive Steroids

The importance of GABA (gamma-aminobutyric acid), the most prevalent inhibitory neurotransmitter in the mammalian brain, as a therapeutic target for seizure management extends beyond traditional GABA reuptake inhibitors to include selective GABA receptor modulators. Ganaxolone, a neuroactive steroid, belongs to a novel class of anticonvulsant agents that exhibit potent modulatory effects on GABAA receptors[24][40]. Ganaxolone selectively binds to a specific site on GABAA receptors, enhancing the receptor's response to GABA and increasing the frequency of channel opening events, resulting in increased chloride ion influx and neuronal membrane hyperpolarization that decreases excitability[24][40].

Preclinical studies demonstrated the potent anticonvulsant effects of ganaxolone in the mouse amygdala kindling model, where it effectively suppressed both behavioral and electrographic seizures, underscoring its potential as a broad-spectrum anti-epileptic agent[24]. A phase 2 open-label study of adjunctive ganaxolone in twenty-three TSC patients aged two to thirty-two years with refractory TSC-related epilepsy reported a median percentage reduction in seizure frequency of 16.6% versus baseline after four-week titration to maximum doses of sixty-three mg/kg/day (maximum 1800 mg/day)[24]. At least fifty percent of participants achieved response rates of 30.4% or higher[24]. The most common adverse events were somnolence, fatigue, and sedation[24]. Notably, post hoc analysis suggested possible superior efficacy in patients not experiencing somnolence, leading authors to propose that enhancing tolerability through optimized dose titration may improve seizure control in future trials[24].

A phase 3, global, double-blind, randomized, placebo-controlled trial evaluating the efficacy and safety of adjunctive ganaxolone treatment in children and adults with epilepsy associated with TSC is currently ongoing[24]. This trial will enroll approximately 162 participants aged one to sixty-five years with clinical or genetic TSC diagnosis and refractory epilepsy (defined as failure to achieve seizure control despite adequate trial of at least two antiepileptic drugs)[24]. Participants are randomized 1:1 to receive oral ganaxolone or matching placebo three times daily, with study design comprising a four-week prospective baseline phase, four-week titration period, and twelve-week maintenance period[24]. The primary efficacy outcome is percentage change in twenty-eight-day seizure frequency from baseline during titration and maintenance periods, with secondary endpoints including fifty percent responder rate, clinical global impression of improvement, quality of life measures, and adverse event monitoring[24].

## Investigational and Emerging Therapies

### Novel mTOR Pathway Inhibitors and Combination Approaches

Beyond established mTOR inhibitors, novel mechanistic approaches to targeting mTOR dysregulation in TSC are under investigation. Research has identified that loss of TSC2 confers inflammation via nuclear factor-kappa B (NF-κB) pathway dysregulation that is not directly regulated by mTORC1 inhibition alone[31]. This discovery prompted investigation of combination therapies targeting both NF-κB and mTORC1 signaling[31]. In TSC2-deficient cell models, combined mTORC1 and NF-κB inhibition proved potent at preventing anchorage-independent growth, and markedly importantly, unlike mTORC1 inhibition alone, was sufficient to prevent colony regrowth after cessation of treatment[31]. These findings suggest that NF-κB pathway inhibitors may represent viable adjunct therapy with current mTOR inhibitors to treat TSC, potentially providing more durable therapeutic benefit[31].

### Clinical Trials for Emerging Pharmacologic Approaches

Multiple active clinical trials are evaluating novel therapeutic approaches in TSC populations. The RaRE-TS trial (NCT05534672) is a placebo-controlled study assessing the efficacy and safety of rapamycin (sirolimus) in drug-resistant epilepsy associated with TSC in individuals aged three months to fifty years[16]. The ViRap trial (NCT04987463) is a two-arm, randomized, double-blind, double-dummy, placebo-controlled study evaluating the efficacy, tolerability, and safety of vigabatrin versus rapamycin as preventive treatment in infants with TSC, with participants randomized to receive vigabatrin or rapamycin based on presence of epileptiform activity on baseline video EEG[16]. These trials represent ongoing efforts to optimize preventive approaches and establish comparative efficacy of different treatment strategies in early TSC.

## Contraindications and Drugs to Avoid in TSC

### Contraindicated Antiepileptic Drugs

Certain antiepileptic medications are contraindicated or require extreme caution in TSC patients due to evidence of seizure exacerbation. As noted previously, carbamazepine, oxcarbazepine, and phenytoin carry documented risk of precipitating or aggravating infantile spasms[12][12]. The mechanistic basis for this contraindication relates to these drugs' effects on GABA transmission in specific brain regions; the aggravation of absence and myoclonic seizures seen with carbamazepine and oxcarbazepine in genetic absence epilepsy animal models and in patients with idiopathic generalized epilepsies is likely due to these drugs' ability to enhance GABA currents in the ventral basal thalamus[40]. In TSC populations, this regional GABA enhancement may paradoxically worsen seizure control in the context of the underlying mTOR pathway dysregulation.

### Other Considerations

Long-term use of benzodiazepines and barbiturates, which have prominent sedating properties, should generally be avoided in TSC populations[12]. This avoidance relates to the association between prolonged benzodiazepine exposure and cognitive side effects, which are particularly concerning in TSC patients already at elevated risk for neurodevelopmental complications. The cognitive effects of polypharmacy in TSC-associated neuropsychiatric disorders represent an important clinical consideration, as cognitive impairment is already prevalent in this population and may be exacerbated by excessive medication burdens.

## Adverse Events Associated with TSC Pharmacotherapy

### Adverse Effects of mTOR Inhibitors

The most common adverse reactions occurring in at least ten percent of patients receiving everolimus in the EXIST-3 trial were stomatitis, diarrhea, vomiting, nasopharyngitis, upper respiratory tract infection, pyrexia, cough, and rash[1]. Stomatitis and mouth ulceration represent the most frequently reported treatment-related adverse events, occurring in 43.2% and 32.4% of patients, respectively, in long-term extension studies[29]. For management of stomatitis and mucositis, clinical practice includes use of dexamethasone swish-and-spit at treatment initiation to reduce oral ulcer incidence, with some centers also adding lysine supplementation to decrease recurrent ulcer development[11].

Both everolimus and sirolimus are associated with increased infection risk, including serious infections. While sirolimus did not increase infection risk compared to placebo in the phase 3 LAM clinical trial, this null finding was likely due to the effect of low-dose sirolimus employed[10]. In contrast, higher-dose mTOR inhibitor regimens are associated with increased infection risk, including serious bacterial infections. Two case reports documented severe pneumonia caused by Mycoplasma pneumoniae in children receiving everolimus for TSC-associated epilepsy[33]. Both pediatric patients required intensive care unit admission for severe pneumonia with pleural effusion and subsequently tested positive for Mycoplasma pneumoniae by polymerase chain reaction[33]. One patient required broad-spectrum antibiotics and high-concentration oxygen support, while the other developed septic shock requiring mechanical ventilation, vasoactive drugs, pleural drainage, and broad-spectrum antibiotics[33]. Importantly, in both cases, discontinuation of everolimus was followed by clinical improvement, with both patients discharged without sequelae[33].

Other serious adverse effects documented with mTOR inhibitor therapy include metabolic complications. The potential adverse effects include transaminitis, proteinuria, hypertriglyceridemia, hyperlipidemia, diabetogenic effects, immunosuppression and infection, oral mucositis, pneumonitis, bone marrow suppression, and fetal growth restriction[34]. Specific metabolic complications documented include anemia (12-76%), leucopenia (11%), and thrombocytopenia (up to 30%)[34]. Metabolism-related adverse effects include diabetes mellitus (20-27%), hyperlipidemia (30-64%), and hypertriglyceridemia (21-57%)[34].

### Adverse Effects of Vigabatrin

The primary limiting factor for vigabatrin use involves the potential for irreversible peripheral visual field defects that may occur with prolonged exposure, particularly at higher doses. While taurine supplementation has been proposed to mitigate vigabatrin-related retinal toxicity based on animal model findings, clinical evidence for taurine deficiency as the underlying mechanism remains lacking[11]. Nevertheless, some clinical centers provide taurine supplementation given the absence of known contraindications[11].

### Adverse Effects of Cannabidiol

Mild to moderate gastrointestinal upset and somnolence represent the primary adverse effects of cannabidiol. These effects were present in 91% of patients receiving less than twenty-five mg/kg/day and nearly all patients receiving greater than twenty-five mg/kg/day[11]. Drug interactions between cannabidiol and other medications are clinically significant; cannabidiol affects the metabolism of several medications and can exacerbate their side effects, particularly for valproic acid and clobazam, often necessitating dose adjustments of concurrent medications[11].

## Drug-Drug Interactions and Pharmacokinetic Considerations

### mTOR Inhibitor Pharmacokinetics and Drug Interactions

Sirolimus and everolimus undergo hepatic metabolism and can interact with drugs affecting these metabolic pathways. These compounds inhibit several major organic anion-transporting polypeptide (OATP) transporters expressed in the liver and intestine[35]. Sirolimus and everolimus inhibited in a dose-dependent manner the uptake of estrone sulphate by OATP1A2 and OATP1B1 and that of mycophenolic acid 7-O-glucuronide by OATP1B3[35]. Inhibitory concentrations for these OATPs ranged from 1.3 to 11.9 µM for sirolimus and 4.1 to 4.3 µM for everolimus[35]. Importantly, the major OATP transporters expressed in the liver and intestine do not appear to contribute significantly to the pharmacokinetics of sirolimus or everolimus uptake, but rather mTOR inhibitors themselves function as inhibitors of these transporters, potentially affecting the pharmacokinetics of other substrate medications[35].

## Combination Therapies and Treatment Strategies

### Adjunctive Treatment Approaches

TSC-associated epilepsy frequently requires combination therapy due to the refractory nature of seizures in many patients. A retrospective study of long-term seizure control using valproate, levetiracetam, and lamotrigine in mono- and combination therapy showed that combination therapy was superior to levetiracetam and lamotrigine monotherapy for complete seizure control (p=0.031)[27]. Notably, combination therapy not including valproate was non-inferior to valproate monotherapy in all settings, suggesting that effective seizure control can be achieved through multiple therapeutic approaches[27]. Complete seizure control was achieved in less than fifty percent of patients in five-year follow-up, underscoring the challenges of achieving complete seizure freedom in genetic epilepsies[27].

### Integration of Targeted and Symptomatic Therapies

Contemporary management of TSC-associated epilepsy increasingly integrates targeted mTOR-inhibitor therapy with conventional antiepileptic drugs to achieve optimal seizure control while addressing underlying disease pathophysiology. The EXIST-3 trial demonstrated that everolimus produced statistically significant seizure reduction when added to one to three existing antiepileptic drugs, indicating that combination therapy with mTOR inhibitors represents a rational approach for patients with inadequate seizure control on conventional medications[1][14]. The trial specifically enrolled patients with inadequate seizure control despite receiving at least two sequential antiepileptic drug regimens, establishing the patient population most likely to benefit from this adjunctive approach[1].

Post-hoc analysis of EXIST-3 data examining adjunctive everolimus in pediatric populations revealed sustained reductions in seizure frequency after one year of treatment across both younger children (aged less than six years) and older children (aged six to less than eighteen years)[14]. Response rates (defined as at least fifty percent reduction in seizure frequency) were 34.1% (95% CI 24.6-39.7) versus 30.0% (95% CI 19.6-42.1) in the younger subgroup and 43.8% (95% CI 36.8-51.1) versus 27.5% (95% CI 19.6-36.0) in the older subgroup for low-exposure and high-exposure everolimus respectively compared to placebo[14]. Median reductions in seizure frequency were 12.3% (95% CI -10.1 to 24.8) in younger children receiving low-exposure everolimus and 29.8% (95% CI 10.1-54.1) in those receiving high-exposure, compared to 3.8% (95% CI -15.4 to 24.2) in placebo[14]. At cutoff date for the extension phase, grade 3 or 4 adverse events were reported in 45% of younger patients (commonly pneumonia, n=16) and 38% of older patients (commonly pneumonia and stomatitis)[14].

### Preventive Strategies and Early Intervention

Emerging evidence supports early intervention with targeted therapies before manifestation of clinical disease to potentially alter the natural history of TSC complications. The EPISTOP trial established preventive vigabatrin treatment as effective in reducing risk and severity of epilepsy in TSC infants[3][5][42]. While preventive vigabatrin treatment resulted in reduced risk of seizures, infantile spasms, and drug-resistant epilepsy, there was no difference in the prevalence of developmental delay or autism at age two years in the initial report[3]. These findings prompted investigation of other prevention strategies, particularly with mTOR inhibitors that might address underlying disease pathophysiology while also modifying epileptogenesis.

The STOP2A trial's encouraging preliminary findings in preventive sirolimus have prompted larger scale investigation[16][16]. The preliminary work suggested a favorable efficacy profile compared to previous TSC cohorts not exposed to early sirolimus treatment, with effects of sirolimus on epilepsy prevention and cognitive/neurodevelopmental outcomes when initiated early in life, before onset of EEG abnormalities and clinical seizures, demonstrating promise that supports the need for larger clinical trials to confirm safety and efficacy of sirolimus in infants with TSC[16]. The larger follow-up phase 1/2b multicenter, placebo-controlled clinical trial (TSC-STEPS) is already underway to confirm and extend these results[16].

## Special Populations and Clinical Considerations

### Neonatal and Fetal Applications

mTOR inhibitors have been successfully employed in prenatal settings to manage life-threatening fetal cardiac rhabdomyomas[19][34]. A systematic literature review identified twenty documented cases from fifteen reports, all presenting lifesaving effects of mTOR inhibitors in fetuses and neonates with cardiac rhabdomyomas[34]. Cardiac rhabdomyoma represents the most common primary fetal tumor of the heart, accounting for 60-70% of all cardiac tumors and closely associated with TSC[34]. The tumor is typically benign but can cause outflow tract obstruction, arrhythmias, low cardiac output, hydrops, and if progressive, heart failure and fetal demise[34]. In ten reports, the indication for initiating mTOR-inhibitor treatment was progressive rhabdomyoma growth, mostly with consecutive in- or outflow tract obstruction and imminent low cardiac output, congestion, and hydrops fetalis[34].

Despite the lifesaving potential of fetal mTOR inhibition, adverse effects warrant careful consideration. One case report documented a prenatally suspected cardiac rhabdomyoma with imminent bilateral outflow tract obstruction that was prenatally treated with sirolimus with achievement of tumor regression; however, prenatal sirolimus had to be discontinued after five weeks due to maternal medical reasons, and the patient subsequently developed incessant atrioventricular re-entrant tachycardia that was unresponsive to electric or medical cardioversion with amiodarone and unresponsive to postnatal everolimus[19]. The patient developed massive capillary leak syndrome within hours, and this combination with restrictive ventricular filling properties and tachycardia resulted in death on the seventh day of life[19]. These cases underscore that while mTOR inhibitors offer lifesaving potential in fetal cardiac rhabdomyoma, serious adverse effects cannot be disregarded[19][34].

### TSC-Associated Neuropsychiatric Disorders Management

TSC-associated neuropsychiatric disorders (TAND) represent a major source of morbidity beyond seizure control. A retrospective study of neuropsychiatric profiles in TSC patients with epilepsy demonstrated that uncontrolled seizures were associated with higher rates of intellectual disability and more pronounced TAND manifestations compared to controlled seizures[37]. Autism spectrum disorder (ASD) was reported in 42% of a studied cohort, with significant correlations found between epilepsy severity and ASD-related domains on the TAND checklist[37]. Intellectual disability was prevalent in 67.6% of the cohort, with variability attributed to genetic background and early severe neurological presentations[37].

Despite advances in treatment options including mTOR inhibitors and newer antiepileptic drugs, unmet needs remain in comprehensive TSC care[37]. A study analyzing psychiatric problems in TSC found that mood disorders occurred in 35.3% of TSC patients compared to 6.2% of controls, anxiety disorders in 58.8% versus 12.5%, and ADHD in 13.3% versus 6.7%[36]. Any psychiatric disorder was documented in 76.5% of TSC patients compared to 25% of controls[36]. Problems with attention in TSC patients have been documented even after controlling for intellectual quotient, with eighteen of twenty children with TSC demonstrating deficits on at least one attentional task[36]. Medications for managing these neuropsychiatric manifestations include antipsychotics for irritability and aggression (risperidone, aripiprazole), stimulants for ADHD (methylphenidate), and selective serotonin reuptake inhibitors for anxiety and depression[36]. The International TSC Consensus Guidelines published recommendations in September 2023 for identification and treatment of TAND through the TAND Consortium, addressing the urgent clinical need for systematic approaches to these prevalent complications[39].

## Regulatory Status and Global Approval Landscape

The regulatory approval landscape for TSC therapeutics has evolved substantially, reflecting accumulating clinical evidence and recognition of unmet medical needs. In the United States, the FDA has approved everolimus for TSC-associated seizures, SEGAs, and renal angiomyolipomas, vigabatrin for infantile spasms and drug-resistant focal seizures, cannabidiol for TSC-associated seizures, and sirolimus for LAM. In Europe, the EMA approved Votubia (everolimus) on September 2, 2011, with conditional marketing authorization converted to full authorization on November 16, 2015, for SEGA and renal angiomyolipoma treatment[21]. For TSC-associated partial-onset seizures, everolimus was approved by the EMA as an adjunctive treatment in patients from two years of age[5].

The Japanese Pharmaceuticals and Medical Devices Agency (PMDA) has also approved various TSC therapeutics, reflecting recognition of the disease burden in Asian populations[20]. Other emerging regulatory approvals are expected as investigational agents including ganaxolone and combination therapies advance through clinical development programs.

## Conclusion and Future Directions

The pharmacological management of tuberous sclerosis complex has undergone remarkable transformation over the past fifteen years with the identification of mTOR pathway dysregulation as the central pathophysiologic mechanism and the consequent development and regulatory approval of mTOR inhibitors for multiple disease manifestations. Everolimus has demonstrated efficacy for TSC-associated seizures, SEGAs, and renal angiomyolipomas, with long-term follow-up data supporting sustained benefit over years of continuous therapy. Vigabatrin remains the first-line treatment for infantile spasms in TSC, with emerging evidence supporting preventive approaches to modify the natural history of epilepsy. Cannabidiol represents a newly approved targeted option for TSC-associated seizures with favorable tolerability in pediatric populations.

Beyond currently approved therapies, multiple investigational approaches are advancing through clinical development, including novel combination strategies targeting both mTOR and NF-κB pathways, preventive mTOR inhibitor regimens in asymptomatic infants, and neuroactive steroid modulators of GABA receptors. The challenge for future clinical practice involves optimizing patient selection for specific interventions, developing biomarkers to predict treatment response, and integrating disease-modifying targeted therapies with symptomatic antiepileptic drugs and management of TSC-associated neuropsychiatric disorders to achieve comprehensive improvement in quality of life and neurodevelopmental outcomes. Earlier recognition and treatment of TSC, particularly during critical neurodevelopmental windows before irreversible damage occurs, represents a major priority identified across recent clinical trial findings and expert consensus guidelines. Continued basic research into TSC pathophysiology, coupled with ongoing clinical trials evaluating novel therapeutic combinations and preventive approaches, will likely yield additional treatment options and refine existing strategies to further improve outcomes for this challenging rare genetic disorder.