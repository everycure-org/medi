---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:43:45.413018'
end_time: '2026-04-04T14:46:09.449474'
duration_seconds: 144.04
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: Beckwith-Wiedemann syndrome
  mondo_id: MONDO:0007534
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 42
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** Beckwith-Wiedemann syndrome
- **MONDO ID:** MONDO:0007534

## Research Objectives

Please provide a comprehensive research report on **drug therapies for Beckwith-Wiedemann syndrome**.
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

# Drug Therapies for Beckwith-Wiedemann Syndrome: A Comprehensive Review of Approved, Investigational, and Repurposing Candidates

Beckwith-Wiedemann syndrome (BWS) is a complex genomic imprinting disorder characterized by overgrowth, neonatal hypoglycemia, abdominal wall defects, macroglossia, and a substantially elevated predisposition to embryonal tumors[1][1]. The therapeutic landscape for BWS encompasses multiple drug classes addressing distinct clinical manifestations of this heterogeneous condition. Current pharmacological management focuses primarily on managing neonatal hypoglycemia caused by hyperinsulinism, preventing and treating associated malignancies through chemotherapy regimens, and emerging targeted therapies aimed at correcting the underlying imprinting defects that drive disease pathogenesis. This comprehensive report synthesizes current knowledge on drug therapies approved for BWS management, investigational agents in clinical development, repurposed medications showing clinical promise, and relevant contraindications and adverse events that clinicians must consider when treating this challenging syndrome.

## Genetic and Molecular Basis for Pharmacological Intervention

### The Imprinting Defects Underlying BWS and Drug Targeting Strategy

Beckwith-Wiedemann syndrome results from dysregulation of the chromosome 11p15 imprinted region, which contains two critical imprinting control regions (ICR1 and ICR2) that regulate the expression of key growth-regulating genes including *H19*, *IGF2*, *KCNQ1*, and *CDKN1C*[1][4]. The molecular pathology encompasses several distinct mechanisms that inform therapeutic strategies. The telomeric domain is controlled by the *H19/IGF2* intergenic differentially methylated region (also known as imprinting control region 1, or IC1), and this region includes shared enhancers of *H19* and *IGF2* as well as a CTCF binding factor-dependent insulator located between the two genes[1]. In normal development, CTCF binds to the imprinting control region on the maternal allele to produce an insulator that results in expression of *H19* and silencing of *IGF2*, while on the paternal allele, methylation of the ICR prevents CTCF binding, leading to *IGF2* expression and silencing of the *H19* promoter[1][1].

The centromeric domain is controlled by the *KCNQ1OT1* transcription start site differentially methylated region (known as imprinting control region 2, or IC2), and this region is located on the 5′ end of *KCNQ1OT1* and includes the promoter region for *KCNQ1*[1]. The molecular defects in BWS can be categorized into several types: gain of methylation at IC1 on the maternal allele (IC1 GOM) found in approximately 5–10 percent of patients, loss of methylation at IC2 on the maternal allele (IC2 LOM) found in approximately 50 percent of patients, paternal uniparental disomy of chromosome 11 (pUPD11) which shows both IC1 GOM and IC2 LOM, copy number variations, and mutations in *CDKN1C*[1][1]. These underlying molecular mechanisms directly inform the development of targeted drug therapies, particularly those aimed at normalizing expression of growth factors like IGF2 or modulating downstream signaling pathways.

## Approved Drug Therapies for Beckwith-Wiedemann Syndrome

### Diazoxide for Neonatal Hypoglycemia and Hyperinsulinism

Diazoxide represents the first-line and currently the only FDA-approved medication specifically designated for the treatment of hyperinsulinism in children[7]. Approximately 50 percent of patients with Beckwith-Wiedemann syndrome develop neonatal hypoglycemia, typically driven by pathologic hyperinsulinism that occurs in infants who cannot appropriately suppress insulin secretion in response to low glucose levels[1][6][20]. Diazoxide functions as an adenosine triphosphate-sensitive potassium channel (KATP) opener that acts on pancreatic beta cells to inhibit insulin secretion through hyperpolarization of the cell membrane and attenuation of glucose-stimulated insulin exocytosis[39].

The clinical efficacy of diazoxide in BWS-associated hyperinsulinism is well-established through multiple clinical observations. A randomized clinical trial examining diazoxide treatment in neonatal hypoglycemia demonstrated that while early treatment with low-dose oral diazoxide did not reduce time to initial resolution of hypoglycemia as traditionally defined, it did significantly reduce time to enteral bolus feeding without intravenous fluids, duration of hypoglycemia, and frequency of blood glucose testing compared with placebo[39]. More importantly, only 2 newborns (6 percent) treated with diazoxide had hypoglycemia after the loading dose compared with 20 (53 percent) with placebo, demonstrating substantial clinical benefit[39]. A recent case report documented successful treatment of a male neonate with clinical Beckwith-Wiedemann syndrome presenting with neonatal hypoglycemia and multiple ear pits, where low-dose diazoxide (6 mg/kg/day) combined with chlorothiazide (10 mg/kg/day) resolved all episodes of postprandial hypoglycemia within 48 hours[27].

The typical dosing regimen for diazoxide in hyperinsulinism involves administration at 5-15 mg/kg/day divided into two or three doses, with treatment typically starting within the first hours or days of life when hypoglycemia is detected[1]. Common adverse effects include fluid retention, which can be managed with concurrent thiazide diuretic therapy, hypertrichosis particularly affecting the face and trunk which typically resolves after discontinuation, and occasional hyperglycemia if the dose is excessive[1][6]. In cases where diazoxide proves ineffective or patients develop unacceptable adverse effects such as pulmonary edema or severe hypertrichosis, alternative medical therapies must be considered[6].

### Somatostatin Analogs: Octreotide and Lanreotide

When diazoxide proves insufficient to control hypoglycemia or patients develop intolerable adverse effects, somatostatin analog therapy represents the established second-line medical treatment for hyperinsulinism in children with Beckwith-Wiedemann syndrome[1][7]. Somatostatin analogs suppress insulin secretion from pancreatic beta cells, thereby reducing the hyperinsulinism that drives severe hypoglycemia[7]. Both short-acting octreotide and long-acting formulations including depot octreotide (long-acting release, or LAR) and lanreotide have been utilized clinically with documented success.

Octreotide, a synthetic analog of somatostatin approved by the FDA for various indications including acromegaly and carcinoid syndrome, has been extensively studied in congenital hyperinsulinism and Beckwith-Wiedemann syndrome patients with refractory hypoglycemia. Octreotide has been used since 1986, when it was first reported in a newborn with severe hyperinsulinistic hypoglycemia and seizures where intravenous infusion (gradually increasing the dose from 2 to 50 µg/24 hours) dramatically reduced circulating insulin levels and stabilized blood glucose levels[7]. The medication can be administered as continuous intravenous infusion, subcutaneous pump therapy, or multiple daily subcutaneous injections, with initial doses typically ranging from 5-15 µg/kg/day[1].

A particularly compelling application of octreotide involves long-acting formulations that offer improved quality of life and compliance. Two cases of Beckwith-Wiedemann syndrome with severe hypoglycemia demonstrated successful treatment with long-acting octreotide (LAR), administered as a monthly intramuscular injection[6]. Both patients treated with LAR for over two years achieved euglycemia above 70 mg/dL and had normal height gain without side effects[6]. The first patient was born at 37 weeks and developed hypoglycemia shortly after birth, initially started on diazoxide but developed pulmonary congestion and was therefore switched to depot octreotide, maintaining euglycemia with this therapy[6]. The second patient was born prematurely at 26 and 4/7 weeks with delayed onset of hypoglycemia until 11 weeks of age due to hydrocortisone administration (indicated for hemodynamic instability) and continuous feeding, was initially partially responsive to diazoxide but experienced severe hypertrichosis necessitating a switch to LAR with excellent response[6].

Lanreotide acetate, another long-acting somatostatin analog administered by deep subcutaneous injection of 30 mg once monthly, has similarly demonstrated safety and efficacy in children with congenital hyperinsulinism[28]. Two children presenting with hypoglycemia 30 minutes after birth were initially treated with diazoxide, hydrochlorothiazide, frequent feedings, and octreotide via insulin pump, achieving normoglycemia with good growth rate and normal weight gain[28]. When treated with lanreotide acetate, octreotide infusion was gradually weaned over one month, continuous glucose monitoring after discontinuation of pump therapy showed normoglycemia, and the first patient has been treated with lanreotide acetate for over 5 years with excellent tolerability[28].

However, somatostatin analogs carry important adverse effects requiring careful monitoring and consideration. A dose-dependent reduction in splanchnic blood flow represents a recognized serious complication, and necrotizing enterocolitis (NEC) has been reported within the first few weeks of initiating predominantly high-dose octreotide therapy[7][37]. All reported cases of NEC associated with octreotide use in infants with hyperinsulinism occurred in neonates younger than 1 month of age and within 15 days of commencing octreotide therapy, at doses of 15–27 µg/kg per day[7]. However, a case of late-onset NEC in a patient with Beckwith-Wiedemann syndrome and hyperinsulinism who was treated with a relatively low dose of octreotide (8 µg/kg/day) has also been reported, occurring 2 months after beginning therapy[37]. This case highlights that NEC can occur later and at lower doses than previously described, and practitioners should maintain heightened awareness of this serious complication[37]. Somatostatin analogs should therefore be used with extreme caution in the neonatal period and only when the potential benefits justify the risks[7].

Additional adverse effects of chronic somatostatin analog therapy include all patients receiving long-term therapy experiencing some decrease in linear growth and two patients having subnormal plasma concentrations of insulin-like growth factor I (IGF-I) and insulin-like growth factor binding protein 3 compatible with suppression of growth hormone by octreotide[34]. Resistance to octreotide therapy, even with increasing doses, occurred in all patients in one series receiving long-term treatment, suggesting that while effective initially, some patients may develop tolerance over time[34].

### Surgical Intervention: Pancreatectomy for Refractory Hyperinsulinism

While surgical intervention cannot be classified as drug therapy, partial or subtotal pancreatectomy represents an established treatment option for hyperinsulinism in BWS that is refractory to maximal medical management with diazoxide and somatostatin analogs[1][20]. Approximately 50 percent of patients with Beckwith-Wiedemann syndrome develop neonatal hypoglycemia, but the vast majority of these cases are mild and medically manageable[1]. However, approximately 4 percent of BWS patients have hypoglycemia that extends beyond one month of age and requires intensive medical management and even surgery[20].

A case series of four Beckwith-Wiedemann syndrome patients with severe persistent hyperinsulinism that was refractory to diazoxide and octreotide therapy underwent pancreatectomy following 18-F-DOPA PET/CT imaging that showed either diffuse uptake throughout an enlarged pancreas (three patients) or a large area of focal uptake in the pancreatic body (one patient)[20]. All patients had hypoglycemia since birth that did not respond to medical management with diazoxide or octreotide, and required glucose infusion rates of up to 30 mg/kg/min[20]. Pathologic analysis revealed marked diffuse endocrine proliferation throughout the pancreas that occupied up to 80 percent of the parenchyma with scattered islet cell nucleomegaly, and one patient had a small pancreatoblastoma in the pancreatectomy specimen[20]. The hyperinsulinism improved in all cases after pancreatectomy, with patients being able to fast safely for more than 8 hours[20].

The degree of pancreatectomy performed varied among patients. One patient who underwent an 85 percent pancreatectomy was weaned off intravenous glucose infusion within 10 days and fasted safely for 18 hours at 12 days after the operation[20]. Another patient who underwent a 95 percent pancreatectomy was able to wean the rate of glucose intake over the course of several months and fasted safely for 10 hours at 1 year of age[20]. Importantly, at long-term follow-up, no patient developed diabetes mellitus or had evidence of pancreatic exocrine insufficiency[20]. However, the hyperinsulinism in BWS tends to improve with time, with and without surgery, even in those cases that are severe and prolonged, which represents an argument against performing near-total pancreatectomy in BWS patients with severe hypoglycemia[20].

## Investigational and Pipeline Therapies

### IGF2 Pathway Inhibition: Picropodophyllin and IGF-1 Receptor Antagonists

The most mechanistically targeted investigational approach to treating Beckwith-Wiedemann syndrome involves correction of the IGF2 overexpression that drives the overgrowth phenotype through pharmacological inhibition of IGF-1 receptor (IGF-1R) signaling. Picropodophyllin (PPP) is an IGF-1R kinase inhibitor that has demonstrated proof-of-concept efficacy in mouse models of BWS. This approach is based on the understanding that one-third of BWS cases and two-thirds of Silver-Russell syndrome (SRS) cases are consistent with misexpression of insulin-like growth factor 2 (IGF2), an important facilitator of fetal growth[8][8].

Preclinical research demonstrated that genetically normalizing IGF2 levels in a double rescue experiment corrected the fetal overgrowth phenotype in a BWS mouse model, and pharmacologically, the BWS growth phenotype was rescued by reducing IGF2 signaling during late gestation[8][8]. Researchers treated CTCFm/+ and wild-type fetuses in utero with PPP (20 mg/kg) for 2 days (starting on gestational day 16.5) or 5 days (starting on gestational day 13.5) and measured the weight of the fetuses and placentas at gestational day 18.5[8][8]. The weight of CTCFm/+ fetuses was significantly reduced after both 2 and 5 days of PPP treatment, with the 5-day treatment showing greater reduction[8]. The mechanism of action involves IGF2 binding to IGF-1R and inducing autophosphorylation, which activates the Ras-mitogen-activated protein kinase (MAPK) and phosphatidylinositol 3-kinase (PI3K) signaling pathways[8][8].

To confirm the mechanism of how PPP treatment rescues BWS overgrowth, researchers tested the effects of PPP on IGF-1R signaling in treated fetuses and found an increased level of phospho-IGF1R in the CTCFm/+ compared with wild-type fetal kidneys[8]. The level of phospho-IGF1R was substantially reduced in the PPP-treated CTCFm/+ samples compared with control CTCFm/+ samples, and reductions were also observed in phospho-MAPK (ERK1/2) and phospho-AKT in the PPP-treated samples[8]. These findings support the proposal that CTCFm/+ overgrowth in the kidney is mediated by the IGF2-IGF1R axis and that PPP corrects the overgrowth by interfering with IGF2 signaling[8].

Importantly, no adverse effects of PPP treatment were detected on the pregnant mothers, as animals were monitored daily with no behavioral or physical abnormalities observed during and after treatment, all PPP-treated mothers successfully nursed and groomed pups until weaning, and weight gain of PPP-treated pregnant females was similar to control mothers[8]. This animal study encouraging clinical investigations to target IGF2 for prenatal diagnosis and prenatal prevention in human BWS and SRS[8][8]. However, cases with normal IGF2 levels would not respond to IGF2 therapy even if they displayed specific molecular diagnostic features of BWS, such as those positive for ICR2 epigenotype only[8].

Linsitinib (OSI-906) represents another investigational IGF-1R inhibitor that is a dual inhibitor of IGF-1R and insulin receptor (IR) tyrosine kinase activity. A phase Ib study examined linsitinib in combination with everolimus (an mTOR inhibitor) as treatment for patients with refractory metastatic colorectal cancer[14]. The maximum tolerated dose was determined to be OSI-906 50 mg twice daily and everolimus 5 mg once daily, although no clinical activity was observed in the refractory colorectal cancer patients[14]. While this particular trial did not achieve its clinical objectives, the safety profile established may inform future applications in other conditions including potentially BWS-associated overgrowth, though such use remains investigational.

### mTOR Inhibitors: Everolimus and Sirolimus

The mammalian target of rapamycin (mTOR) represents a central node in the PI3K-AKT-mTOR pathway that is dysregulated in multiple overgrowth syndromes including some manifestations of Beckwith-Wiedemann syndrome[12][30]. Everolimus is an oral mTOR inhibitor that has been studied in various malignancies and has demonstrated activity in relapsed/refractory lymphomas and may have potential applications in BWS-associated tumors. A phase II trial of single-agent everolimus in patients with relapsed Hodgkin lymphoma (HL) demonstrated an overall response rate of 47 percent (9 of 19; 95 percent confidence interval 24-71 percent) with 8 partial responses and 1 complete response[12]. The rationale to test mTOR inhibitors in hematologic malignancies is based on studies demonstrating activation of the PI3K pathway in these tumors[12].

Sirolimus (also known as rapamycin) is the parent drug of the class of mTOR inhibitors and was originally approved as an oral immunosuppressant to prevent acute rejection in solid organ transplantation in 1999[12]. Sirolimus has been explored for treatment of PIK3CA-related overgrowth spectrum (PROS) and other overgrowth syndromes[17][30]. In a clinical series examining sirolimus for CLOVES syndrome (a PIK3CA-related overgrowth disorder), 93 percent of patients reported improvement in quality of life, 86 percent had improvement in one symptom, and 89 percent had improvement in D-Dimer or Fibrinogen levels, though some patients experienced hematologic changes, infections, and liver toxicity[30]. These mTOR inhibitors represent promising approaches for future investigation in BWS, particularly for managing overgrowth manifestations and potentially preventing or treating associated malignancies, though clinical trials specifically in BWS remain limited.

### Glucagon-Like Peptide-1 Receptor Agonists

Recently updated clinical guidance for Beckwith-Wiedemann syndrome management mentions that newer medications for hyperinsulinism management may include glucagon-like peptide-1 (GLP-1) receptor agonists or mTOR inhibitors[2][4]. GLP-1 receptor agonists represent an emerging class with insulin-sensitizing properties and potential utility in hyperinsulinism-related hypoglycemia. While most clinical evidence for GLP-1 receptor agonists has focused on their role in managing polycystic ovary syndrome and glucose metabolism, a meta-analysis comparing GLP-1 receptor agonists with metformin in polycystic ovary syndrome demonstrated that compared with metformin, GLP-1 receptor agonists were more effective in improving insulin sensitivity (standard mean difference -0.40, 95 percent confidence interval -0.74 to -0.06, P = 0.02) and reducing body mass index (SMD -1.02, 95 percent confidence interval -1.85 to -0.19, P = 0.02)[11]. However, GLP-1 receptor agonists were associated with a higher incidence of nausea and headache than metformin[11]. The application of GLP-1 receptor agonists specifically in BWS-associated hyperinsulinism remains investigational and requires further study.

## Chemotherapy Regimens for BWS-Associated Malignancies

### Treatment of Wilms Tumor in Beckwith-Wiedemann Syndrome

Children with Beckwith-Wiedemann syndrome face substantially elevated risk of developing Wilms tumor, with approximately 15 percent of children with BWS developing bilateral tumors[25]. Wilms tumors in BWS individuals exhibit distinct characteristics from those of sporadic Wilms tumors, and the management of these patients requires a peculiar approach emphasizing nephron preservation[9]. The most important feature is the higher risk of developing bilateral disease at some time during the course of illness, including synchronous bilateral disease at diagnosis or metachronous recurrence after initial presentation with unilateral disease[9].

The established approach for Wilms tumor in BWS patients involves neoadjuvant chemotherapy to facilitate nephron-sparing surgical approaches[9]. Neoadjuvant chemotherapy for patients with bilateral disease or for those with unilateral Wilms tumor but bilaterally predisposed disorders such as BWS is the approach recommended by both the Società Italiana di Oncologia Pediatrica (SIOP) and the Children's Oncology Group (COG)[9]. One major goal of preoperative chemotherapy, even in the presence of monolateral tumor, is to obtain tumor shrinkage to maximize nephron-sparing surgery opportunities[9]. The possibility of contralateral metachronous tumor must be considered, and avoiding radical nephrectomy must be prioritized while guaranteeing surgical oncological outcome[9].

A landmark clinical trial (COG AREN0534) examined prenephrectomy chemotherapy induction in patients with bilateral Wilms tumors and unilateral Wilms tumors with predisposing conditions including Beckwith-Wiedemann syndrome[3]. For patients with bilateral Wilms tumors, the initial induction therapy consisted of three-drug chemotherapy (Regimen VAD: vincristine, dactinomycin, and doxorubicin), with patients evaluated at 6 and 12 weeks for feasibility of undergoing partial nephrectomy[3]. For patients with unilateral Wilms tumor and conditions predisposing to bilateral disease such as Beckwith-Wiedemann syndrome, hemihypertrophy, or other overgrowth syndromes, prenephrectomy 2-drug chemotherapy induction with vincristine and dactinomycin was utilized[3]. This trial aimed to improve 4-year event-free survival to 73 percent for patients with bilateral Wilms tumor and to facilitate partial nephrectomy in lieu of nephrectomy in 25 percent of children with unilateral tumors and BWS by using prenephrectomy 2-drug chemotherapy induction[3].

Several studies reported excellent Wilms tumor response to preoperative chemotherapy in BWS patients. Welter and colleagues documented that BWS patients showed a significant volume reduction of 86.9 percent after neoadjuvant chemotherapy[9]. In contrast, in other patients with Wilms tumor-predisposing syndromes included in the same cohort such as Denys-Drash syndrome (DDS), no real change of tumor volume under preoperative chemotherapy was observed[9]. This finding can be explained by the frequent stromal histology seen in DDS patients, which is the primary reason for failure of response to preoperative chemotherapy[9].

Ehrlich and colleagues reported results of a prospective COG AREN0534 study including 34 patients (9 BWS patients) with multicentric or bilaterally predisposed unilateral Wilms tumor treated with a standardized approach of preoperative chemotherapy to facilitate nephron-sparing surgery in lieu of radical nephrectomy[9]. Overall, pre-nephrectomy chemotherapy allowed nephron-sparing surgery in 65 percent of the patients[9]. The use of preoperative chemotherapy is strongly recommended to maximize the possibility of nephron-sparing surgery, even in the presence of unilateral tumors[9]. Nephron-sparing surgery is particularly preferred for patients with BWS, given their significant chance of having both kidneys involved by Wilms tumor and the fact that around 20 percent of them develop bilateral disease at some time during the course of disease[9].

Adjuvant postoperative treatment guidelines are based on histological types and local stage and follow the same principles as for Wilms tumors in patients without predisposing syndromes. At the end of treatment, in cases of nephroblastomatosis in one or both sides, a maintenance regimen including vincristine and dactinomycin could be indicated to prevent or reduce the incidence of metachronous Wilms tumor[9].

### Treatment of Hepatoblastoma in Beckwith-Wiedemann Syndrome

Children with Beckwith-Wiedemann syndrome face substantially increased risk of hepatoblastoma, with most hepatoblastomas developing by age 2 to 3 years[1][22][22]. Patients with localized and lower stage hepatoblastoma can achieve high survival rates between 80 and 100 percent, but patients with late stage tumors face a poorer prognosis[1]. A comprehensive chemotherapy regimen for hepatoblastoma has been developed and studied extensively. A phase 3 trial examined the combination chemotherapy regimen C5VD (cisplatin, 5-fluorouracil, vincristine, and doxorubicin) in children with initially unresectable hepatoblastoma[35]. The study aimed to test the feasibility and toxicity of the novel therapeutic C5VD regimen with the addition of doxorubicin to the standard C5V regimen for patients considered to be intermediate-risk[35].

One hundred two evaluable patients were enrolled in this study[35]. Delivery of C5VD was feasible and tolerable with a mean percentage of the target dose of cisplatin delivered at 96 percent (95 percent confidence interval 94-97 percent), 5-FU at 96 percent (95 percent confidence interval 94-97 percent), doxorubicin at 95 percent (95 percent confidence interval 93-97 percent), and vincristine at 90 percent (95 percent confidence interval 87-93 percent)[35]. The addition of doxorubicin to the previous standard regimen of C5V was feasible, tolerable, and efficacious, suggesting C5VD as a good regimen for future clinical trials, particularly for patients with unresectable disease at diagnosis[35].

A recent prospective study from 2024-2025 examined a dose-intensified C5VD regimen in 24 children with newly diagnosed locally advanced hepatoblastoma[40]. All 24 patients achieved complete macroscopic resection of hepatic lesions without liver transplantation[40]. Serum alpha-fetoprotein levels decreased significantly after two chemotherapy cycles[40]. During a median follow-up of 38.4 months (range 15.8-50.7 months), all patients maintained continuous complete remission, with 3-year event-free survival and overall survival rates of 100 percent[40]. The incidence rates across 144 chemotherapy cycles of grade 3-4 neutropenia, thrombocytopenia, and infections were 97 percent, 77 percent, and 71 percent respectively, with no treatment-related deaths[40]. Notably, 5 patients (21 percent) developed Brock grade greater than or equal to 3 hearing loss, of whom 1 required a hearing aid[40]. The dose-intensified C5VD regimen demonstrated significant efficacy with an overall favorable safety profile, though grade 3-4 myelosuppression and infection represent predominant toxicities, and high-dose cisplatin-induced ototoxicity remains a concern requiring improved otoprotective strategies[40].

A case report described successful treatment of doxorubicin and cisplatin-resistant hepatoblastoma in a child with Beckwith-Wiedemann syndrome with high-dose acetaminophen and N-acetyl cysteine, though this represents an unusual approach applied outside standard treatment protocols[15][15].

## Drug Repurposing Candidates for Beckwith-Wiedemann Syndrome

### Growth Hormone for Specific BWS Subtypes

Growth hormone therapy has been explored for certain clinical presentations within the Beckwith-Wiedemann spectrum, particularly in cases where growth retardation occurs in the postnatal period due to specific imprinting defects. While Beckwith-Wiedemann syndrome is typically characterized by overgrowth, growth hormone (GH) therapy can theoretically be considered in carefully selected patients where normal IGF2 levels result in growth restriction after birth, a scenario that can occur in certain epigenetic subtypes[8]. The parental conflict hypothesis and imprinting studies suggest that some BWS subgroups may potentially benefit from targeted growth factor modulation, though such use remains highly specialized and investigational[32].

A systematic review examining the risk of neoplasia in pediatric patients receiving growth hormone therapy noted that in children without known risk factors for malignancy, GH therapy can be safely administered without concerns about increased risk for neoplasia[23]. However, GH use in children with medical diagnoses predisposing them to development of malignancies, such as Beckwith-Wiedemann syndrome, should be critically analyzed on an individual basis, and if chosen, appropriate surveillance for malignancies should be undertaken[23]. The report emphasized that GH can be used to treat GH-deficient childhood cancer survivors who are in remission with the understanding that GH therapy may increase their risk for second neoplasms[23].

### Alpelisib and Other PIK3 Inhibitors

Although not specifically approved for Beckwith-Wiedemann syndrome, alpelisib (a phosphatidylinositol 3-kinase [PIK3] inhibitor) and related compounds may have potential applications based on shared mechanistic pathways with other overgrowth syndromes. PIK3CA-related overgrowth spectrum (PROS) and related conditions involve dysregulation of the PI3K-AKT-mTOR pathway[30]. In PROS patients, alpelisib at 50 mg daily has demonstrated rapid positive response with reduction in overgrowth in individual cases[30], though larger clinical trials remain limited. These agents represent candidates for repurposing to BWS patients with specific molecular subtypes involving PI3K pathway dysregulation, though such applications currently remain investigational and require mechanistic confirmation.

### Aspirin for Cancer Prevention in BWS

Recent research examining aspirin's potential in cancer prevention identified multiple mechanisms through which aspirin may reduce cancer risk and progression[19]. While not specifically studied in Beckwith-Wiedemann syndrome, aspirin's multifaceted mechanism of action through cyclooxygenase (COX) enzyme inhibition, leading to decreased prostaglandin E2 (PGE2) levels and disruption of cancer-related signaling pathways including PI3K/AKT and ERK, suggests potential preventive applications in BWS patients with elevated tumor risk[19]. Aspirin inhibits IκB kinase (IKK), preventing NF-κB activation and reducing cell survival signals, and activates AMPK, indirectly inhibiting mTOR signaling critical for cell growth and proliferation[19]. Furthermore, aspirin helps reduce cancer metastasis by inhibiting platelet-tumor cell interactions, enhancing immune surveillance, and suppressing inflammatory and COX-2 pathways[19].

Evidence from colorectal cancer research demonstrates that aspirin use significantly lowers cancer risk, with benefits being substantial for those using aspirin long-term[19]. CRC patients with PIK3CA mutations experienced a significant decrease in mortality when treated with aspirin, whereas no survival benefit was observed in cases with wild-type PIK3CA[19]. These findings highlight the potential of using PIK3CA mutation status to identify patients who could benefit substantially from aspirin therapy[19]. While no published evidence currently demonstrates aspirin's efficacy specifically for cancer prevention in BWS patients, the mechanistic basis and epidemiological evidence from other populations suggests this represents a reasonable repurposing candidate for investigation in future trials.

## Drug Contraindications and Adverse Events in Beckwith-Wiedemann Syndrome

### Medications Associated with Imprinting Defects and BWS Risk

Assisted reproductive technologies (ART), including in vitro fertilization (IVF) and intracytoplasmic sperm injection (ICSI), represent a well-documented risk factor for Beckwith-Wiedemann syndrome through effects on DNA methylation at imprinted loci[1][1]. There is a 10-fold increased risk of BWS with ART and an absolute risk of approximately 1 in 1,100[1]. More than 90 percent of children with BWS conceived by ART have IC2 loss of methylation[1]. Further research is needed to illuminate the relationship between ART and imprinting defects[1]. While ART is not a drug therapy per se, the underlying mechanisms suggest that certain medications affecting methylation or reproductive physiology could theoretically increase BWS risk, though no specific pharmaceutical agents have been identified as contraindicated for this reason.

### Complications of Octreotide and Somatostatin Analogs

As discussed in the approved medications section, octreotide and lanreotide carry important adverse effects requiring careful monitoring in BWS patients. Necrotizing enterocolitis represents a serious, potentially life-threatening complication particularly in the neonatal period and within the first weeks of octreotide initiation[7][37]. While most reported cases of NEC associated with octreotide use in infants with hyperinsulinism occurred in neonates younger than 1 month of age and within 15 days of commencing octreotide therapy at doses of 15–27 µg/kg per day, cases of late-onset NEC at lower doses (8 µg/kg/day) occurring 2 months after beginning therapy have been documented in BWS patients[37]. These treatments should be used with extreme caution in the neonatal period, and somatostatin analogs should only be employed when potential benefits justify risks[7].

Additional considerations include long-term growth suppression effects of chronic somatostatin analog therapy. All patients receiving long-term octreotide therapy experienced some decrease in linear growth, and some patients had subnormal plasma concentrations of insulin-like growth factor I and insulin-like growth factor binding protein 3 compatible with suppression of growth hormone by octreotide[34]. Furthermore, resistance to octreotide therapy, even with increasing doses, occurred in all patients in one series receiving long-term treatment[34].

### Ototoxicity from Cisplatin-Based Chemotherapy

For BWS patients requiring cisplatin-based chemotherapy for hepatoblastoma or other malignancies, cisplatin-induced ototoxicity represents a significant and underappreciated adverse effect requiring surveillance and potentially preventive strategies. In a recent series of 24 children receiving dose-intensified C5VD chemotherapy for hepatoblastoma, 5 patients (21 percent) developed Brock grade greater than or equal to 3 hearing loss, of whom 1 required a hearing aid[40]. High-dose cisplatin-induced ototoxicity remains a concern, highlighting the need for improved otoprotective strategies in these vulnerable pediatric patients[40].

## Combination Therapies and Synergistic Approaches

### Multimodal Treatment of Wilms Tumor in BWS

The optimal management of Wilms tumor in Beckwith-Wiedemann syndrome patients employs a multimodal approach combining neoadjuvant chemotherapy, nephron-sparing surgery, and adjuvant therapy based on tumor response and histological findings. The COG AREN0534 trial established protocols wherein patients with bilateral Wilms tumors receive an initial induction phase of three-drug chemotherapy (vincristine, dactinomycin, and doxorubicin—the VAD regimen) with evaluation at 6 and 12 weeks for feasibility of nephron-sparing surgery, followed at week 12 by definitive surgical intervention and then by post-operative chemotherapy and radiation therapy based on histology and stage, with total treatment continuing for 25 or 31 weeks depending on histology[3][16]. Patients with unilateral Wilms tumors and predisposing conditions such as Beckwith-Wiedemann syndrome, hemihypertrophy, or other overgrowth syndromes receive a modified regimen with 2-drug chemotherapy induction (vincristine and dactinomycin) to facilitate nephron-sparing approaches[3].

This approach represents a sophisticated combination strategy where the fundamental principle is balancing oncological cure against preservation of renal function through preservation of nephrons. The neoadjuvant chemotherapy serves multiple functions: achieving tumor shrinkage to permit nephron-sparing surgery, stratifying adjuvant therapy intensity based on histological response, and reducing risk of tumor rupture during surgery[9]. This combination approach has enabled excellent outcomes in BWS patients with Wilms tumor, with studies documenting nephron-sparing surgery in 65 percent of patients who received preoperative chemotherapy[9].

### Combination Chemotherapy for Hepatoblastoma: C5VD Regimen

The established C5VD combination regimen (cisplatin, 5-fluorouracil, vincristine, and doxorubicin) represents a validated synergistic combination for treatment of pediatric hepatoblastoma[35]. This regimen was systematically studied to assess its feasibility compared with the previously standard C5V regimen (cisplatin, 5-fluorouracil, and vincristine). The addition of doxorubicin to the C5V regimen was demonstrated to be feasible and tolerable with excellent outcomes, including high rates of complete resection and remission maintenance[35][40]. The combination exploits the distinct mechanisms of action of these four chemotherapy agents, with cisplatin providing platinum-based alkylating activity, 5-fluorouracil providing antimetabolite effects through thymidylate synthase inhibition, vincristine providing microtubule disruption, and doxorubicin providing topoisomerase II inhibition and intercalating DNA damage[35].

### Multimodal Hyperinsulinism Management

Management of severe persistent hyperinsulinism in Beckwith-Wiedemann syndrome often involves sequential or combination approaches employing multiple pharmaceutical agents and ultimately surgical intervention[1][20]. The typical progression involves first-line diazoxide therapy, escalation to somatostatin analog therapy if diazoxide fails or adverse effects limit use, and finally surgical pancreatectomy in cases refractory to maximal medical management[1]. This tiered combination approach reflects the severity of potential complications of severe hypoglycemia (including seizures, developmental impairment, and brain damage) against the potential risks and adverse effects of increasingly intensive interventions[20].

## Tumor Surveillance Protocols and Screening Considerations

### Surveillance Recommendations for Hepatoblastoma and Wilms Tumor

Patients with Beckwith-Wiedemann syndrome require systematic surveillance for hepatoblastoma and Wilms tumor, which informs rational use of diagnostic approaches but does not directly involve pharmacotherapy[1][22][36]. However, understanding surveillance protocols contextualizes the role of diagnostic and therapeutic drugs used in detecting and treating these complications. Current recommendations suggest abdominal ultrasounds every 3 months until age 8 years (with some updated guidance recommending risk stratification to potentially reduce screening in lower-risk molecular subtypes), and alpha-fetoprotein (AFP) measurements at intervals ranging from 6 weeks to 3 months until age 4 years[36][42]. Serum AFP levels should be interpreted in the context of the clinical picture, and patients with BWS tend to have higher AFP levels in early childhood compared with normal pediatric values[1].

Risk stratification based on genetic and epigenetic cause of BWS has been proposed to optimize screening protocols. Paternal uniparental disomy of 11p15 (pUPD11) and gain of methylation at imprinting center 1 (IC1) carry higher tumor risks as high as 16 percent and 28 percent respectively, with recommendations for screening every 3 months until age 5 years[42]. Loss of methylation at imprinting center 2 (IC2), which carries only a 2.6 percent risk of tumor formation, may not require intensive surveillance in some risk-stratification approaches[42]. These findings have important implications for the rational use of imaging and laboratory resources but do not directly modify pharmacological management.

## Emerging and Future Directions in BWS Pharmacotherapy

### Prenatal Intervention and Prevention

One of the most promising emerging approaches involves prenatal diagnosis and intervention targeting the underlying imprinting defects responsible for Beckwith-Wiedemann syndrome. Research using mouse models derived proof-of-principle evidence showing that IGF2-based diagnosis and therapy at the fetal stage could prevent the overgrowth symptoms of BWS and the growth retardation of Silver-Russell syndrome[8][8]. Pharmacological rescue of the BWS growth phenotype was achieved by reducing IGF2 signaling during late gestation using picropodophyllin in experimental systems[8][8].

These findings collectively suggest that IGF2-dependent BWS and SRS cases can be identified by prenatal diagnosis and can be prevented by prenatal intervention targeting IGF2[8]. However, such approaches remain investigational and would require substantial further development to reach clinical application, including determining appropriate timing of intervention, optimal dosing, potential off-target effects, and long-term safety profile in both fetuses and mothers. Additionally, cases with normal IGF2 levels would not respond to IGF2 therapy even if they displayed specific molecular diagnostic features of BWS[8].

### Importance of Comprehensive Multidisciplinary Approach

Recent reviews emphasize that management of Beckwith-Wiedemann spectrum has progressed significantly, with advancements in molecular diagnostics enhancing understanding of genotype-phenotype relationships and enabling improved personalized therapeutic approaches[10]. International guidelines have been developed to optimize diagnosis, clinical surveillance, and therapeutic interventions, aiming to improve quality of life and patient outcomes[10]. New findings on cognitive, behavioral, and psychosocial aspects of BWS highlight the need for routine screening and supportive care[10]. The transition to adult care for BWS patients remains underexplored, though recommendations include attention to residual pediatric complications, fertility, and potential long-term risks[10].

## Conclusion: Current State and Future Perspectives on BWS Pharmacotherapy

The pharmacological management of Beckwith-Wiedemann syndrome encompasses multiple drug classes addressing distinct clinical manifestations of this heterogeneous condition. Diazoxide remains the only FDA-approved medication specifically designated for hyperinsulinism in children and represents first-line therapy for neonatal hypoglycemia associated with BWS, with somatostatin analogs including octreotide and lanreotide serving as established second-line agents for diazoxide-refractory or intolerant patients. Chemotherapy regimens centered on VAD or C5VD combinations represent established multimodal approaches to treating BWS-associated malignancies, particularly Wilms tumor and hepatoblastoma, with excellent oncological outcomes and increasingly preserved renal function through nephron-sparing strategies.

Investigational approaches targeting the underlying molecular pathophysiology show particular promise, including IGF-1R kinase inhibitors such as picropodophyllin that have demonstrated preclinical proof-of-concept for preventing fetal overgrowth in animal models of BWS, and mTOR inhibitors including everolimus and sirolimus that target dysregulated growth signaling pathways. These mechanistically targeted agents may enable future therapeutic interventions that address the root cause of disease rather than merely managing symptomatic manifestations.

Drug repurposing candidates including aspirin for cancer prevention and PIK3 pathway inhibitors for overgrowth management represent additional avenues for future investigation. However, clinicians managing BWS patients must maintain awareness of important adverse effects and contraindications, particularly the risk of necrotizing enterocolitis with somatostatin analog therapy in the neonatal period and cisplatin-induced ototoxicity with chemotherapy regimens.

The future of BWS pharmacotherapy likely involves increasingly personalized, genotype-informed approaches that stratify patients based on their specific imprinting defects and then apply targeted interventions addressing those particular molecular abnormalities. Prenatal diagnosis and intervention strategies targeting IGF2 represent particularly intriguing but still investigational possibilities that may eventually enable prevention of BWS manifestations before birth. As our understanding of the complex imprinting defects driving BWS continues to evolve and new therapeutic modalities are developed, the field moves toward more sophisticated, targeted interventions that may ultimately transform outcomes for patients with this challenging genetic disorder.