---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T10:31:29.617378'
end_time: '2026-04-04T10:34:10.192967'
duration_seconds: 160.58
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: breast-ovarian cancer, familial, susceptibility to, 1
  mondo_id: MONDO:0011450
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
- **Disease Name:** breast-ovarian cancer, familial, susceptibility to, 1
- **MONDO ID:** MONDO:0011450

## Research Objectives

Please provide a comprehensive research report on **drug therapies for breast-ovarian cancer, familial, susceptibility to, 1**.
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

# Comprehensive Drug Therapy Review for Breast-Ovarian Cancer, Familial, Susceptibility to 1 (BRCA1)

This report synthesizes current evidence on pharmacological interventions for hereditary breast and ovarian cancer associated with germline BRCA1 mutations, encompassing FDA-approved therapeutics, investigational compounds in clinical development, drug repurposing opportunities, and critical considerations regarding contraindications, adverse effects, and combinatorial strategies. Recent advances in precision oncology, particularly the development of poly(adenosine diphosphate-ribose) polymerase inhibitors (PARP inhibitors) and targeted therapies, have substantially transformed the treatment landscape for BRCA-mutated malignancies. This report provides oncologists, genetic counselors, and patients with comprehensive guidance on therapeutic options while critically evaluating the strength of evidence supporting each intervention and identifying key gaps in current knowledge.

## Epidemiological Context and Clinical Significance

Germline mutations in the BRCA1 gene confer substantial lifetime cancer risk, with women carrying pathogenic BRCA1 variants facing greater than 60% risk of developing breast cancer and 39% to 58% risk of developing ovarian cancer during their lifetime. These risks exceed the general population baseline of approximately 13% for breast cancer and 1.1% for ovarian cancer. Among women with newly diagnosed breast cancer, approximately 5 to 10% carry pathogenic BRCA1 or BRCA2 mutations[5], emphasizing the clinical importance of accurate risk stratification and mutation testing. The identification of a BRCA1 mutation has profound implications not only for the individual patient but also for treatment selection, family planning decisions, and risk management strategies. Unlike sporadic breast cancers, BRCA1-associated malignancies exhibit distinct pathological features, with approximately 71% of BRCA1-carrier breast cancers being triple-negative (estrogen receptor-negative, progesterone receptor-negative, and HER2-negative)[31], a phenotype historically associated with aggressive clinical behavior and limited therapeutic options.

## Approved Drug Therapies for BRCA1-Associated Malignancies

### PARP Inhibitors: Mechanisms and Clinical Applications

The approval of poly(adenosine diphosphate-ribose) polymerase inhibitors represents the most significant therapeutic advance for BRCA-mutated cancers in recent years, establishing the practical application of synthetic lethality—a concept wherein cancer cells deficient in homologous recombination repair become selectively vulnerable to PARP inhibition. PARP enzymes catalyze the addition of ADP-ribose polymers to DNA-damaged proteins, facilitating single-strand break repair through the base excision repair pathway. Inhibition of PARP results in accumulation of single-strand DNA breaks that, during DNA replication, are converted to irreparable double-strand breaks in BRCA1/2-deficient cells[47]. In contrast, normal cells with functional BRCA1/2 proteins can repair these lesions through homologous recombination, creating a therapeutic window of selectivity[7].

#### Olaparib (Lynparza®)

Olaparib was the first PARP inhibitor to achieve FDA approval, establishing the feasibility of the PARP-BRCA synthetic lethality concept in clinical practice. The drug received FDA approval in December 2014 for patients with BRCA1/2-mutated ovarian cancer who had received three or more lines of chemotherapy, followed by approval in January 2018 for metastatic breast cancer with germline BRCA mutations[2][7]. The landmark OlympiAD trial enrolled 302 patients with HER2-negative metastatic breast cancer carrying germline BRCA mutations (including some with triple-negative disease) who had received prior chemotherapy[2]. Patients were randomized to receive olaparib (300 mg twice daily) or standard chemotherapy selected by the treating physician. The trial demonstrated that median progression-free survival with olaparib was 7.0 months compared with 4.2 months for standard therapy, with 59% of olaparib-treated patients showing objective response compared with 28.8% of the control arm[2]. Notably, the rate of grade 3 or higher adverse events was actually lower in the olaparib group (36.6%) compared with chemotherapy (50.5%), though serious adverse effects included hematologic toxicities and the rare but significant emergence of myelodysplastic syndrome and acute myeloid leukemia[2].

Beyond the metastatic setting, olaparib demonstrated efficacy in early-stage disease through the phase III OlympiA trial, which enrolled 1,836 patients with stage II-III HER2-negative breast cancer harboring germline BRCA1/2 mutations. Patients received one year of adjuvant olaparib (300 mg twice daily) or placebo following completion of standard chemotherapy and local treatment. The trial reported a 42% reduction in the relative risk of invasive disease-free survival events (hazard ratio 0.58; p < 0.0001) at all measured timepoints. These findings led to FDA approval of adjuvant olaparib as a standard-of-care option for high-risk BRCA-mutated early breast cancer. For ovarian cancer, the SOLO1 trial demonstrated that olaparib maintenance therapy following first-line platinum-based chemotherapy reduced the risk of disease progression or death by 70% in patients with BRCA-mutated advanced ovarian cancer[12]. At seven-year follow-up, 67.0% of olaparib-treated patients remained alive compared to 46.5% of placebo recipients, and 45.3% versus 20.6% were alive without having received subsequent therapy[40].

#### Talazoparib (Talzenna®)

Talazoparib, a highly potent PARP inhibitor that was approved by the FDA in October 2018, was studied in the phase III EMBRACA trial, which randomized 431 patients with germline BRCA-mutated (gBRCAm), HER2-negative locally advanced or metastatic breast cancer to receive talazoparib (1 mg daily) or physician's choice of chemotherapy[9]. The trial met its primary efficacy endpoint, demonstrating significantly prolonged progression-free survival with talazoparib (estimated median 8.6 months) compared to chemotherapy (5.6 months), with hazard ratio of 0.54 (95% CI: 0.41, 0.71; p < 0.0001)[9]. The most common adverse reactions were fatigue, anemia, nausea, neutropenia, headache, thrombocytopenia, vomiting, and alopecia, occurring in ≥20% of patients[9]. Detailed safety analyses from EMBRACA revealed that the most common treatment-related adverse events with talazoparib were hematologic, typically occurring within the first 3-4 months of therapy, with grade 3-4 anemia lasting approximately 7 days[39]. Importantly, permanent discontinuation of talazoparib due to hematologic adverse events was low (<2% of patients), and adverse events were generally manageable through dose modification and supportive care[39]. The companion diagnostic BRACAnalysis CDx test was simultaneously approved to identify patients eligible for talazoparib therapy[9].

#### Niraparib (Zejula®)

Niraparib received FDA approval in April 2020 for first-line maintenance treatment of advanced epithelial ovarian, fallopian tube, or primary peritoneal cancer in patients with complete or partial response to first-line platinum-based chemotherapy[11]. The phase III PRIMA trial randomized 733 patients to niraparib or placebo, with efficacy testing first conducted in the homologous recombination deficient (HRD) population, followed by overall population analysis[11]. In the HRD population, niraparib significantly prolonged progression-free survival compared to placebo (21.9 months versus 10.4 months; hazard ratio 0.43; p < 0.0001), and maintained benefit in the overall population (13.8 months versus 8.2 months; hazard ratio 0.62; p < 0.0001)[11]. Notably, niraparib was the first PARP inhibitor approved for first-line maintenance treatment that did not require an FDA-approved companion diagnostic test, making it accessible to a broader patient population[11]. However, niraparib-based first-line maintenance did not translate to overall survival benefit in the final analysis, with hazard ratio for overall survival of 1.01 (95% CI: 0.84-1.23; P = 0.8834) in the overall population, though this may reflect subsequent PARP inhibitor therapy received by placebo-arm patients[41]. The dose of niraparib must be adjusted based on patient body weight and baseline platelet count, ranging from 200 to 300 mg once daily[11].

#### Rucaparib (Rubraca®)

Rucaparib received FDA approval in April 2018 for maintenance treatment of recurrent epithelial ovarian, fallopian tube, or primary peritoneal cancer in patients achieving complete or partial response to platinum-based chemotherapy[10]. The phase III ARIEL3 trial enrolled 561 patients with recurrent ovarian cancer who had received at least two prior platinum-based regimens and demonstrated that rucaparib significantly improved progression-free survival in all three analyzed populations: overall patients (median 10.8 versus 5.4 months; HR 0.36; p<0.0001), HRD-positive patients (median 13.6 versus 5.4 months; HR 0.32; p<0.0001), and BRCA-mutated patients (median 16.6 versus 5.4 months; HR 0.23; p<0.0001)[10]. The FoundationFocus CDx BRCA LOH test was concurrently approved to determine HRD status[10]. A subsequent trial evaluated rucaparib as first-line maintenance monotherapy in the ATHENA-MONO trial, and long-term follow-up data from ARIEL3 (with median follow-up of 77.0 months) showed that while overall survival was similar between treatment arms, progression-free survival benefit was maintained through the subsequent therapy line, supporting rucaparib's use in maintenance settings[42].

### Platinum-Based Chemotherapy

BRCA1/2-mutated tumors exhibit exceptional sensitivity to platinum-based chemotherapy agents, including cisplatin and carboplatin, which induce DNA inter-strand crosslinks that cannot be efficiently repaired in cells lacking functional homologous recombination repair[15][15]. This heightened sensitivity forms the mechanistic basis for platinum inclusion in treatment regimens for BRCA-mutated cancers. A meta-analysis of neoadjuvant platinum-based chemotherapy in triple-negative breast cancer (a phenotype heavily enriched for BRCA1 mutations) demonstrated pooled pathologic complete response rates of 48% with platinum-containing regimens compared to 30-40% without platinum agents. In this analysis, carboplatin and cisplatin demonstrated comparable efficacy, with pathologic complete response rates of 47.0% and 47.3%, respectively (no significant difference between agents), though carboplatin has a more favorable toxicity profile in many clinical settings. For BRCA1 carriers, platinum-based neoadjuvant chemotherapy yielded higher pCR rates (61.1%) compared to BRCA2 carriers (12.5%) when administered as part of platinum-containing regimens (p = 0.022)[32], highlighting biological differences between BRCA1 and BRCA2 mutations that influence chemotherapy responsiveness.

### Taxane-Based Chemotherapy

Taxanes (docetaxel and paclitaxel) represent microtubule-stabilizing chemotherapy agents approved for breast cancer treatment that block cell proliferation and induce apoptosis[6]. However, the efficacy of taxanes in BRCA-mutated breast cancer varies by hormone receptor status and presence of other genetic alterations. BRCA1 mutation carriers with hormone-negative breast cancers demonstrated less sensitivity to taxane chemotherapy compared to non-BRCA1 carriers with hormone-negative disease[6]. In contrast, BRCA1 and BRCA2 carriers with hormone-positive cancers showed similar taxane sensitivity to sporadic cases[6]. An anthracycline-taxane combination approach for neoadjuvant chemotherapy achieved 46% pathologic complete response in BRCA1 mutation carriers, compared to 22% in sporadic breast cancer patients[6], suggesting that BRCA-mutated tumors remain responsive to combination chemotherapy despite differential taxane sensitivity in hormone-negative disease. A recent retrospective analysis of HER2-negative early breast cancer patients undergoing neoadjuvant chemotherapy found that platinum-containing regimens provided greater benefit to BRCA1 carriers than BRCA2 carriers, underscoring the importance of considering specific BRCA mutation status when designing chemotherapy regimens[32].

### Hormone Therapies and Endocrine Agents

Tamoxifen, a selective estrogen receptor modulator approved for hormone receptor-positive breast cancer treatment, has demonstrated chemopreventive efficacy in BRCA1/2 mutation carriers. A large study of 1,504 patients with germline BRCA1 or BRCA2 mutations showed a 50% reduction in contralateral breast cancer risk when tamoxifen was used as adjuvant therapy[6]. In a subsequent analysis of duration-response relationships, short-term tamoxifen use (less than one year) demonstrated protective efficacy comparable to or exceeding conventional five-year treatment courses[14]. The univariate odds ratio for tamoxifen use and contralateral breast cancer was 0.52 (95% CI: 0.37 to 0.73) for BRCA carriers combined, with protective effects similar between BRCA1 (univariate odds ratio 0.58; 95% CI: 0.39 to 0.86) and BRCA2 (odds ratio 0.36; 95% CI: 0.17 to 0.75) carriers[14]. These findings suggest that short-term tamoxifen use may be as effective as prolonged therapy for contralateral breast cancer prevention in BRCA-mutated carriers. However, tamoxifen carries risks of thromboembolic events, cataracts, and endometrial cancer that must be weighed against preventive benefits.

Cyclin-dependent kinase 4/6 inhibitors (palbociclib, ribociclib, abemaciclib) are approved for hormone receptor-positive, HER2-negative metastatic breast cancer in combination with endocrine therapy[50]. However, emerging evidence suggests that germline BRCA mutations may predict inferior outcomes with CDK4/6 inhibitor-based therapy. A retrospective analysis of HR+/HER2- metastatic breast cancer patients treated with CDK4/6 inhibitors plus endocrine therapy found that carriers of germline pathogenic variants in DNA repair genes (including BRCA1, BRCA2, ATM, and CHEK2) had significantly shorter median progression-free survival compared to wild-type patients[50]. An exploratory analysis from the phase III PADA-1 trial reported that patients with pathogenic germline variants in BRCA1, BRCA2, or PALB2 had significantly shorter median progression-free survival with aromatase inhibitor plus palbociclib (14.3 months versus 26.7 months in gBRCAwt patients; HR = 0.58; P = 0.056)[50], suggesting reduced CDK4/6 inhibitor efficacy in these subgroups. This biological observation has prompted recommendations that PARP inhibitors be prioritized in high-risk early-stage and metastatic settings for BRCA-mutated patients, with CDK4/6 inhibitors reserved for selected cases.

### HER2-Targeted Therapies

While BRCA1-associated breast cancers are typically HER2-negative, some BRCA-associated tumors and a subset of BRCA-wild-type ovarian cancers express HER2. Fam-trastuzumab deruxtecan-nxki (Enhertu), an antibody-drug conjugate consisting of a HER2 monoclonal antibody linked to topoisomerase I inhibitor payloads, received FDA approval in December 2025 for first-line treatment of adults with unresectable or metastatic HER2-positive (IHC 3+ or ISH+) breast cancer in combination with pertuzumab[24]. The DESTINY-Breast09 trial (N=1,157) demonstrated superior progression-free survival with fam-trastuzumab deruxtecan-nxki plus pertuzumab compared to standard taxane-based therapy (40.7 months versus 26.9 months; HR 0.56; 95% CI: 0.44, 0.71; p < 0.0001), with confirmed objective response rates of 87% and 81%, respectively[24]. In ovarian cancer, the DESTINY-Ovarian01 phase 3 trial (initiated in December 2025) is evaluating fam-trastuzumab deruxtecan-nxki in combination with bevacizumab as first-line maintenance therapy in patients with HER2-expressing advanced ovarian cancer, as HER2 expression occurs in up to 55% of ovarian cancers and is associated with advanced stages, higher recurrence frequency, and shorter survival[16].

### Folate Receptor-Alpha-Targeted Therapy

Mirvetuximab soravtansine-gynx (Elahere), an antibody-drug conjugate targeting folate receptor-alpha (FRα), received FDA regular approval in March 2024 for adult patients with FRα-positive, platinum-resistant epithelial ovarian, fallopian tube, or primary peritoneal cancer who have received one to three prior systemic treatment regimens[25]. The MIRASOL trial (Study 0416) randomized 453 patients with platinum-resistant ovarian cancer to receive mirvetuximab soravtansine-gynx (6 mg/kg intravenously every 3 weeks) or investigator's choice chemotherapy (paclitaxel, pegylated liposomal doxorubicin, or topotecan)[25]. Median overall survival was 16.5 months (95% CI: 14.5, 24.6) in the mirvetuximab arm versus 12.7 months (95% CI: 10.9, 14.4) in the chemotherapy arm (HR 0.67; 95% CI: 0.50, 0.88; p = 0.0046), while median progression-free survival was 5.6 months (95% CI: 4.3, 5.9) versus 4.0 months (2.9, 4.5) (HR 0.65; 95% CI: 0.52, 0.81; p < 0.0001)[25]. Over 50% of serous ovarian cancers express high levels of FRα, making this therapy relevant to a substantial proportion of ovarian cancer patients[4]. The prescribing information includes a boxed warning for ocular toxicity, with additional warnings for pneumonitis, peripheral neuropathy, and embryo-fetal toxicity[25].

## Investigational and Pipeline Drugs in Clinical Trials

### PARP Inhibitor Development and New Mechanisms

While four PARP inhibitors have achieved FDA approval (olaparib, talazoparib, niraparib, and rucaparib), several additional PARP inhibitors remain in clinical development, including veliparib (ABT-888) and atrimustine (AZD2461)[6]. Veliparib has been evaluated in phase I/II trials in combination with platinum-based chemotherapy and topoisomerase inhibitors[46], with a phase II trial of single-agent veliparib demonstrating an overall survival benefit exceeding 26 months in patients with BRCA-mutated ovarian cancer, though this agent has not yet achieved FDA approval[46]. These investigational agents may offer distinct pharmacological properties, improved tolerability profiles, or enhanced efficacy in specific patient subgroups.

Recent mechanistic insights have challenged the prevailing model of PARP inhibitor action. A landmark study published in Nature by Petropoulous et al. demonstrated that PARP inhibitor-mediated synthetic lethality with BRCA-deficient cells results predominantly from transcription-replication conflicts (TRC) rather than from PARP trapping on DNA. In this model, PARP1 normally detects transcription-replication conflicts through direct interaction with TIMELESS, preventing collisions between replication and transcription machineries. When PARP1 is inhibited or catalytically blocked, unresolved transcription-replication conflicts accumulate, leading to DNA damage that cannot be repaired in homologous recombination-deficient cells. These findings have important implications for PARP inhibitor design, suggesting that future inhibitors minimizing PARP trapping while maintaining catalytic inhibition may achieve superior selectivity for HR-deficient cells while reducing toxicity in normal tissues.

### Combination Trials with PARP Inhibitors and Immunotherapy

The MEDIOLA trial evaluated combination of the anti-PD-L1 antibody durvalumab with olaparib in patients with germline BRCA-mutated and BRCA wild-type solid tumors[19]. In the germline BRCA-mutated cohort (n=32) with platinum-sensitive recurrent ovarian cancer, the 12-week disease control rate was 81% with a median progression-free survival of 11.1 months and objective response rate of 71.9%[19]. An exploratory analysis identified that patients with BRCA mutations achieved the highest response rates to PD-1/PD-L1 inhibitor combinations with PARP inhibition, regardless of BRCA1 or BRCA2 mutation status or PD-L1 expression[28]. The TOPACIO trial (Keynote-162) evaluated pembrolizumab combined with niraparib in platinum-resistant recurrent ovarian cancer, finding overall objective response rate of 25% with 68% disease control rate; notably, patients with BRCA mutations achieved 45% objective response rate and 73% disease control rate. These combination studies establish proof-of-concept for synergistic anti-tumor effects combining DNA damage induction (via PARP inhibition) with immune checkpoint blockade, potentially through enhanced tumor immunogenicity triggered by increased genomic instability and altered tumor microenvironment composition[28].

### WEE1 Inhibitor Development

WEE1 is a protein kinase that regulates cell cycle checkpoint control and DNA damage response, and inhibition of WEE1 can render cancer cells vulnerable to DNA-damaging agents including chemotherapy and PARP inhibitors[4]. Adavosertib (AZD1775), a first-in-class selective small-molecule WEE1 inhibitor, has been evaluated in a phase Ib study in patients with advanced solid tumors including ovarian cancer, triple-negative breast cancer, and small-cell lung cancer[27]. In this trial of 80 patients in the expansion phase (NCT02482311), the most common treatment-related adverse events were diarrhea (56.3%), nausea (42.5%), fatigue (36.3%), vomiting (18.8%), and decreased appetite (12.5%), with treatment-related grade ≥3 adverse events in 32.5% and serious adverse events in 10.0%[27]. Adavosertib demonstrated some antitumor activity in this heavily pretreated population[27]. Ongoing phase III trials are evaluating WEE1 inhibition in combination with platinum chemotherapy or PARP inhibitors in recurrent ovarian cancer, with the rationale that WEE1 inhibition suppresses checkpoint recovery, allowing accumulation of replication-derived DNA damage[4].

### KRAS/MEK Pathway Inhibitors in Low-Grade Serous Ovarian Cancer

Avutometinib and defactinib combination therapy represents a novel approach targeting KRAS-mutated low-grade serous ovarian cancer. On May 8, 2025, the FDA granted accelerated approval to the combination of avutometinib (a MEK1/2 inhibitor) and defactinib (a FAK inhibitor) (Avmapki Fakzynja Co-pack, Verastem, Inc.) for adult patients with KRAS-mutated recurrent low-grade serous ovarian cancer who have received prior systemic therapy[26]. The RAMP-201 trial evaluated this combination in 57 patients with measurable KRAS-mutated recurrent low-grade serous ovarian cancer requiring at least one prior systemic therapy including platinum-based regimens[26]. The confirmed objective response rate was 44% (95% CI: 31, 58) with duration of response ranging from 3.3 to 31.1 months[26]. Since KRAS mutations appear in 15% to 54% of low-grade serous ovarian cancers and help tumors grow faster, this precision medicine approach addresses an unmet need in this previously treatment-refractory malignancy[4].

### Investigational Clinical Trials

Multiple investigational agents are actively being studied in clinical trials for BRCA-mutated cancers. The MOMA-313 trial (NCT06545942) is treating advanced cancers with DNA-repair mutations using MOMA-313 alone or in combination with the PARP inhibitor olaparib[3]. The trial examining "A New Targeted Therapy CX-5461 to Treat Advanced Breast, Ovarian, Pancreatic or Prostate Cancer with Inherited or Tumor Mutations" (identified by its NCT number in the clinical trials database) represents another investigational approach[3]. The LUZERN study evaluated niraparib combined with aromatase inhibitors for HR-positive/HER2-negative advanced breast cancer with germline BRCA1/2 mutations or germline BRCA wild-type with homologous recombination deficiency[30]. This phase II trial found promising results with clinical benefit rate of 73% in cohort A (BRCA-mutated patients) and 54% in exploratory cohort B (BRCA wild-type with HRD), with a manageable safety profile[30].

## Drug Repurposing Candidates and Off-Label Uses

### Immunotherapy in BRCA-Associated Triple-Negative Breast Cancer

While not specifically approved for BRCA-mutated cancers, immune checkpoint inhibitors show promise for triple-negative breast cancer, a phenotype enriched for BRCA1 mutations. Pembrolizumab demonstrated beneficial effects when combined with chemotherapy in early-stage triple-negative breast cancer. In the PD-L1-positive population receiving pembrolizumab combined with neoadjuvant chemotherapy, pathologic complete response rates were 69% compared to 49% with placebo, with superior safety profile[28]. Atezolizumab, an anti-PD-L1 antibody, received FDA approval as first immune checkpoint inhibitor for triple-negative breast cancer when combined with nab-paclitaxel for patients with PD-L1-positive advanced disease. The IMPASSION130 trial showed improved event-free survival and pathologic complete response rates with atezolizumab combination therapy in the neoadjuvant setting.

### Circulating Tumor DNA-Guided Therapy Selection

Circulating tumor DNA (ctDNA) analysis, while not itself a drug, represents an emerging precision medicine tool for therapy selection and response monitoring in BRCA-mutated cancers. ctDNA profiling enables early detection of molecular relapse, often months to years before clinical manifestation. In triple-negative breast cancer undergoing neoadjuvant chemotherapy, early ctDNA clearance during therapy was independently associated with pathological complete response (odds ratio = 13.06; 95% CI: 3.54-57.95) and residual cancer burden (odds ratio = 19.00; 95% CI: 4.98-89.06). This liquid biopsy approach could enable real-time therapeutic adjustment in BRCA-mutated cancers, guiding decisions regarding continuation, escalation, or de-escalation of therapy based on ctDNA dynamics.

### Tumor-Agnostic PARP Inhibitor Use

Real-world evidence demonstrates clinical benefit from PARP inhibitors in non-BRCA mutations associated with homologous recombination deficiency. An analysis of off-label PARP inhibitor treatment in solid tumors identified that while tumors lacking BRCA mutations had lower response rates than BRCA-mutated tumors, appreciable benefit was observed in selected cases[36]. Within analyzed mutations (RAD51, FANCA, ATM, ATRX, PALB2, and CHEK2), the longest progression-free survival (48 months) and overall survival (48 months) was observed in a patient with spindle-cell sarcoma harboring BRCA1 amplification along with multiple other genetic alterations[36]. These findings suggest potential for tumor-agnostic PARP inhibitor use in selected patients with homologous recombination pathway defects, though prospective trials are needed to optimize patient selection and define precise indications.

### Fertility Preservation Considerations

For BRCA-mutated individuals seeking fertility preservation, emerging evidence suggests that BRCA mutations may negatively impact ovarian reserve. Some studies indicate that unaffected BRCA mutation carriers have decreased primordial follicle density and lower anti-müllerian hormone (AMH) levels compared to age-matched cancer-free women. However, a multidisciplinary approach is necessary to balance fertility preservation goals with cancer prevention strategies. Preimplantation genetic diagnosis (PGD), which identifies BRCA-mutated embryos during in vitro fertilization, has been advocated by approximately 59% of BRCA carriers surveyed, offering potential to prevent transmission of pathogenic mutations to offspring. The common recommendation is that BRCA carriers should complete family planning before age 35 if desired, reflecting the increased cancer risk with advancing age and the biological reality of declining fertility in this population.

## Contraindications and Drugs to Avoid or Use With Caution

### Estrogen-Containing Hormone Replacement Therapy

While tamoxifen has demonstrated preventive benefits, estrogen-containing hormone replacement therapy is generally contraindicated in BRCA-mutation carriers given the role of estrogen in driving BRCA1-associated breast cancer development. The heightened breast cancer risk in BRCA1/2 carriers appears mediated in part through estrogen signaling pathways, making exogenous estrogen exposure particularly hazardous. Post-menopausal BRCA carriers undergoing risk-reducing oophorectomy for ovarian cancer prevention present a clinical dilemma regarding menopausal symptom management. While hormone replacement therapy is highly recommended for premenopausal women undergoing oophorectomy to ameliorate severe menopausal symptoms and preserve bone health, estrogen-based formulations are contraindicated. Instead, non-hormonal approaches and, in selected cases, progestin-only formulations may be employed, though this remains an area of evolving clinical guidance[33].

### Fertility Drugs in BRCA-Mutated Individuals

Concern has been raised regarding potential increased cancer risk associated with fertility drug use in BRCA-mutation carriers. Women with BRCA mutations exposed to fertility drugs have not been adequately evaluated for ovarian cancer risk, and the combination of hormonal stimulation from fertility drugs and baseline elevated cancer susceptibility represents a potential risk[23]. Most fertility studies have evaluated breast cancer risk rather than ovarian cancer risk in BRCA carriers. A retrospective cohort study of 3,837 women evaluated for infertility found that clomiphene use was associated with a 2.3-fold increased ovarian cancer risk (95% CI, 0.5 to 11.4), based on nine ovarian cancers, with enhanced risk among women receiving ≥12 monthly cycles (RR: 11.1; 95% CI: 1.5 to 82.3)[23]. Though limited data exist specifically in BRCA carriers, these findings warrant careful counseling regarding fertility preservation risks versus benefits when advising BRCA-mutated women considering assisted reproductive technologies.

### CDK4/6 Inhibitors as First-Line Therapy

While not strictly contraindicated, cyclin-dependent kinase 4/6 inhibitors appear less effective in BRCA-mutated hormone receptor-positive breast cancer compared to wild-type patients, leading to recommendations that PARP inhibitors be prioritized instead[50]. The biological basis for reduced CDK4/6 inhibitor efficacy in BRCA-mutated tumors likely reflects altered cell cycle checkpoint regulation consequent to BRCA1 loss, combined with BRCA1's role in suppressing G1/S transition through p21-mediated mechanisms[50]. These mechanistic considerations have led to revised treatment sequencing recommendations, with PARP inhibitors advanced to earlier treatment positions in BRCA-mutated HR-positive disease.

## Adverse Events of Clinical Relevance

### Hematologic Toxicities Associated with PARP Inhibitors

Hematologic toxicities represent the most frequent adverse effects of PARP inhibitors, with all approved agents showing elevated risk. A meta-analysis of 31 randomized controlled trials including over 10,000 solid tumor patients revealed that PARP inhibitor administration significantly increased risk of all-grade anemia (relative risk = 2.15; 95% CI: 1.68-2.76; p < 0.00001), neutropenia (RR = 1.50; 95% CI: 1.21-1.85; p = 0.0002), and thrombocytopenia (RR = 2.59; 95% CI: 1.88-3.58; p < 0.00001)[48]. For grade ≥3 events, the pooled incidence was 25% for anemia, 18.7% for neutropenia, and 11% for thrombocytopenia, with patients on PARP inhibitors having significantly higher risk of grade ≥3 anemia (RR = 5.43; 95% CI: 3.45-8.56; p < 0.00001), neutropenia (RR = 1.70; 95% CI: 1.22-2.37; p = 0.002), and thrombocytopenia (RR = 5.42; 95% CI: 2.83-10.39; p < 0.00001)[48]. In the OlympiAD trial of olaparib in metastatic BRCA-mutated breast cancer, the most common adverse reactions were nausea (58%), anemia (40%), fatigue (37%), vomiting (30%), and neutropenia (27%)[12]. The talazoparib EMBRACA trial demonstrated predominantly hematologic toxicities occurring within the first 3-4 months of therapy, with grade 3-4 anemia lasting approximately 7 days, and permanent discontinuation due to hematologic toxicity occurring in <2% of patients[39].

Management strategies for PARP-inhibitor-related hematologic toxicities typically involve dose modification and supportive care. For grade >3 neutropenia, PARP inhibitors should be interrupted and resumed only if toxicity resolves at a reduced dosage. Platelet transfusions are recommended when platelet counts fall below 10 × 10^9/L, while dose reduction or interruption is advised when platelet counts drop below 50-100 × 10^9/L or if bleeding occurs[48]. Interestingly, subgroup analysis based on treatment duration revealed that patients receiving PARP inhibitors for 12 months or fewer experienced lower risk of all-grade anemia, neutropenia, and grade ≥3 neutropenia[48], suggesting that limiting treatment duration may mitigate hematologic toxicity while potentially maintaining clinical efficacy.

### Myelodysplastic Syndrome and Acute Myeloid Leukemia

A serious but uncommon adverse effect observed across PARP inhibitor classes is the emergence of myelodysplastic syndrome (MDS) and acute myeloid leukemia (AML). The OlympiAD trial reported severe adverse effects including the development of certain blood or bone marrow cancers (MDS/AML) in metastatic breast cancer patients[2]. In the SOLO1 maintenance ovarian cancer trial, while the incidence of MDS and AML remained low during long-term follow-up (7-year follow-up), such events were monitored as serious safety concerns[40]. A meta-analysis of myeloid malignancy risk found that the first-line maintenance olaparib setting showed an increased hazard ratio of 1.96 (95% CI: 1.39-2.8) for MDS/AML compared to control[35]. The mechanisms underlying PARP-inhibitor-associated myeloid malignancies remain incompletely understood but may relate to prolonged PARP inhibition in hematopoietic stem cells, leading to mutagenic events. Given the rare but potentially serious nature of this adverse effect, hematologic monitoring is recommended during and after PARP inhibitor therapy, with baseline and periodic complete blood counts advised.

### Non-Hematologic Toxicities

Beyond hematologic effects, PARP inhibitors commonly cause nausea, fatigue, vomiting, and diarrhea. In the niraparib PRIMA trial, the most common adverse reactions (≥10%) included thrombocytopenia, anemia, nausea, fatigue, neutropenia, constipation, musculoskeletal pain, leukopenia, headache, insomnia, vomiting, dyspnea, decreased appetite, dizziness, cough, hypertension, and acute kidney injury[11]. Mirvetuximab soravtansine-gynx carries a boxed warning for ocular toxicity, with additional warnings for pneumonitis, peripheral neuropathy, and embryo-fetal toxicity[25]. The most common adverse reactions with mirvetuximab (≥20%) include increased liver enzymes, fatigue, blurred vision, nausea, diarrhea, abdominal pain, keratopathy, peripheral neuropathy, and musculoskeletal pain[25].

Tamoxifen-associated adverse effects include menopause-like symptoms (hot flashes, night sweats, vaginal dryness), weight gain, irregular menstruation, leg swelling, nausea, vaginal discharge, and skin rash. More serious but rare tamoxifen adverse effects include blood clots, deep vein thrombosis, strokes, cataracts, and endometrial cancer. A research study testing risks versus benefits of tamoxifen in 788 women found that benefits outweighed risks for 74% of participants but not for 20%, highlighting the importance of individualized risk-benefit assessment.

## Combination Therapies and Synergistic Approaches

### PARP Inhibitors Combined with Bevacizumab

The combination of PARP inhibitors with bevacizumab (anti-VEGF monoclonal antibody) demonstrates superior efficacy compared to PARP inhibitor monotherapy in ovarian cancer. A network meta-analysis of 15 randomized controlled trials involving 6,416 ovarian cancer patients found that combination therapy with PARP inhibitors and anti-angiogenic agents demonstrated superior clinical efficacy over PARP inhibitor monotherapy. Specifically, niraparib combined with bevacizumab ranked first in improving progression-free survival, followed by olaparib with cediranib (pan-VEGFR inhibitor). In terms of safety, no statistically significant difference existed in grade ≥3 adverse events between different PARP inhibitors or when combined with anti-angiogenic agents. The PRIMA trial demonstrated that niraparib as first-line maintenance therapy in newly diagnosed advanced ovarian cancer provided benefit regardless of HRD status, while maintenance olaparib plus bevacizumab (olaparib-bevacizumab combination) provided benefit in patients with newly diagnosed advanced ovarian cancer whose tumors tested positive for homologous recombination deficiency (HRD)[8]. For patients with platinum-sensitive recurrent ovarian cancer previously treated with PARP inhibitors, the NIRVANA-R trial (KGOG 3056) evaluated PARP inhibitor rechallenge combined with bevacizumab. The primary endpoint was met, with 59.1% of 44 patients remaining progression-free at 6 months, with estimated 6-month progression-free survival rate of 68% (95% CI: 55%-85%) and median progression-free survival of 11.5 months (95% CI: 7.9-not reached).

### PARP Inhibitors Combined with Chemotherapy

Combination of PARP inhibitors with chemotherapy, particularly platinum agents and topoisomerase I inhibitors, has been explored to enhance DNA damage and overcome innate or acquired PARP inhibitor resistance. A phase I trial of ABT-888 (veliparib) combined with topotecan demonstrated a mechanistic interaction between PARP inhibition and topoisomerase I-mediated DNA damage. The trial showed greater than 75% reduction in PAR (poly(ADP-ribose)) levels in paired tumor biopsies and documented increases in γH2AX response (a marker of DNA double-strand breaks) in circulating tumor cells and peripheral blood mononuclear cells. While combination chemotherapy with platinum agents or taxanes followed by PARP inhibitor monotherapy remains standard-of-care, ongoing trials are investigating whether concurrent PARP inhibition during chemotherapy enhances clinical benefit in selected patient populations[18].

### PARP Inhibitors Combined with Immune Checkpoint Inhibitors

The rationale for combining PARP inhibitors with immune checkpoint inhibitors derives from the hypothesis that PARP-inhibition-induced DNA damage increases tumor immunogenicity through multiple mechanisms, including enhanced antigen presentation and increased infiltration of cytotoxic T lymphocytes. The MEDIOLA trial of durvalumab (anti-PD-L1) combined with olaparib demonstrated 12-week disease control rate of 81% with median progression-free survival of 11.1 months and objective response rate of 71.9% in germline BRCA-mutated patients with platinum-sensitive recurrent ovarian cancer[19][28]. Similarly, the TOPACIO trial of pembrolizumab (anti-PD-1) combined with niraparib demonstrated overall objective response rate of 25% in all comers and 45% in BRCA-mutated patients with platinum-resistant recurrent ovarian cancer. These combination studies establish proof-of-concept for dual mechanism targeting, combining DNA damage induction with immunotherapy, though prospective randomized trials are needed to establish whether these combinations provide clinically meaningful improvements over standard monotherapies.

### Combination Therapy in Triple-Negative Breast Cancer

For BRCA1-associated triple-negative breast cancer, multiple combination approaches are under investigation. Pembrolizumab combined with chemotherapy showed improved pathologic complete response rates (69% versus 49% with placebo) in the PD-L1-positive population receiving neoadjuvant therapy[28]. Combination of pembrolizumab with niraparib (TOPACIO trial) demonstrated four-fold higher objective response rate in BRCA-mutated patients compared to BRCA wild-type patients (21.6% versus 5.3%), with significantly higher PD-L1 expression in BRCA mutation carriers. These findings underscore the importance of multimodal therapy in triple-negative BRCA-associated breast cancer, which historically represented a treatment challenge due to lack of estrogen or HER2 targeting options, though the emergence of PARP inhibitors and immunotherapy has substantially improved outcomes.

### Sequential PARP Inhibitor Therapy

An emerging strategy involves sequential use of different PARP inhibitors in patients initially responsive but subsequently developing resistance. The NIRVANA-R trial demonstrated that niraparib combined with bevacizumab provided clinical benefit in patients with platinum-sensitive recurrent ovarian cancer who had previously received first-line or earlier-line PARP inhibitor maintenance therapy. The trial enrolled patients who were previously treated with PARP inhibitor maintenance but were not progression-free at study entry. This approach acknowledges that resistance mechanisms to one PARP inhibitor may differ from resistance to another class member, suggesting potential for re-treatment with an alternative PARP inhibitor plus bevacizumab.

## Resistance Mechanisms and Therapeutic Strategies to Overcome Resistance

### Secondary BRCA Mutations Conferring Resistance

The most well-characterized resistance mechanism to PARP inhibitors involves secondary somatic mutations in BRCA1 or BRCA2 that restore protein function. A pivotal study analyzed primary and recurrent BRCA1/2-mutated ovarian carcinomas and found that secondary mutations restoring BRCA1/2 were significantly more common in recurrent carcinomas (13 of 46; 28.3%) than in primary carcinomas (2 of 64; 3.1%; P = 0.0003). Among 26 platinum-resistant recurrent ovarian cancers, 12 (46.2%) harbored secondary mutations restoring BRCA1/2, compared with only 1 of 19 (5.3%) platinum-sensitive recurrent cancers (P = 0.003). These secondary mutations restore wild-type BRCA1/2 protein function, thereby re-enabling homologous recombination repair and conferring resistance to both platinum chemotherapy and PARP inhibitors[49]. Notably, 92% of carcinomas with secondary mutations restoring BRCA1/2 proved to be platinum-resistant, establishing the clinical significance of this resistance mechanism.

### BRCA1 Promoter Hypermethylation Reversal

Methylation of the BRCA1 promoter leads to gene silencing and reduced BRCA1 protein expression, creating a mechanism of homologous recombination deficiency similar to germline BRCA1 mutations and conferring PARP inhibitor sensitivity[7]. However, under therapeutic pressure from PARP inhibitor exposure, the methylation status of the BRCA1 promoter may transition from fully methylated to partially or fully demethylated, leading to re-expression of BRCA1, restoration of homologous recombination repair function, and consequently resistance to PARP inhibitors[7]. This epigenetic mechanism of resistance highlights the dynamic nature of tumor biology and underscores the importance of considering combination therapies or sequential mono-therapies that target complementary pathways.

### 53BP1-Mediated Resistance in BRCA1-Deficient Cells

In BRCA1-deficient cells, p53 binding protein 1 (53BP1) inhibits homologous recombination repair by preventing DNA end-resection. A recent murine study demonstrated that loss of 53BP1 in BRCA1-deficient tumors promotes ATM-dependent DNA end processing, generating single-stranded DNA suitable for homologous recombination repair[7]. This mechanism has been validated using RAD51 focus formation and sister chromatid exchange assays. Therefore, 53BP1 deletion enhances homologous recombination repair capability and reduces sensitivity to PARP inhibitors, ultimately leading to acquired PARP inhibitor resistance[7]. In a genetically engineered mouse model of BRCA1-deficient breast cancer, loss of 53BP1 induced olaparib resistance in vivo, with olaparib-resistant tumors demonstrating restored DNA repair capacity as evidenced by RAD51 focus formation. These findings underscore the complexity of PARP inhibitor resistance and identify potential therapeutic targets (53BP1 inhibition) to overcome resistance.

### ABCB1-Mediated Drug Efflux

The multidrug resistance transporter ABCB1 (encoding P-glycoprotein) confers resistance to certain PARP inhibitors and platinum chemotherapy. Paclitaxel-resistant ovarian cancer cells demonstrating increased ABCB1 expression were cross-resistant to olaparib, doxorubicin, and rucaparib but not to veliparib or AZD2461. Active efflux of paclitaxel, olaparib, doxorubicin, and rucaparib from ABCB1-expressing cells was confirmed in drug-resistant cell lines and in bacteria expressing recombinant P-glycoprotein. This ABCB1-mediated mechanism of paclitaxel and olaparib resistance has important clinical implications, as routine prescription of first-line paclitaxel may significantly limit subsequent PARP inhibitor treatment options in ovarian cancer patients, while selection of alternative PARP inhibitors not transported by P-glycoprotein (such as veliparib) may circumvent this resistance.

### Complex Molecular Resistance Mechanisms

Resistance to PARP inhibitors often involves multiple concurrent mechanisms. PTEN loss, which is associated with BRCA1-mutated breast cancer, has been linked to both sensitivity and resistance to PARP inhibitors depending on the genetic context[49]. Loss of PTEN promotes downregulation of RAD51 in endometrial cancer cell lines, rendering these cells sensitive to PARP inhibition[49]. However, presence of wild-type PTEN also confers PARP inhibitor resistance in some breast cancer models[49]. Additionally, gain-of-function mutations in CCNE1 (which encodes cyclin E1) are commonly associated with homologous recombination proficiency and predicted to confer PARP inhibitor resistance. These observations underscore that PARP inhibitor resistance is multifactorial, often involving combinations of restored DNA repair, altered drug metabolism, modified cell cycle checkpoint regulation, and tumor microenvironmental changes.

## Emerging Biomarkers and Precision Medicine Approaches

### Homologous Recombination Deficiency Testing

Accurate identification of homologous recombination deficiency (HRD) status is essential for selecting patients most likely to benefit from PARP inhibitor therapy. Current diagnostic approaches combine sequencing to detect BRCA1/2 mutations with genome-wide analysis of structural genomic alterations indicative of HRD. The three primary genomic instability markers used for HRD assessment include loss of heterozygosity (LOH), telomeric allelic imbalance (TAI), and large-scale state transitions (LST). A tumor is classified as HRD-positive if it harbors a deleterious mutation in BRCA1/2 or if the genomic instability score (GIS) exceeds a predefined threshold of 42. Available HRD testing assays include FoundationOne CDx (assessing BRCA status and genomic LOH), Caris HRD Status, and AmoyDx HRD Focus panel, with differences in molecular strategies and tumor purity requirements. However, an important limitation is that resistance does not eliminate the HRD genotype; tumors may retain genomic scars indicating HRD positivity upon retesting while acquiring PARP inhibitor resistance.

### Circulating Tumor DNA as Dynamic Biomarker

Circulating tumor DNA profiling represents an emerging precision medicine tool enabling real-time monitoring of PARP inhibitor response in BRCA-mutated cancers. In triple-negative breast cancer patients receiving neoadjuvant chemotherapy, baseline ctDNA fraction <1% correlated with smaller tumor burden and higher pathologic complete response rates. Early ctDNA clearance during therapy predicted both pathological complete response (odds ratio = 13.06; 95% CI: 3.54-57.95) and residual cancer burden classification (odds ratio = 19.00; 95% CI: 4.98-89.06). These findings suggest that ctDNA monitoring could enable real-time therapeutic adjustment in BRCA-mutated cancers, guiding decisions regarding continuation, escalation, or de-escalation of therapy based on treatment response.

### Genetic Testing Recommendations

Germline BRCA1/2 testing is imperative for appropriate patient selection for PARP inhibitor therapy. The National Comprehensive Cancer Network (NCCN) clinical practice guidelines recommend germline BRCA1/2 testing for certain patients with breast, ovarian, pancreatic, and prostate cancer who meet testing criteria[37]. The BRACAnalysis CDx test, an FDA-approved companion diagnostic for PARP inhibitor treatment, provides accurate germline BRCA1/2 results with average turnaround time of 14 days after sample receipt, facilitating timely treatment decisions[34]. Testing typically begins with a relative with known BRCA-related cancer to establish germline mutation status in the family[37].

## Preventive Strategies and Risk Management

### Risk-Reducing Prophylactic Surgery

For healthy BRCA1/2 mutation carriers, risk-reducing mastectomy (RRM) substantially reduces breast cancer incidence and mortality. A Cochrane Database analysis of 39 surveillance studies with 7,384 women after bilateral prophylactic mastectomy demonstrated reduced breast cancer incidence and breast cancer-specific mortality, particularly in BRCA1/2 carriers[33]. A major study by Metcalfe et al. in 2014 demonstrated that among 390 women with family history of early-stage breast cancer who were BRCA1/2 carriers and initially treated with unilateral or bilateral mastectomy, 181 patients who underwent contralateral prophylactic mastectomy had significantly better outcomes compared to those with unilateral surgery[33]. The number needed to treat to prevent one contralateral breast cancer was approximately 6 (favorable ratio)[33].

For ovarian cancer prevention, risk-reducing salpingo-oophorectomy (RRSO) is recommended for BRCA1/2 carriers after family planning is complete, as this procedure can prevent at least 90% of epithelial ovarian cancers[33]. For premenopausal healthy women undergoing RRSO, hormone replacement therapy with non-estrogen formulations is highly recommended to ameliorate menopausal symptoms and preserve bone health[33]. Risk-reducing surgery decisions should incorporate comprehensive counseling regarding cancer risks, surgical morbidity, and quality-of-life implications, with timing optimized based on individual factors including age, cancer history, and family history[33].

### Chemoprevention with Tamoxifen

For BRCA1/2 mutation carriers without prior breast cancer, tamoxifen offers chemoprevention benefits. A large study of 1,504 patients with germline BRCA1 or BRCA2 mutations demonstrated a 50% reduction in contralateral breast cancer risk when tamoxifen was used as adjuvant therapy following treatment of the initial breast cancer[6]. Short-term tamoxifen use (less than one year) demonstrated protective efficacy comparable to conventional five-year treatment courses, suggesting that limited-duration therapy may offer an acceptable risk-benefit balance[14]. However, tamoxifen's association with thromboembolic events and endometrial cancer risk necessitates careful patient selection and individualized risk-benefit assessment.

## Conclusion: Clinical Integration and Future Directions

The therapeutic landscape for hereditary breast and ovarian cancer associated with BRCA1 mutations has undergone remarkable transformation within the past decade, shifting from chemotherapy-centric approaches to precision medicine strategies leveraging the concept of synthetic lethality. Poly(adenosine diphosphate-ribose) polymerase inhibitors—now with four FDA-approved members (olaparib, talazoparib, niraparib, and rucaparib) representing the drug class—have demonstrated substantial clinical benefits across early-stage and advanced-stage disease, as well as both breast cancer and ovarian cancer settings. The approval of olaparib as adjuvant therapy for early-stage BRCA-mutated breast cancer based on the OlympiA trial represents a watershed moment, offering patients with high-risk disease a chance to potentially improve disease-free survival and potentially achieve long-term remission or cure.

Beyond PARP inhibitors, the clinical armamentarium has expanded to include HER2-targeted therapies for HER2-expressing ovarian cancers (fam-trastuzumab deruxtecan-nxki), folate receptor-alpha-directed therapy (mirvetuximab soravtansine-gynx) for platinum-resistant ovarian cancer, targeted KRAS pathway inhibition for KRAS-mutated low-grade serous ovarian cancer, and immune checkpoint inhibitors combined with various DNA-damaging and targeted agents. These advances reflect the recognition that BRCA-mutated cancers, while sharing homologous recombination repair deficiency as a unifying feature, comprise molecularly heterogeneous tumors amenable to multiple therapeutic approaches.

However, substantial challenges remain. Acquired resistance to PARP inhibitors emerges in many patients, mediated through diverse mechanisms including secondary BRCA1/2 mutations restoring protein function, BRCA1 promoter demethylation, 53BP1 loss enabling partial homologous recombination restoration, ABCB1-mediated drug efflux, and complex combinations thereof. Future research must focus on identifying biomarkers predictive of resistance and developing therapeutic strategies to overcome or prevent resistance. Sequential PARP inhibitor therapy combined with bevacizumab, as demonstrated in the NIRVANA-R trial, represents one such approach, though additional strategies targeting resistance mechanisms remain urgently needed.

Additionally, the optimal sequencing of multiple approved therapies remains incompletely defined. For hormone receptor-positive BRCA-mutated breast cancer, the inferior outcomes observed with cyclin-dependent kinase 4/6 inhibitors compared to PARP inhibitors raises questions about optimal treatment ordering and identifies an area where prospective trials are needed to establish evidence-based recommendations. The role of immune checkpoint inhibitors in combination with DNA-damaging therapies shows promise but requires confirmation through randomized controlled trials before routine clinical adoption.

Precision medicine approaches leveraging circulating tumor DNA monitoring, homologous recombination deficiency testing, and emerging biomarkers of immunotherapy response offer potential to further refine therapy selection and enable real-time monitoring of treatment efficacy. Implementation of these precision medicine approaches into routine clinical practice remains challenging due to cost, accessibility, and standardization issues, but represents an important frontier for future clinical research.

Finally, the recognition that BRCA mutations predispose to multiple cancer types (including breast, ovary, pancreas, and prostate cancer in men) necessitates a comprehensive approach to cancer risk management in mutation carriers that extends beyond single-organ surveillance and prevention strategies. Future efforts should focus on developing integrated care models that address the complex medical, psychological, reproductive, and preventive needs of BRCA mutation carriers across the lifespan, from genetic counseling and risk-reducing surgery through chemotherapy and PARP inhibitor therapy.