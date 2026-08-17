---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T07:18:55.231292'
end_time: '2026-04-04T07:21:37.960080'
duration_seconds: 162.73
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: alopecia, androgenetic, 1
  mondo_id: MONDO:0007184
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 52
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** alopecia, androgenetic, 1
- **MONDO ID:** MONDO:0007184

## Research Objectives

Please provide a comprehensive research report on **drug therapies for alopecia, androgenetic, 1**.
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

# Comprehensive Research Report: Drug Therapies for Androgenetic Alopecia (MONDO:0007184)

This report synthesizes current evidence on approved pharmacological treatments, investigational agents, repurposing candidates, and contraindicated medications relevant to androgenetic alopecia (AGA). **Androgenetic alopecia represents the most common form of hair loss, affecting approximately 50 million men and 30 million women in the United States, with prevalence increasing substantially with age and affecting over 80 percent of men by age 70[19].** Current FDA-approved pharmacological options remain limited to two agents for decades—topical minoxidil and oral finasteride—though recent advances in JAK inhibitor development and emerging combination therapy approaches have substantially expanded the therapeutic armamentarium. This report examines the evidence base for these therapies, explores investigational candidates in clinical development, discusses off-label repurposing opportunities, and identifies medications known to cause or exacerbate androgenetic alopecia as adverse effects.

## FDA-Approved Drug Therapies for Androgenetic Alopecia

### Topical Minoxidil: Mechanism, Efficacy, and Clinical Application

**Minoxidil represents the only topical formulation approved by the U.S. Food and Drug Administration for androgenetic alopecia[5].** The drug was originally developed as an oral antihypertensive agent and was serendipitously observed to promote hair growth as a side effect, leading to its eventual FDA approval for topical treatment of male and female pattern hair loss. Minoxidil is available in multiple formulations including 2 percent and 5 percent aqueous solutions, and more recently as a 5 percent foam formulation designed to improve tolerability and compliance[35].

The mechanism of action of minoxidil in treating androgenetic alopecia operates through multiple distinct pathways that distinguish it from other antihypertensive agents and render it uniquely effective for hair restoration. **Minoxidil acts through multiple pathways including functioning as a vasodilator, serving as an anti-inflammatory agent, inducing the Wnt/β-catenin signaling pathway in dermal papilla cells, exerting antiandrogen effects, and affecting the length of the anagen and telogen phases of the hair cycle[35].** The primary mechanism appears to involve potassium channel opening, which allows increased potassium influx into cells and prevents calcium entry, thereby lowering the inhibitory impact of epidermal growth factor on hair development[18]. Furthermore, **minoxidil significantly reduces expression of the interleukin-1 gene in human immortalized keratinocytes, probably through slowing the inflammatory process inherent in androgenetic alopecia[18].** Recent mechanistic studies have identified novel targets through which minoxidil exerts its therapeutic effects: **minoxidil treatment of androgenetic alopecia acts not only on androgenic receptors but also on two new targets, steroid 17-alpha-hydroxylase/17,20 lyase (CYP17A1) and aromatase (CYP19A1), modulating enzymatic and hormonal pathways central to hair follicle dysfunction[18].**

The pharmacokinetic profile of minoxidil demonstrates that **approximately 1.4 percent of topical minoxidil is absorbed through the skin[35].** Importantly, minoxidil functions as a prodrug that must be metabolized by follicular sulfotransferase to its active form, minoxidil sulfate, to exert therapeutic effects[35]. This metabolic requirement has important implications for individual variability in response to treatment: individuals with higher sulfotransferase activity may respond better than patients with lower sulfotransferase activity[35]. This pharmacogenetic consideration may partially explain why some patients experience robust hair regrowth with minoxidil therapy while others demonstrate minimal response.

Clinical efficacy data for topical minoxidil demonstrate consistent hair growth promotion across diverse patient populations. **In a five-year study, 2 percent minoxidil exhibited peak hair growth in males at year one with a decline in subsequent years[35].** Topical minoxidil causes hair regrowth in both frontotemporal and vertex areas, and **the 5 percent solution and foam were not significantly different in efficacy from the 2 percent solution[35].** In landmark clinical trials establishing efficacy, **finasteride treatment improved scalp hair by all evaluation techniques at 1 and 2 years, with clinically significant increases in hair count measured in a 1-inch diameter circular area of balding vertex scalp of 107 and 138 hairs versus placebo at 1 and 2 years respectively[17].**

Cardiovascular safety considerations remain important for practitioners prescribing topical minoxidil despite its topical route of administration. During early studies assessing cardiac effects, **topical minoxidil caused significant increases in left ventricular end-diastolic volume, in cardiac output by 0.751 L/min, and in left ventricular mass by 5 g/m², although blood pressure did not change and minoxidil increased heart rate by 3-5 beats per minute during 6 months of follow-up[33].** The authors concluded that "in healthy subjects short-term use of topical minoxidil is likely not to be detrimental. However, safety needs to be established regarding ischaemic symptoms in patients with coronary artery disease as well as for the possible development of left ventricular hypertrophy in healthy subjects during years of therapy[33]." These findings suggest that while topical minoxidil appears safe for most patients, careful cardiovascular monitoring may be warranted in patients with underlying coronary artery disease or hemodynamic compromise.

### Oral Finasteride: Mechanism, Efficacy, and Safety Profile

**Oral finasteride is widely used for the treatment of androgenetic alopecia, with only two US FDA-approved drugs for the condition—topical minoxidil and oral finasteride[3].** Finasteride received FDA approval for androgenetic alopecia in 1997 following demonstration of efficacy in slowing hair loss and promoting hair regrowth in males with male pattern baldness. The drug functions as a selective inhibitor of the 5-alpha-reductase type 2 enzyme, which converts testosterone to dihydrotestosterone (DHT), the primary androgen implicated in androgenetic alopecia pathogenesis.

**Finasteride, a 5-alpha-reductase type II inhibitor and 4-aza-3-oxosteroid compound, works by competitively inhibiting 5-alpha-reductase type 2, resulting in inhibition of the conversion of testosterone to dihydrotestosterone, markedly suppressing serum DHT levels[6].** The clinical significance of DHT reduction in treating androgenetic alopecia derives from the fundamental pathophysiology of the disease: **pathogenesis of androgenetic alopecia is related to the binding of dihydrotestosterone to androgen receptors located at the hair follicle, with DHT produced by conversion of testosterone using 5-alpha-reductase type 2, an enzyme located in the follicle dermal papilla, and androgenetic alopecia-predisposed dermis exhibiting high levels of DHT and increased expression of androgen receptor[6].**

Classic clinical efficacy trials established the effectiveness of finasteride in slowing hair loss and promoting regrowth. **In men with male pattern hair loss, finasteride 1 mg/day slowed the progression of hair loss and increased hair growth in clinical trials over 2 years[17].** More specifically, **finasteride treatment improved scalp hair by all evaluation techniques at 1 and 2 years at p less than 0.001 versus placebo, with clinically significant increases in hair count of baseline equals 876 hairs—measured in a 1-inch diameter circular area—of 107 and 138 hairs versus placebo at 1 and 2 years respectively[17].** Importantly, **treatment with placebo resulted in progressive hair loss, while patients' self-assessment demonstrated that finasteride treatment slowed hair loss, increased hair growth, and improved appearance of hair, with these improvements corroborated by investigator assessments and assessments of photographs[17].**

The approved dosing regimen for finasteride in androgenetic alopecia differs substantially from higher doses used for benign prostatic hyperplasia. **For male pattern hair loss, adults receive 1 milligram once daily[7].** This lower dose for hair loss appears optimized for efficacy while minimizing systemic exposure and adverse effects compared with the 5 mg daily dose used for prostate hyperplasia.

Sexual and endocrine adverse effects represent the most concerning potential complications of finasteride therapy. **Finasteride increased sexual dysfunction only slightly, but its impact diminished over time[34].** More specifically, common adverse effects include decreased interest in sexual intercourse, inability to have or keep an erection, and loss in sexual ability, desire, drive, or performance[7]. The mechanistic basis for these sexual adverse effects involves the elevation of serum testosterone following 5-alpha-reductase inhibition: **finasteride and dutasteride inhibit 5-alpha-reductase, which converts testosterone to DHT, increasing serum testosterone levels, with excess testosterone then converted into estradiol by aromatase, which can result in gynecomastia[22].**

Importantly, **women who are pregnant or may become pregnant should not handle crushed or broken tablets, as finasteride can be absorbed through the skin and cause birth defects in male babies[7].** This teratogenic potential reflects the critical role of DHT in normal male sexual development. Additionally, **finasteride is significantly teratogenic and has been shown to cause feminization of male fetuses, as well as sexual side effects, depression, headache, nausea, and hot flashes[32].**

Prostate cancer risk represents an additional concern requiring careful patient counseling. **Finasteride will not prevent prostate cancer but may increase your risk of developing high-grade prostate cancer, and this medicine may affect the results of the prostate-specific antigen test, which may be used to detect prostate cancer[7].** Healthcare providers should thoroughly discuss this potential risk with patients before initiating therapy and ensure appropriate baseline PSA assessment.

### Topical and Oral Finasteride: Advancing Bioavailability and Systemic Exposure Reduction

Recent pharmaceutical advances have focused on developing topical formulations of finasteride to achieve localized DHT suppression while minimizing systemic exposure and associated adverse effects. **Currently, only topical minoxidil and oral finasteride are approved by the Food and Drug Administration and the European Medicines Agency for treatment of androgenetic alopecia[6].** However, systematic reviews have documented that **in all studies of topical finasteride, there was significant decrease in the rate of hair loss, increase in total and terminal hair counts, and positive hair growth assessment with topical finasteride[6].** More importantly for safety considerations, **both scalp and plasma DHT significantly decreased with application of topical finasteride; however no changes in serum testosterone were noted[6].** This distinction is clinically significant because it suggests that topical finasteride may achieve therapeutic efficacy through local DHT suppression without the systemic hormonal perturbations associated with oral finasteride therapy.

**This study provides the first evidence that topical finasteride slows androgenetic alopecia-associated hair loss by modulating DHT levels, while also reducing systemic exposure to the medication[6].** Animal studies have supported this approach: **comparing topical finasteride 2 percent solution to a fern extract in a testosterone-induced alopecia albino mouse model demonstrated higher follicular density and anagen:telogen ratios in groups treated with topical finasteride[6].**

## Approved Drug Therapies for Alopecia Areata: Janus Kinase Inhibitors

The FDA approval of Janus kinase (JAK) inhibitors for alopecia areata represents a paradigm shift in autoimmune alopecia treatment, as these represent the first disease-modifying agents approved specifically for this condition. However, it is important to note that alopecia areata differs mechanistically from androgenetic alopecia: **alopecia areata is an autoimmune disease mediated by T lymphocytes[11]**, whereas androgenetic alopecia is androgen-dependent.

### Baricitinib (Olumiant): Mechanism and Clinical Efficacy

**In June 2022, the FDA approved the JAK inhibitor baricitinib (Olumiant), a daily pill, for adults with severe alopecia areata[1].** This approval represented the first time the FDA specifically approved an oral drug for severe alopecia areata. **JAK inhibitors are immunomodulatory drugs that target the immune system pathway that is overactive in alopecia areata, with immune cells and hair follicles interacting through cellular mechanisms that utilize Janus kinase proteins to transmit signals in the immune system, and by taking a JAK inhibitor, the immune cell attack on the hair follicle is blocked allowing the normal hair growth cycle to continue and hair to regrow[1].**

**Olumiant was first approved in 2018 to treat rheumatoid arthritis[1].** Clinical trials assessing baricitinib efficacy in alopecia areata demonstrated substantial hair regrowth: **clinical trials showed that 32 percent to 35 percent of patients with extensive alopecia areata who took Olumiant at the 4 mg/kg dose had hair regrowth covering 80 percent or more of their scalp after 36 weeks of treatment[1].** Importantly, **longer term studies showed that hair regrowth continued over time with Olumiant treatment, with some people showing a delayed response to treatment initially but achieving hair regrowth over a longer period of time[1].** Most significantly, **after continuous treatment with Olumiant for 2 years (104 weeks), 90 percent of patients had hair regrowth covering 80 percent or more of their scalp[1].**

### Ritlecitinib (Litfulo): Expanding Access to Adolescent Populations

**In June 2023, the FDA approved another JAK inhibitor, LITFULO (ritlecitinib), for the treatment of severe alopecia areata in adults and adolescents ages 12 and up[1].** This approval represented significant progress in treating younger populations with severe autoimmune alopecia, as **Litfulo is the first approved treatment for individuals under the age of 18[1]**. The medication is administered orally once daily. **In a clinical trial, treatment with Litfulo was significantly more effective in achieving hair regrowth to 80 percent or more scalp hair coverage compared to placebo[1].**

### Deuruxolitinib (Leqselvi): Latest JAK Inhibitor Approval

**In July 2024, the FDA approved a third JAK inhibitor, deuruxolitinib (Leqselvi), for the treatment of alopecia areata in adults 18 years and older[1].** Leqselvi represents a novel dosing strategy compared to earlier JAK inhibitors. **Leqselvi is a new oral medication that is taken two times per day[1].**

Comparative efficacy analysis of JAK inhibitors demonstrates important differences in response patterns based on disease severity assessment methodology. **Regarding patients with alopecia areata defined as ≥50 percent scalp hair loss, baricitinib 4 mg once daily demonstrated the highest efficacy, however among patients with alopecia areata defined as a SALT score ≥50, oral deuruxolitinib 12 mg twice daily demonstrated the highest efficacy[30].** Furthermore, **both deuruxolitinib and baricitinib demonstrated a dose-response relationship[30]**, suggesting optimization of dosing based on individual patient response. The systematic review concluded that **patients with alopecia areata unresponsive to traditional therapies may benefit from treatment with baricitinib or deuruxolitinib[30].**

## Investigational and Pipeline Drug Therapies

### XVIE: Novel Investigational Therapy from Processed Amniotic Fluid

A Phase I/II clinical trial is currently evaluating an investigational injectable product derived from processed human amniotic fluid for androgenetic alopecia. **This study tests whether XVIE, an investigational injectable product made from processed human amniotic fluid, is safe and may help regrow hair in adults with androgenetic alopecia[8].** The theoretical mechanism involves growth factor signaling: **XVIE contains growth factors and extracellular vesicles that may stimulate hair follicle activity[8].** The trial design demonstrates careful safety monitoring appropriate for a novel biological product. **This Phase I/II randomized, double-blind, placebo-controlled trial evaluates the safety and preliminary efficacy of XVIE (decellularized allogeneic human amniotic fluid) administered via intradermal scalp injection in adults with androgenetic alopecia, with thirty participants randomly assigned to receive either XVIE or a saline placebo injected into the scalp in two treatment sessions, 90 days apart[8].** Importantly, **safety assessments include incidence, severity, and relatedness of treatment-emergent adverse events and serious adverse events, graded per CTCAE v5.0, with adverse events of special interest including scalp-specific events such as new-onset alopecia in previously unaffected areas, a decrease of 15 percent or greater in Total Area Hair Count within the injection zone, and scarring alopecia not consistent with natural androgenetic alopecia progression[8].** The study is registered as NCT07482423 and is currently not yet recruiting but is under development at two U.S. clinical sites.

### Topical Dutasteride: Superior Efficacy Compared to Finasteride

**Dutasteride, a dual 5-alpha-reductase inhibitor that selectively inhibits both type-I and II 5-alpha-reductase isoenzymes, has emerged as a potential treatment for androgenetic alopecia with off-label use[9].** Recent pharmaceutical development has focused on creating topical formulations to achieve superior efficacy compared to finasteride while minimizing systemic absorption and associated adverse effects. **Several randomized clinical trials have provided evidence of oral dutasteride's efficacy in mild to moderate androgenetic alopecia and have demonstrated its superiority over placebo and finasteride[9].**

A Phase II controlled clinical trial directly compared topical dutasteride formulations with oral finasteride in male androgenetic alopecia patients. **Dutasteride topical solution (0.05 percent w/v) demonstrated to be more efficacious than finasteride (1 mg/day) in treating male androgenetic alopecia[9].** The mechanisms underlying dutasteride's superior efficacy compared to finasteride reflect its broader enzyme inhibition profile: **finasteride selectively inhibits the type 2 isoenzyme, whereas dutasteride blocks both type 1 and 2 isoenzymes[22].** Specific efficacy measurements demonstrated: **pairwise comparisons revealed a significant improvement in total area hair count with 0.05 percent dutasteride compared to finasteride at week 24 (p=0.0083) as the primary efficacy endpoint[9].** Furthermore, **there was a statistically significant improvement in total area hair count with dutasteride 0.05 percent topical solution than placebo at week 12 (p=0.038) as a secondary efficacy endpoint[9].**

Systemic hormone levels demonstrated the anticipated differences between topical and oral formulations. **Dutasteride caused modest changes in serum testosterone/dihydrotestosterone, while finasteride caused moderate changes[9].** This finding suggests that topical dutasteride may achieve hair regrowth efficacy through localized DHT suppression while minimizing systemic hormonal perturbations associated with oral dutasteride or the higher systemic exposure seen with oral finasteride at therapeutic doses.

### Clascoterone: Selective Topical Antiandrogen

Clascoterone represents a distinct mechanism of androgenic alopecia treatment through selective androgen receptor antagonism localized to the scalp. **Clascoterone is a new topical antiandrogen developed for androgenetic alopecia[3].** In clinical development, **in a phase II exploratory study, clascoterone was more effective than cyproterone acetate or 17-alpha estradiol in increasing hair shaft diameter and hair follicle density[3].** Remarkably, **hair growth was higher in patients using clascoterone compared with topical minoxidil[3].**

Safety and tolerability data support favorable clinical applicability. **Clascoterone is generally well tolerated, with adverse effects in a phase II and III acne clinical trial including mild dryness, erythema, and hypertrichosis at the application site[3].** However, cost represents a significant barrier to widespread adoption: **for one tube (60 g) of 1 percent clascoterone, the average retail price is $656 USD[3].**

Recent clinical trial data demonstrate substantial efficacy improvements. **In Scalp 1, clascoterone achieved a 539 percent relative improvement in Target-Area Hair Count versus vehicle, while Scalp 2 delivered a 168 percent relative improvement[16].** These preliminary efficacy signals have prompted regulatory filing: **US, EU filings planned after Phase III wins for Cosmo's male pattern baldness drug clascoterone[16]**.

## Off-Label Repurposing and Combination Therapy Approaches

### Oral Minoxidil: Systemic Administration for Enhanced Efficacy

**After 6 months of administration, minoxidil 5 mg/day was significantly more effective than topical 5 percent and 2 percent in male androgenetic alopecia[35].** This finding has led to increased off-label use of oral minoxidil, particularly in patients demonstrating inadequate response to topical formulations. **Low-dose 0.5-5 mg/day may also be safe and effective for female pattern hair loss and chronic telogen effluvium[35].** Furthermore, **sublingual minoxidil may be safe and effective in male and female pattern hair loss[35].**

### Topical and Oral 5-Alpha-Reductase Inhibitors Combined with Minoxidil

**One meta-analysis including 15 randomized controlled trials found that all combination therapies produced higher global photographic assessment scores compared with monotherapy[3].** The synergistic potential of combining different mechanism-targeted agents has driven development of combination formulations. **A combination of low-level laser therapy and microneedling with minoxidil showed significant increases in hair counts compared with monotherapy[3].** Specifically targeting the combination of topical minoxidil with topical antiandrogens, **in one randomized controlled trial including 60 patients with androgenetic alopecia, topical minoxidil gel 5 percent and topical spironolactone gel 1 percent combined showed significant increases in anagen hairs within 12 months[3].**

A comprehensive analysis of combination treatment strategies proposed a structured approach to optimize therapy while managing adverse effects. **The authors consider finasteride, topical minoxidil, platelet-rich plasma, and low-level laser therapy to be core modalities, while others are alternative options[4].** The proposed treatment phases optimize tolerability and compliance: **at the initiation phase lasting initial 1–2 months of treatment, primary concern is smooth initiation of treatment with minimum side effects, preventing any patient dropout due to side effects that may be minor but may affect quality of life, such as dryness, irritation, temporary shedding[4].** More specifically, **the authors recommend initiation with topical minoxidil 5 percent local application once daily, with or without a peptide serum once daily with supplements of vitamins, micronutrients and protein-rich diet as necessary in the patient[4].** During the consolidation phase from 2–5 months, **as patients are getting accustomed to the schedule and are beginning to see early result, peptide is stopped and topical minoxidil 5 percent application is increased to twice daily, which is indeed the recommended dose, and at this stage, a modality that affects the basic pathogenesis including oral 1mg finasteride if patient is accepting it and/or platelet-rich plasma once a month for 3–4 months is initiated[4].**

A specific clinical trial examining combined oral and topical formulations demonstrated rapid hair regrowth. **While all 15 patients demonstrated significant growth of hair by the end of the treatment period, the eight patients who utilized all four treatment options experienced significant growth in as little as 30 days; for the patients using topical finasteride/dutasteride/minoxidil alone, significant hair growth was experienced after three months[6].** Furthermore, **the topical finasteride/dutasteride/minoxidil was formulated as a hypoallergenic lotion and found to be safe even in subjects with atopy[6].**

### Minoxidil and Finasteride Combination Studies

**As compared with control males, the compound index of hair growth raised from 30 percent at baseline up to 60 percent within 3 months of combined drug regimen which is better than oral drug only where there was no change, but still far beyond normalization of productivity considered as 100 percent[40].** More detailed analysis of follicle dynamics revealed: **exogen release and anagen initiation from pre-existing but functionally deficient follicles occurred mainly during combined drug treatment, with anagen initiation by topical minoxidil 5 percent not being maintained by oral finasteride alone[40].** These findings underscore that different mechanisms—finasteride addressing hormonal drivers while minoxidil addresses vascular supply and follicle cycling—provide complementary therapeutic effects.

### Topical Minoxidil-Finasteride Combination Therapy: Meta-Analytic Evidence

Recent systematic review and meta-analysis synthesized evidence for topical minoxidil-finasteride combination therapy. **This meta-analysis of seven randomized controlled trials (*N* = 396) demonstrated superior efficacy of topical minoxidil-finasteride combination over monotherapy for male androgenetic alopecia[2].** Specific efficacy parameters demonstrated: **pooled analyses showed clinically meaningful improvements in hair density (MD = 9.22, p = 0.04), hair diameter (MD = 2.26, p = 0.005), and global photographic assessment (MD = 0.79, p < 0.00001), all exceeding minimal clinically important thresholds[2].**

The treatment effect demonstrated hierarchical patterns based on improvement magnitude. **The treatment effect followed a hierarchical pattern, with combination therapy showing strongest benefits for marked improvement (OR = 3.29, p = 0.015) and more variable results for moderate outcomes[2].** Importantly, the authors acknowledged evidence limitations: **while primary outcomes demonstrated robust effects with moderate certainty evidence, observed heterogeneity in some endpoints and sample size limitations suggest the need for standardized assessment methods and larger confirmatory studies to strengthen these conclusions[2].**

### Platelet-Rich Plasma for Androgenetic Alopecia

Platelet-rich plasma (PRP) represents a biologically derived treatment approach that has gained substantial clinical interest despite variable evidence quality. **The majority of studies demonstrated that PRP is effective in increasing hair density and thickness in patients with androgenetic alopecia[14].** The mechanistic basis for PRP efficacy involves its growth factor composition: **the efficacy of PRP is attributed to its high concentration of growth factors, including platelet-derived growth factor, transforming growth factor-beta, and vascular endothelial growth factor, which stimulate hair follicle activity and promote the anagen growth phase of the hair cycle[14].**

Specific efficacy data demonstrated improvements in multiple parameters. One representative study found that **after three PRP treatments, hair count, density, and diameter significantly improved at three and six months, with PRP increasing hair density from three months and hair count, diameter, and anagen hair ratio at six months compared to the control side[14].** Combination strategies enhanced results: **a notable increase in hair count, density, and growth rate was noted following platelet-rich plasma therapy compared to minoxidil treatment, with platelet-rich plasma combined with topical minoxidil yielding greater enhancements in efficacy and patient satisfaction compared to monotherapy[14].**

### Low-Level Laser Therapy

**Low-level laser therapy has been reported to stimulate hair growth in men and women in androgenetic alopecia and was approved by the US FDA in 2007[15].** The proposed mechanism involves modulation of the hair cycle. **It is assumed to stimulate anagen phase re-entry in telogen hair follicles, prolong the duration of anagen phase, and increase rates of proliferation in active anagen hair follicles[15].** Furthermore, **it helps to promote reparative regeneration, which occurs during wound healing, and physiological regeneration, which occurs during the hair cycle, which relies heavily on cell proliferation[15].**

The physiological basis for laser phototherapy involves mitochondrial and metabolic effects. **It is feasible to suggest that the hair growth stimulatory effect of laser phototherapy is mediated through either a direct or an indirect increase in the proliferative activity within the hair follicle epithelial matrix, with low-level laser therapy potentially accelerating keratinocyte and fibroblast mitosis by generating reactive oxygen species and antioxidants[15].** The most accepted mechanistic explanation involves metabolic enhancement: **the most widely accepted mechanism is that low-level laser therapy displaces nitric oxide from cytochrome c oxidase allowing an influx of oxygen to bond to cytochrome c oxidase and progress forward in the respiratory process to adenosine triphosphate production and reactive oxygen species signaling[15].**

Specific clinical recommendations guide optimal LLLT application. **The most widely used regimen is alternate days for a period of at least 4 months to start seeing the results and to be continued indefinitely[4].** Additionally, **low-level laser therapy increases the terminal hair count and reduces hair fall with minimal adverse effects[4].** Patient populations demonstrating best response characteristics are identified: **patients with male androgenetic alopecia (Hamilton–Norwood III and IV) and female androgenetic alopecia (Ludwig I and II) respond best, since effective photobiostimulation depends on a minimum of hair for effective photobiostimulation and on a maximum of hair for the laser beam to reach the scalp without absorption or interference from existing hairs[15].**

### Microneedling in Combination with Topical Treatments

**The combined microneedling group showed statistically significant improvement in hair density and diameter, and good safety compared with monotherapy[28].** Systematic analysis of combination microneedling therapy demonstrated substantial efficacy improvements. **Finally, 13 randomized controlled trials involving 696 androgenetic alopecia patients were included to compare the clinical effectiveness and adverse events of combined microneedling therapy with single microneedling therapy or single drug therapy for androgenetic alopecia, with results showing: hair density and diameter changes showed the combined microneedling group was significantly better than any single treatment group with statistical significance (MD = 13.36, 95% CI = [8.55, 18.16], Z = 5.45, p < 0.00001; MD = 2.50, 95% CI = [0.99, 4.02], Z = 3.23, p = 0.001)[28].** Patient satisfaction metrics favored combined approaches: **the doctor satisfaction rating of the combined microneedling group was significantly higher than that of any single treatment group, with statistical difference (RR = 2.03, 95% CI = [1.62, 2.53], Z = 6.24, p < 0.00001)[28].**

### Hair Transplantation

**Hair transplantation by follicular unit extraction method provides good hair cover in androgenetic alopecia in males, with this modern dermatosurgical technique offering many innovations[23].** Patient selection optimizes outcomes: **patients less than 33 years and with Hamilton stage 4a and less were found to have better results after the procedure[23].** Quantitative outcomes demonstrate meaningful improvements: **average number of follicular units transplanted in patients was 1290 with improvement in hair density of 30.61 follicular units per square centimeter[23].**

### Scalp Micropigmentation

A relatively novel approach to alopecia management involves cosmetic camouflage through tattooing techniques. **Scalp micropigmentation is emerging for camouflaging localized alopecia, yet standardized protocols and long-term efficacy data remain limited[27].** The technique achieves cosmetic improvement through simulation rather than actual hair growth. **Unlike methods that attempt to increase real hair density, scalp micropigmentation employs tattoo techniques to simulate the appearance of cut hair shafts, thereby reducing scalp visibility[27].** Clinical outcomes demonstrate patient satisfaction: **all patients achieved significant cosmetic improvement, with immediate posttreatment visual density scores averaging 8.7 ± 1.1, where androgenetic cases scored higher at 9.1 ± 0.5[27].** Long-term cosmetic retention showed modest decline: **at 6-month follow-up, visual density scores declined modestly to 7.7 ± 1.4, though scarring alopecia showed greater fading (Δ = 1.6 vs. Δ = 0.9 in androgenetic, p = 0.03)[27].** Patient satisfaction remained high: **patient satisfaction was high with mean patient satisfaction score of 2.7/3, with 85.7 percent of androgenetic cases "very satisfied"[27].**

## Off-Label Drug Repurposing: Antiandrogens and Hormone-Modulating Therapies

### Spironolactone for Female Pattern Hair Loss

**Spironolactone both reduces adrenal androgen production and exerts competitive blockade on androgen receptors in target tissues[25].** The drug has extensive off-label use in female pattern hair loss despite lacking FDA approval for this indication. **Spironolactone has been used off-label in female pattern hair loss for over 20 years and has been shown to arrest hair loss progression with a long-term safety profile[25].**

Clinical experience demonstrates substantial efficacy in selected populations. **A significant percentage of women also achieve partial hair regrowth with spironolactone[25].** More recent retrospective data found that **nearly 75 percent of women reported stabilization or improvement of their hair loss after treatment with spironolactone[32].** Additionally, **an open intervention study of 80 women who received treatment with spironolactone (200 mg daily) or cyproterone acetate (50 mg daily or 100 mg for 10 days per month if premenopausal) showed that three of four patients demonstrated an improvement or stabilization of their disease with no difference of effect between the therapies received[32].**

Notably, **spironolactone is not used in male androgenetic alopecia because of the risk of feminization[25]**, reflecting the potency of androgen receptor antagonism in blocking DHT effects on male sexual tissues and development.

### Bicalutamide: Peripheral Selective Antiandrogen

**Bicalutamide is a non-steroidal anti-androgen and blocks androgen receptor binding by disrupting dihydrotestosterone-induced events for androgen receptor gene expression[21].** Unlike earlier antiandrogens, **bicalutamide acts on the peripheral receptors at the prostate without affecting the hypothalamus-pituitary axis, competitively inhibiting androgen binding to androgen receptor without affecting 5-alpha reductase or inducing hormonal fluctuations with minimal impact on serum luteinizing hormone and testosterone levels and less hepatotoxicity[21].**

Clinical experience with bicalutamide for female pattern hair loss remains limited but promising. **In female pattern hair loss treated with oral bicalutamide 25-50 mg once daily in 44 cases, when continued for more than 6 months, there was mean reduction of Sinclair scale of 0.81 representing 27.5 percent improvement[21].** Combination approaches may optimize outcomes: **oral bicalutamide was given 10 mg daily in combination with oral minoxidil (n = 308) and oral spironolactone (n = 172), and this combination showed a favorable safety profile with a mild increase in serum transaminases, less than with flutamide[21].** Safety monitoring data revealed: **in oral bicalutamide for female pattern hair loss, safe doses are up to 50 mg given daily or on an alternate day, and a maximum of 3-fold rise in transaminases was noted in 11.7 percent of patients, with transaminases reverting to normal on continued treatment[21].**

### Flutamide: Antiandrogen with Hepatotoxicity Concerns

**Flutamide is an oral anti-androgen that acts by competitively inhibiting the uptake of androgen and its nuclear binding in target tissues[32].** The agent has demonstrated efficacy in female pattern hair loss: **flutamide has been shown to be effective for the treatment of female pattern hair loss in hyperandrogenic women at a dose of 250 mg per day[32].** Additionally, **in a large prospective study including 101 women with female pattern hair loss receiving varying doses of flutamide for 4 years, there was a significant reduction in alopecia scores compared with baseline, with the maximum drug effect occurring after 2 years and being maintained for 2 years thereafter[3].**

However, hepatotoxicity concerns substantially limit flutamide's clinical utility. **Flutamide has a well-known risk of causing elevated liver enzymes, especially at high doses, something not seen commonly with another drug in its class, bicalutamide[3].** Furthermore, **flutamide increases luteinizing hormone and testosterone levels and is hepatotoxic, limiting its use in conditions such as hirsutism and female pattern hair loss[21].**

### Cyproterone Acetate: Combination Oral Contraceptive Strategy

**Cyproterone acetate is an antiandrogen that blocks gonadotropin-releasing hormone and androgen receptors[3].** In women with female pattern hair loss, cyproterone acetate is typically combined with estrogen-containing oral contraceptives. **When compared with no treatment, patients who received ethinyl estradiol 50 μg and cyproterone acetate 2 mg with cyproterone acetate 20 mg on days 5 to 20 of the menstrual cycle for 1 year had a significant increase in their percentage of anagen hairs with trends toward a larger shaft diameter of full anagen hairs and a decreased number of hairs that were less than 40 microns[32].**

## Natural Products and Botanical Therapies

### Saw Palmetto: Botanical Antiandrogen

**Saw palmetto is a botanical extract with antiandrogenic properties that has gained commercial popularity for its purported benefits on hair regrowth[10].** Systematic evidence synthesis found: **five randomized clinical trials and 2 prospective cohort studies demonstrated positive effects of topical and oral supplements containing saw palmetto (100–320 mg) among patients with androgenetic alopecia and telogen effluvium[10].**

Quantitative improvements in hair parameters were documented. **Sixty percent improvement in overall hair quality, 27 percent improvement in total haircount, increased hair density in 83.3 percent of patients, and stabilized disease progression among 52 percent were noted with use of various topical and oral saw palmetto-containing supplements[10].** Specific study findings demonstrated: **the saw palmetto-containing topical demonstrated a time-dependent mean increase in hair count of 17 percent by week 10 and 27 percent by week 50, as compared to 6 and 14 percent among the vehicle group at weeks 10 and 50 respectively (p < 0.005)[10].** Mechanistic research revealed: **saw palmetto has shown promising results in murine models, with hair regrowth mediated through transforming growth factor-β and mitochondrial signaling pathways[10].** Furthermore, **in vitro models have demonstrated saw palmetto's ability to inhibit inflammatory gene expression in human keratinocytes, suggesting a multifaceted mechanism for the treatment of androgenetic alopecia, in addition to its proposed antiandrogenic properties[10].**

Combination therapy enhanced results substantially. **The use of topical saw palmetto and oral gelatin-cystine supplement caused a further increase of approximately 50 percent in all hair growth parameters (p < 0.005) when compared to use of either agent alone[10].**

### Plant Extracts: Saw Palmetto, Red Ginseng, and Cnidium

Recent comprehensive reviews have identified multiple plant extracts demonstrating anti-androgenic, anti-inflammatory, antioxidative, and anti-apoptotic properties. **Saw palmetto extract demonstrates multi-target actions—anti-androgenic, modulation of follicular microenvironment, and regulation of hair cycle—that are supported by in vitro, animal, and multiple clinical studies[5].**

Red ginseng extracts demonstrated promising mechanisms and clinical outcomes. **In in vivo study, red ginseng extracts induced premature transformation of hair follicles from catagen to anagen, stimulated hair growth, and promoted the expression of growth factors vascular endothelial growth factor and insulin-like growth factor-1 in 7-week-old C57BL/6 mice[5].** Furthermore, **the expressions of β-catenin protein and Wnt target proteins cyclin D1 and cyclin E were increased, the activities of antioxidant enzymes GPx and SOD-2 were increased, and the phosphatidylinositol 3-kinase/Akt pathway was activated in the skin lesions after treatment with red ginseng extracts[5].**

Ferulic acid, an active constituent of *Cnidium officinale*, demonstrated estrogen receptor-alpha mediated hair growth promotion. **Both Cnidium officinale extract and ferulic acid significantly enhanced dermal papilla cell proliferation and mitochondrial activity, and extended hair follicle length during the anagen phase[43].** The mechanism involved estrogen receptor signaling: **ferulic acid elevated c-JUN messenger RNA expression, indicating estrogen-like estrogen receptor alpha activation, and increased the expression of MAPK pathway-associated proteins, along with key growth factors[43].**

### Oral Nutritional Supplements

Multiple oral nutritional formulations have been studied as adjuvants to pharmaceutical therapies. **A food supplement in tablet formulation containing hydrolysed fish-origin collagen (300 mg/dose), taurine, cysteine, methionine, iron, and selenium has been recently available[41].** Clinical efficacy was demonstrated: **this clinical trial demonstrated that the tested oral supplement, containing hydrolysed fish-origin collagen, taurine, cysteine, methionine, iron and selenium was able to improve the clinical efficacy of specific anti-hair loss treatments in subjects with hair loss disorders such as androgenetic alopecia or female androgenetic alopecia or chronic telogen effluvium[41].**

Marine-derived protein supplements demonstrated efficacy in female hair loss. **An oral marine protein supplement is designed to promote hair growth in women with temporary thinning hair[29].** The key ingredient composition included: **The key ingredient in marine protein supplement tablets is AminoMar marine complex, horsetail which contains a naturally occurring form of silica, acerola cherry which provides vitamin C, biotin, and zinc, with AminoMar composed of a proprietary blend of shark powder and mollusc powder derived from sustainable marine sources[29].**

## Medications that Cause or Worsen Androgenetic Alopecia as Adverse Effects

### Oral Retinoids: Isotretinoin-Induced Telogen Effluvium

**Although the side effects of isotretinoin treatment are mostly predictable, one potential and more unpredictable side effect is hair loss, typically telogen effluvium[12].** The frequency of this adverse effect remains variable across studies. **The percentage of patients that report hair loss after isotretinoin treatment ranges from 0.28 percent to 12.0 percent[12].**

Demographic and dosing characteristics associated with increased risk were identified. **Compared to patients on isotretinoin without hair loss, patients who developed hair loss were older, had higher cumulative isotretinoin dose, and longer duration of treatment (P = 0.008, P = 0.004, and P < 0.001, respectively)[12].** These relationships persisted after statistical adjustment: **these relationships were still significant after removal of outliers from both groups[12].**

### Antidepressants and Anticonvulsants

Multiple drug classes used in psychiatry and neurology are associated with hair loss. **Common examples of medications causing hair loss include chemotherapy, antidepressants, and anti-seizure medications[13].** More specifically, **according to one study, bupropion may have a higher risk of hair loss compared with other antidepressants[13].** Importantly, **hair loss usually reverses once you stop taking an antidepressant, but you shouldn't stop taking an antidepressant on your own, as this can cause serious side effects, especially if you've been taking it for a while[13].**

Anticonvulsant-associated hair loss demonstrates variable frequency by agent. **Hair loss is a reported side effect of some anticonvulsant medications, with one review finding hair loss was more common with valproic acid and pregabalin but less common with levetiracetam[13].**

### Blood Thinners and Anticoagulants

**Anticoagulants associated with hair loss include heparin, warfarin, apixaban, and rivaroxaban[13].** However, clinical management options exist: **if hair loss is an issue, your prescriber may try switching you to a different blood thinner, but you shouldn't make any changes to your anticoagulant medication without their OK[13].**

### GLP-1 Receptor Agonists: Weight Loss-Associated Hair Loss

Recent reports of hair loss associated with GLP-1 receptor agonist use warrant careful mechanistic consideration. **Some people taking GLP-1 related medications, such as semaglutide and tirzepatide, reported hair loss in clinical trials[13].** However, the mechanistic basis may reflect body weight changes rather than direct medication effects: **it's not clear if this is from the medication itself or if it's due to the weight loss seen with these medications, because losing weight rapidly is a known cause of hair loss[13].**

## Contraindications and Special Populations

### Women of Childbearing Potential and Finasteride Teratogenicity

**Women and children should not use finasteride, with women who are pregnant or may become pregnant not handling crushed or broken tablets, as finasteride can be absorbed through the skin and cause birth defects in male babies[7].** More specifically, **if a woman does come in contact with this medicine, the affected area should be washed right away with soap and water[7].**

### Pediatric Androgenetic Alopecia

**Currently, there are no FDA-approved treatments for androgenetic alopecia in children under the age of 12[1].** When treating these younger patients therapeutically, practitioners must utilize off-label approaches: **when treating these younger patients, a dermatologist might try to obtain insurance coverage for a JAK inhibitor that is FDA-approved for adults by prescribing one for off-label use[1].**

## Recent Therapeutic Advances and Combination Protocols

### Retinoids Combined with Minoxidil

Emerging evidence supports synergistic effects of combining topical retinoids with minoxidil. **Recent studies and research have shown the positive effect that topical retinoids have had on androgenetic alopecia, with retinoids having a supporting effect with drugs like minoxidil in treating patients with alopecia[37].** The mechanistic rationale involves complementary pathways: **while minoxidil focuses on the anagen phase where hair growth occurs and eventually promotes new hair growth in androgenetic alopecia, retinol aids by improving the scalp conditions, eventually encouraging hair growth[37].**

Preclinical evidence demonstrated synergistic potential: **All-trans retinoic acid combined with minoxidil enhanced hair growth significantly as compared to minoxidil alone, suggesting the key role retinoids can play[37].** Recent clinical observations support this approach: **a recent case series (2025) demonstrated that sequential application of 0.1 percent tretinoin gel followed by 5 percent minoxidil over 90 days significantly improved hair density, anagen conversion, and thickness in androgenetic alopecia nonresponders[37].** Moreover, **a novel Phase I trial is currently underway to assess the safety of a new topical solution in male androgenetic alopecia patients[37].**

### Concentrated Growth Factors

Investigational concentrated growth factor technology represents emerging biological treatment. **According to this tiny single-arm trial, the use of concentrated growth factor injection may help androgenetic alopecia by increasing terminal hair density and hair density[26].** Efficacy parameters demonstrated: **at 3 months after the first injection, the hair density and hair growth ratio were significantly improved, with significant improvements found in Global Aesthetic Improvement Scale score by both patients and independent doctors and the hair growth promotion sustained for 6 months after first treatment[26].** Safety assessments were favorable: **no severe topical or systemic adverse events occurred during the treatment[26].**

### IGF-1 and Growth Factor-Based Approaches

Insulin-like growth factor-1 represents a critical growth factor in hair follicle physiology with therapeutic potential. **Insulin-like growth factor 1 has been shown to affect follicular proliferation, tissue remodeling, and the hair growth cycle, as well as follicular differentiation, identifying IGF-1 signaling as an important mitogenic and morphogenetic regulator in hair follicle biology[31].** Mechanistically, **dermal papillary cells from balding scalp follicles were found to secrete significantly less IGF-1 than their counterparts from nonbalding scalp follicles[31].**

Human disease observations provide clinical insight: **hypotrichosis in primary growth hormone deficiency, and a lack of response of female and male androgenetic-type alopecia to treatment with topical minoxidil and oral finasteride in patients who had undergone surgical resection of the pituitary gland, provide further evidence for an effect of IGF-1 on hair growth and alopecia[31].**

### Prostaglandin Analogs: Latanoprost and Bimatoprost

Prostaglandin-based therapies have demonstrated hair growth promotion in multiple alopecia phenotypes. **Latanoprost is a prostaglandin F2-alpha analog, which is clinically used to cure glaucoma[47].** The clinical observations that prompted investigation derived from ophthalmology: **the use of prostaglandin analogs for alopecia came about when eyebrow and eyelash hair growth were observed in glaucoma patients[3].**

Mechanistic understanding of prostaglandin effects on hair cycling reveals complexity. **Prostaglandin F2 and PGE2 cause hair growth and prolong the anagen phase, whereas PGD2 inhibits hair growth[3].** Clinical efficacy has been documented: **in a randomized control trial using 0.1 percent latanoprost in 16 patients with male pattern hair loss, after 24 weeks, there was significantly increased hair density compared with baseline and placebo-treated areas[3].**

More potent prostaglandin analogs show enhanced effects. **Furthermore, 0.3 percent bimatoprost remarkably accelerated hair regrowth in the androgenetic alopecia mouse model and exhibited a 585 percent increase within 10 days compared to the control group, implying that most of the telogen was transitioned into the anagen phase[47].** The magnitude of effect in preclinical studies proved substantial: **this 5 percent bimatoprost formulation significantly increased the hair weight by 8.45- and 1.30-fold compared to the 5 percent bimatoprost in ethanol and 5 percent minoxidil-treated groups, respectively[47].**

## Conclusion and Future Therapeutic Directions

Androgenetic alopecia remains one of the most common dermatological conditions affecting quality of life for millions of individuals worldwide. **Androgenetic alopecia affects an estimated 50 million men and 30 million women in the United States[19].** While the pathophysiology has been extensively characterized—involving androgens, genetic predisposition, and hair follicle miniaturization—therapeutic options have historically remained limited.

Current approved pharmacological treatments include **only two US FDA-approved drugs for androgenetic alopecia: topical minoxidil and oral finasteride[3].** However, the landscape has substantially evolved with investigation of multiple novel mechanisms including JAK inhibitors initially developed for autoimmune conditions, selective topical antiandrogens, prostaglandin analogs, botanical therapies, and regenerative medicine approaches. Combination pharmacotherapy has emerged as standard clinical practice, with evidence demonstrating synergistic benefits exceeding monotherapy efficacy across multiple drug pairings.

Recent pharmaceutical advances have focused on optimizing bioavailability, reducing systemic adverse effects, and expanding treatment access to previously underserved populations including adolescents and women. Topical formulations of 5-alpha-reductase inhibitors represent exemplary advances in this regard, achieving localized DHT suppression without the systemic hormonal perturbations inherent to oral dosing. Investigational agents including growth factor-based therapies, novel antiandrogens, and cell-based interventions remain in development with the potential to substantially improve long-term outcomes.

Healthcare providers prescribing androgenetic alopecia treatments must carefully weigh efficacy data, individual patient risk-benefit profiles, and mechanism-of-action complementarity when selecting monotherapy versus combination approaches. Patient education regarding realistic treatment timelines—typically requiring 3-6 months for visible improvement—remains essential for adherence and satisfaction. Future research should prioritize large-scale, long-term randomized controlled trials with standardized outcome assessments to optimize therapeutic protocols and identify predictive biomarkers of treatment response.