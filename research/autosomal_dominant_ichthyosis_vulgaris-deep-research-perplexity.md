---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T14:28:24.348099'
end_time: '2026-04-04T14:30:36.664000'
duration_seconds: 132.32
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: autosomal dominant ichthyosis vulgaris
  mondo_id: MONDO:0007810
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
- **Disease Name:** autosomal dominant ichthyosis vulgaris
- **MONDO ID:** MONDO:0007810

## Research Objectives

Please provide a comprehensive research report on **drug therapies for autosomal dominant ichthyosis vulgaris**.
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

# Comprehensive Analysis of Drug Therapies for Autosomal Dominant Ichthyosis Vulgaris

Autosomal dominant ichthyosis vulgaris, the most common form of inherited ichthyosis affecting approximately one person in 250 to 300 individuals, is caused by loss-of-function mutations in the filaggrin gene (FLG) on chromosome 1q21[24][25][25]. While no curative treatment currently exists, an evolving therapeutic landscape now encompasses topical emollients containing urea and ceramides as first-line agents, topical and systemic retinoids as established pharmacological interventions, and increasingly, immune-modulatory biological therapies targeting IL-17 and IL-23 pathways that show promise based on emerging immunopathologic insights[1][5][15]. Recent investigational approaches, including topical isotretinoin formulations with enhanced delivery systems and serine protease inhibitors, represent important advances in development, while drug repurposing of Janus kinase inhibitors and other inflammatory disease therapeutics offers potential alternative pathways for patients with insufficient response to conventional therapy. This comprehensive report synthesizes current evidence on approved therapies, investigational agents in clinical development, repurposing candidates with supporting evidence, contraindicated medications, drug-induced adverse events, and established combination therapy approaches for autosomal dominant ichthyosis vulgaris.

## Approved and Established Drug Therapies for Ichthyosis Vulgaris

### Topical Emollients and Hydrating Agents

The foundation of ichthyosis vulgaris management remains unchanged from earlier treatment paradigms and continues to rely on frequent bathing, environmental humidification, and daily application of topical emollients as the cornerstone of therapy[15]. Urea-based creams containing 10% urea combined with ceramides and natural moisturizing factors have emerged as the first-line topical treatment with the strongest evidence base for efficacy in ichthyosis vulgaris[5][5]. In a clinical study conducted by Clara and colleagues, ichthyosis vulgaris patients who received urea-based emulsions containing 10% urea, ceramides, and natural moisturizing factors applied twice daily demonstrated significant improvement in dryness, reduced pruritus, and achieved cosmetically acceptable results within 30 days of treatment[5][5]. Videodermatoscopy and reflectance confocal microscopy studies performed on these same patients showed complete disappearance of scales, providing objective confirmation of therapeutic benefit[5][5]. The hygroscopic properties of urea, which facilitate water absorption and retention in the stratum corneum, directly address the pathophysiology of xerosis in ichthyosis vulgaris by improving transepidermal water loss measurements[13].

Ammonium lactate 12% lotion represents another effective topical therapeutic agent for ichthyosis vulgaris when combined with physiological lipid-based barrier repair creams[30][5]. A combination therapy approach utilizing ammonium lactate 12% lotion followed by a ceramide-dominant barrier repair cream containing ceramide, cholesterol, and free fatty acids in a physiological 3:1:1 molar ratio proved remarkably effective in case studies of ichthyosis vulgaris patients, with complete eradication of scaling and dryness occurring within one month of twice-daily application, with no symptom recurrence upon continued treatment[30]. The mechanism of this combination approach targets both the corneocytes (the "bricks" of the stratum corneum) through the keratolytic action of lactic acid, and the intercellular lipid bilayer (the "mortar") through provision of physiological lipid mixtures that can traverse the epidermis and be incorporated into lamellar bodies at the granular layer[30]. This dual-component approach achieves sustained barrier repair that persists longer than occlusive agents alone, such as petrolatum, which primarily restrict their effects to the superficial stratum corneum[30].

Propylene glycol solutions at 40% to 60% aqueous concentrations have demonstrated substantial therapeutic efficacy for ichthyosis vulgaris when combined with occlusion[5]. In comparative studies by Goldsmith and colleagues, almost all scales could be easily removed during bathing after only two to three applications of 60% propylene glycol solution combined with overnight occlusion in patients with ichthyosis vulgaris, with 60% dilution proving more effective than 40%[5][5]. The critical importance of occlusion to treatment success distinguishes propylene glycol from other topical agents, as the treatment was found to be unresponsive when application was not followed by occlusion, requiring intermittent continued application to maintain scale clearance[5][5]. Propylene glycol solutions possess significant practical advantages including absence of systemic toxicity, low cost, and ease of application, making them a reliable option for ichthyosis vulgaris in resource-limited settings[5][5].

### Topical Retinoid Therapy

Topical retinoids, primarily tretinoin (retinoic acid) and tazarotene, have been established as effective agents for reducing scaling and hyperkeratotic thickening in autosomal dominant ichthyosis vulgaris through their ability to normalize keratinization and accelerate epidermal cell turnover[15][15][15]. Tretinoin is available in concentrations of 0.025%, 0.05%, and 0.1% cream formulations for daily application, and can be compounded with 2% salicylic acid for enhanced penetration in palmoplantar regions where thickened skin may reduce drug absorption[15][15]. Tazarotene, available as 0.05% and 0.1% gel or cream formulations, may improve ectropion (a complication more common in lamellar ichthyosis but occasionally seen in severe ichthyosis vulgaris) and should be monitored carefully for irritation, particularly in periocular areas and skin folds where barrier function is compromised[15][15].

Adapalene, available as 0.1% and 0.3% gel formulations, represents a third topical retinoid option that may be less irritating than tretinoin or tazarotene while maintaining clinical efficacy[15][15]. The beneficial effects of topical retinoids in ichthyosis vulgaris are dose-dependent, with progressive improvement in scaling, induration, and crusting observed with increasing concentrations; however, cutaneous toxicity becomes limiting at higher doses, particularly with regard to erythema, pruritus, irritation, and photosensitivity[15][15][15]. Patients with ichthyosis vulgaris appear to have an increased risk for these adverse cutaneous effects compared to other dermatologic conditions, likely due to underlying epidermal barrier compromise and associated atopic diathesis[15][15].

Consensus recommendations emphasize that topical retinoids have lower risk for adverse systemic effects compared to oral retinoids and therefore should be considered for use in milder disease presentations and when the risks of systemic therapy outweigh potential benefits[15][15][15]. The safety profile of topical retinoids allows their continued use alongside systemic retinoid therapy in patients requiring more aggressive treatment, potentially allowing lower systemic retinoid dosing or enabling retinoid "holidays" with maintained disease control[15][15][15].

### Systemic Retinoid Therapy

Systemic retinoids, including isotretinoin (13-cis-retinoic acid) and acitretin, are vitamin A derivatives that regulate cell differentiation, proliferation, and apoptosis, thereby accelerating the shedding of excessive scales and decreasing hyperkeratosis with resulting normalization of the epidermis[15][15][28][15]. Isotretinoin is typically administered at maintenance doses of 0.5 to 1 mg/kg/day for ichthyosis vulgaris, with the modern consensus favoring the lowest effective dose to minimize long-term toxicity while maintaining disease control[15][15][19][15]. Historical dose-escalation studies conducted in the early development phase of isotretinoin examined dosage ranges from 1 mg/kg/day to 7 mg/kg/day, and while increasing doses produced greater improvements in scaling, induration, and crusting, cutaneous toxicity became dose-limiting at higher doses, particularly with regard to skin irritation, fragility, and excessive dryness[15][15][15].

Acitretin, the active metabolite of etretinate (no longer manufactured), is administered at maintenance doses of 0.5 to 1 mg/kg/day, typically ranging from 10 to 25 mg daily with maximum doses of 75 mg/day[15][15]. A critical distinction between isotretinoin and acitretin relates to their half-lives and teratogenic potential, with isotretinoin possessing a short half-life of approximately 15-20 hours, while acitretin undergoes enterohepatic recycling and possesses a prolonged half-life of 12 to 36 hours (potentially extending to several years when combined with alcohol, which can lead to etretinate formation through transesterification)[15][15][15]. Due to the prolonged half-life of acitretin and its teratogenic properties, current consensus recommendations suggest considering transition of patients of childbearing potential from acitretin to isotretinoin before puberty if pregnancy is anticipated[15][15][19][15].

The therapeutic efficacy of systemic retinoids in ichthyosis vulgaris is contingent upon continued treatment, as the skin characteristically reverts to its pretreatment condition upon cessation of retinoid therapy[15][15][15]. Accordingly, ichthyosis vulgaris is recognized as a lifelong disorder requiring long-term systemic retinoid treatment in patients with moderate to severe disease, necessitating careful patient counseling regarding potential long-term toxicities[15][15][15]. Although many patients have successfully received systemic retinoids for decades, prospective long-term studies clarifying optimal maintenance dosing strategies remain absent from the medical literature[15][15][15].

The optimal dose of systemic retinoid for ichthyosis vulgaris represents the lowest dose that achieves and maintains desired therapeutic effect while maintaining acceptable mucocutaneous and systemic toxicities, determined through shared decision-making between patient, caregiver, and physician based on consideration of disease severity, quality-of-life impact, and individual risk factors[15][15][15]. Seasonal variation affects disease severity and thus required dosing in some patients, with warmer, humid weather often permitting dose reduction, while dry seasons may necessitate dose escalation to maintain disease control[15][15][15].

## Investigational and Pipeline Drugs for Ichthyosis Vulgaris

### Topical Isotretinoin Formulations with Novel Delivery Systems

TMB-001, developed by Timber Pharmaceuticals, represents an investigational topical formulation of isotretinoin specifically designed to treat congenital ichthyoses, including autosomal dominant ichthyosis vulgaris, using a proprietary Invisicare delivery system technology to enhance absorption and minimize systemic toxicity[18][23]. The development of TMB-001 was motivated by well-recognized concerns regarding long-term toxicity of oral retinoid therapies, particularly regarding bone and eye health, with the goal of achieving therapeutic benefit through topical application while reducing systemic exposure[11]. Topical isotretinoin administered via the Invisicare delivery system works through multiple mechanisms: it promotes keratolysis to reduce skin scaling, enhances skin hydration to restore barrier function, provides lubrication to alleviate discomfort, and modulates gene expression to support healthier skin regeneration[23].

The mechanism of action of topical isotretinoin involves suppression of keratinocyte overproduction through reduction of cellular proliferation, representing a disease-modifying approach distinct from symptomatic treatments that merely strip away existing scales without inhibiting the underlying keratinocyte dysfunction[11]. In phase 2b clinical trial data from the CONTROL study, the average time to treatment response was 28 days, after which shedding of diseased skin and production of new normal-appearing skin became evident[11]. Isotretinoin also reduces inflammation characteristic of many ichthyosis presentations by attenuating the overproduction of skin cells that drives pro-inflammatory pathways, and many treated patients experienced marked reduction in erythema that could previously only be managed through symptomatic approaches[11]. TMB-001 received Orphan Drug Designation from the U.S. Food and Drug Administration for treatment of congenital ichthyosis in 2014, and more recently has been granted Breakthrough Therapy status[8][18][23].

A randomized, double-blind, vehicle-controlled Phase III clinical trial designated ASCEND (NCT05295732) is currently evaluating the safety and efficacy of topical TMB-001 0.05% ointment for treatment of ichthyosis in subjects with either recessive X-linked ichthyosis (RXLI) or autosomal recessive congenital ichthyosis (ARCI) subtypes[18]. The ASCEND study employs a three-period design: an induction period of three weeks with once-daily dosing, a nine-week treatment period with twice-daily dosing, and a 12-week maintenance period in which eligible responders (those achieving ≥1-point reduction in Investigator Global Assessment score from baseline) are randomized to either twice-daily or once-daily maintenance therapy[18]. A subset of preselected centers is recruiting subjects for an Optional Maximal Use arm to evaluate systemic exposure and safety of topical TMB-001[18].

### Biological Therapeutics Targeting Interleukin Pathways

Recent immunopathologic discoveries have revealed that Th17-skewed inflammation characterizes ichthyotic skin across multiple disease subtypes, leading to investigation of biological therapeutics that target interleukin pathways implicated in psoriasis for potential application in ichthyosis[42]. This recognition that inflammatory cytokines including IL-17 and IL-23 contribute to disease pathogenesis in ichthyosis vulgaris has prompted clinical investigation of multiple monoclonal antibody therapeutics[1][1][42].

Secukinumab, a recombinant human monoclonal antibody targeting the IL-17A cytokine, was investigated in a double-blind, randomized, placebo-controlled trial that included patients with several congenital ichthyosis subtypes, though notably not specifically focused on ichthyosis vulgaris[10][1]. In that trial, patients with epidermolytic ichthyosis, Netherton syndrome, lamellar ichthyosis, or congenital ichthyosiform erythroderma received secukinumab 300 mg intravenously every 4 weeks for 16 weeks followed by a 16-week open-label phase and 20-week extension for safety evaluation[10]. The results demonstrated that IL-17A inhibition did not significantly reduce severity among the 18 subjects who completed the 16-week double-blind phase, nor did it increase mucocutaneous infections, though Th17-related biomarkers were not significantly reduced compared to baseline or placebo levels[10]. Importantly, five patients with 29-50% clinical improvement at week 32 requested drug continuation, suggesting that despite not meeting statistical significance for the primary endpoint, some individual patients experienced clinically meaningful benefit[10].

Earlier case series reporting on secukinumab use in specific ichthyosis subtypes documented more favorable responses than the formal clinical trial[1][1]. Luchsinger and colleagues reported a case series of four patients with Netherton syndrome treated with secukinumab during three to 12 months, documenting a reduction in the Ichthyosis Area Severity Index (IASI) from 55% to 88% after six months, with response being less pronounced in two patients with milder variant presentations of Netherton syndrome[1]. In another case report, a patient with Netherton syndrome who presented with facial erythema and frequent flares of ichthyosis on the trunk and extremities achieved complete clearance of facial erythema with secukinumab treatment, with only one mild flare of plaques occurring over three years of follow-up[1].

Dupilumab, a monoclonal antibody that blocks the interleukin-4 receptor and thereby inhibits both IL-4 and interleukin-13 (IL-13), is approved for treatment of atopic dermatitis and has been proposed for investigation in ichthyosis vulgaris based on clinical and phenotypic similarities between ichthyosis vulgaris and eczema[1][9]. Dupilumab inhibits IL-4 and IL-13 signaling, which stimulates Th2 cells that regulate pro-allergic adaptive immune responses, and normalization of filaggrin expression has been documented in atopic dermatitis patients treated with dupilumab, suggesting potential benefit in ichthyosis vulgaris[1][20]. Treatment with dupilumab resulted in decreased clinical severity of skin inflammation and marked improvement of pruritus in reported cases of congenital ichthyosis, though systematic data in ichthyosis vulgaris specifically remain limited[9].

Ustekinumab, a monoclonal antibody that indirectly inhibits IL-17 by targeting interleukin-12 (IL-12) and interleukin-23 (IL-23), has demonstrated clinical benefit in case reports of ichthyosis patients[1]. A case report documented a 15-year-old girl with Netherton syndrome treated with ustekinumab who experienced substantial skin improvement within four weeks with no relapse occurring after one year of follow-up[1]. In another case, two patients with SAM syndrome (caused by mutations in the desmoplakin gene) treated with ustekinumab achieved 58% and 59% reduction in total IASI scores with lower transepidermal water loss measured at 16 weeks[1]. A Phase I clinical trial (NCT04549792) involving 15 patients with ichthyosis (not specified as to subtype) is evaluating ustekinumab as an antibody targeting IL-12/IL-23[1].

Imsidolimab (ANB019), an antibody targeting the IL-36 receptor, is being investigated in a Phase II multicenter, randomized, double-blind, placebo-controlled study for treatment of ichthyosis (NCT04697056)[1][33]. This trial is examining imsidolimab's efficacy and safety in ichthyosis patients, representing an alternative cytokine targeting approach distinct from IL-17 or IL-23 inhibition[1][33].

### Small Molecule Kinase Inhibitors

Small molecules that inhibit protein kinases represent an emerging therapeutic category for ichthyosis, with the ability to interact with specific parts of targeted proteins and inhibit them without disrupting other pathway functions[1][1]. These agents are currently employed to treat inflammatory skin diseases such as apremilast in psoriasis treatment and baricitinib for atopic dermatitis, with promising results establishing proof-of-concept for their application in ichthyosis[1][1]. Regarding ichthyosis specifically, only limited in vitro and mouse-model studies have thus far been conducted, though the immunopathologic insights gained from these investigations suggest potential therapeutic utility that warrants clinical investigation[1].

### Serine Protease Inhibitors

QRX003, an investigational serine protease inhibitor formulated as a lotion, represents a novel therapeutic approach for ichthyosis that addresses skin barrier dysfunction through multiple mechanisms[23][26]. The serine protease inhibitor component of QRX003 functions as a potent anti-inflammatory and antioxidant agent, while the Invisicare delivery technology provides immediate protection against transepidermal water loss and environmental agents[23]. Uniquely, the Invisicare delivery technology both moisturizes the skin while providing a protective barrier, distinguishing it from purely emollient or purely occlusive approaches[23]. QRX003 is currently in Phase II/III clinical trial stage for treatment of ichthyosis[23]. An expanded access treatment protocol clinical trial (NCT06953466) is recruiting subjects with Netherton syndrome to evaluate QRX003 applied topically to the skin[26].

## Drug Repurposing Candidates with Supporting Evidence

### Janus Kinase Inhibitors

Janus kinase (JAK) inhibitors, originally developed for and approved in other inflammatory conditions, have emerged as promising candidates for repurposing in ichthyosis-related disorders based on immunopathologic insights and preliminary clinical evidence. A case series analysis of nine patients with refractory palmoplantar pustulosis (a condition frequently co-occurring with or overlapping ichthyosis manifestations) who remained unresponsive to conventional therapy and apremilast therapy reported exceptional responses when switched to tofacitinib 5 mg twice daily[34]. By the end of 12 weeks of tofacitinib treatment, all nine patients demonstrated significantly decreased palmoplantar pustulosis area and severity index (PPPASI) scores, with eight patients achieving PPPASI50 response (≥50% reduction from baseline) and one patient achieving PPPASI75 response (≥75% reduction from baseline)[34]. The smallest reduction in PPPASI score from baseline was 2.4 points, while the largest reduction was 16.4 points, and no serious adverse events were reported during treatment and follow-up[34].

The mechanism of action of tofacitinib as a pan-JAK inhibitor involves direct targeting of Janus kinases with blockade of IL-36 pathway-induced STAT3 phosphorylation, suppression of Th17 and Th2 cell differentiation, restoration of anti-inflammatory cytokine gene expression, and reduction in release of inflammatory cytokines including IL-7, IL-17A, and IL-22, thereby decreasing immune cell activation and infiltration[34]. Given these mechanisms directly addressing inflammatory pathways implicated in ichthyosis pathogenesis, JAK inhibitors warrant investigation specifically in ichthyosis vulgaris patients with insufficient response to conventional therapies. Selective JAK inhibitors including upadacitinib and baricitinib are increasingly employed for treatment of palmoplantar pustulosis through similar mechanisms of pro-inflammatory cytokine signaling inhibition, thereby modulating innate and adaptive immunity[34]. In one report, five patients with palmoplantar pustulosis treated with upadacitinib achieved PPPASI50 in three patients, PPPASI75 in two patients, and PPPASI90 in one patient by the 12-week treatment endpoint, with no serious adverse events occurring[34].

### Tumor Necrosis Factor-Alpha Inhibitors

Adalimumab, a monoclonal antibody targeting tumor necrosis factor-alpha (TNF-α), represents a TNF-inhibitory therapeutic that has been investigated in clinical trials for ichthyosis. A Phase II clinical trial (NCT02113904) involving 11 patients with Netherton syndrome evaluated adalimumab as an antibody targeting TNF-α and has been completed, though detailed efficacy results from this trial do not appear to be publicly available in the search results provided[1].

Earlier case reports documenting TNF-inhibitor therapy in ichthyosis patients provide preliminary evidence of potential efficacy. In a Netherton syndrome patient treated with infliximab therapy (another TNF-inhibitor), clearance of inflammation was achieved by one year with reduction in all measured cytokines in lesional skin except for IL-10 and TNF-alpha, both of which remained elevated[42]. While this patient experienced significant inflammatory improvement, notably the xerosis, scaling, and serum immunoglobulin E levels did not improve with TNF-inhibition alone, suggesting that TNF-alpha blockade may address certain but not all disease manifestations in ichthyosis[42].

### Interleukin-23 Inhibitors

Risankizumab, an interleukin-23-specific monoclonal antibody, has emerged as a particularly promising repurposing candidate based on compelling case reports demonstrating substantial clinical benefit in ichthyosis vulgaris patients. In a case report, a 60-year-old female patient with coexisting palmoplantar pustulosis and ichthyosis vulgaris was initially treated with guselkumab (another IL-23 inhibitor), which proved effective only for the palmoplantar pustulosis component while having minimal impact on ichthyosis vulgaris[7][5]. Subsequent treatment with risankizumab resulted in significant improvement of both palmoplantar pustulosis and ichthyosis vulgaris, rendering it a possible treatment approach for ichthyosis vulgaris[5]. This clinical observation is supported by mechanistic understanding that IL-23 signaling plays a critical role in development and function of IL-17-producing Th17 cells, and IL-23 pathway inhibition thus provides an alternative mechanism to directly target IL-17-producing cells[16][42].

## Adverse Events and Drug-Induced Ichthyosis

### Statin-Associated Ichthyosis

Multiple case reports have documented acquired ichthyosis developing in patients receiving lipid-lowering statin therapy, representing an important iatrogenic consideration in patients presenting with ichthyosis-like manifestations. A case report described a 79-year-old woman who presented with pruritic erythematous, scaly, and cracked skin lesions on both legs appearing more prominent on extensor surfaces, with clinical diagnosis of acquired ichthyosis made after she had initiated pitavastatin 2 mg daily three months prior for management of dyslipidemia[21]. The statin was suspected as causative agent, and while the patient initially declined discontinuation, dose reduction to pitavastatin 1 mg was undertaken along with topical treatment including urea cream and tretinoin cream[21]. Significant improvement in skin lesions was observed after six weeks of statin dose reduction, with complete remission occurring over several months, and causality assessment using the Naranjo adverse drug reaction probability scale yielded a score indicating a possible relationship between statin use and acquired ichthyosis[21].

A few additional case reports have described acquired ichthyoses related to other lipid-lowering drugs[21]. Sparsa and colleagues reported a 52-year-old woman who developed localized acquired ichthyosis on her arms after two months of treatment with pravastatin[21]. Lacour and colleagues also reported a case of acquired ichthyosis developing during fenofibrate treatment[21]. The proposed mechanism for statin-induced ichthyosis involves prolonged lipid-lowering therapy potentially causing disruption of epidermal lipid composition (though specific effects on epidermal cholesterol levels remain uncertain), thereby worsening barrier function and desquamation characteristics of the epidermis[21]. A previous randomized trial found that effects of statins on epidermal cholesterol concentrations were unlikely, and a recent systematic review demonstrated that skin cholesterol and serum cholesterol concentrations were not correlated, suggesting that an as-yet-unidentified disruption of lipid composition rather than mere diminution of cholesterol levels may be responsible for statin-induced ichthyosis[21]. The pathophysiological mechanisms explaining statin-induced ichthyosis remain incompletely understood, and whether ichthyosis represents a class effect of statins or is specific to individual statins continues to be uncertain[21].

### Medications with Ichthyosis as a Recognized Adverse Effect

Multiple medications are known to potentially cause ichthyosis as an adverse effect, including 5-fluorouracil (5-FU), bleomycin, bortezomib, clofazimine, clomiphene, certain formulations of lactate, phenyllactate, pregabalin, thalidomide, and trandolapril[14]. Additionally, sorafenib, a multikinase inhibitor used in cancer therapy, has been associated with substantial dermatologic side effects including hand-foot skin reactions, facial and scalp erythema, nail changes, and a generalized keratosis pilaris-like eruption resembling ichthyotic manifestations[22]. In cohorts of sorafenib-treated patients, hand-foot skin reactions occurred in 63% of prospectively followed patients and 78% of those in consultation cohorts, facial/scalp erythema in 63-68%, nail changes in 32-33%, and a generalized keratosis pilaris-like eruption in 21-41% of patients[22]. Histologic examination of the generalized keratosis pilaris-like eruption induced by sorafenib demonstrated typical keratosis pilaris appearance, supporting the hypothesis that sorafenib causes alterations in keratinocyte differentiation and proliferation pathways[22].

## Contraindications and Special Considerations

### Retinoids in Pregnancy and Childbearing-Potential Patients

Systemic retinoids, particularly isotretinoin and acitretin, possess well-established teratogenic effects that represent absolute contraindications to their use during pregnancy[15][15][29][15]. In the United States, isotretinoin is restricted to the iPLEDGE program, which requires strict pregnancy prevention measures in patients of childbearing potential, including documented negative pregnancy tests before initiation and monthly thereafter, use of effective contraception, and comprehension of teratogenic risks[15][15][15]. A retrospective cohort study analyzing oral retinoid exposure during pregnancy in South Korea using the NHIS mother-child linked healthcare database examined 3,894,184 pregnancies with 720 exposed to oral retinoids (isotretinoin, alitretinoin, and acitretin) between one month before pregnancy and delivery[29]. The results demonstrated a nonsignificant increase in risk of overall congenital malformations in oral retinoid-exposed pregnancies compared to unexposed pregnancies, and similarly elevated (though not statistically significant) risks for autism spectrum disorder and intellectual disorder in oral retinoid-exposed pregnancies, with wide confidence intervals intersecting the null[29]. Regarding adverse pregnancy outcomes including gestational diabetes mellitus, preeclampsia, and postpartum hemorrhage, no significant differences in risk were observed between oral retinoid-exposed and unexposed pregnancies[29].

The pattern of oral retinoid use demonstrated a marked decrease during pregnancy, with the most significant reduction occurring in the first two months of pregnancy, from 248 pregnancies to 64 pregnancies[29]. Importantly, isotretinoin use did not return to pre-pregnancy levels even one year after delivery, whereas use of alitretinoin and acitretin actually increased above pre-pregnancy levels in the postpartum period, potentially reflecting prescriber caution regarding isotretinoin's teratogenic potential[29]. Consensus recommendations specifically state that when choosing a systemic retinoid for treatment of disorders of cornification in patients of reproductive potential, isotretinoin should be considered first-line due to its shorter half-life compared to acitretin, which possesses a prolonged half-life of up to three years[15][15][19][15]. Accordingly, clinicians should consider transitioning patients of childbearing potential from acitretin to isotretinoin before puberty if pregnancy is anticipated[15][15][19][15].

### Retinoid Caution in Netherton Syndrome

Netherton syndrome, another congenital ichthyosis (though distinct from autosomal dominant ichthyosis vulgaris in its genetic basis and pathophysiology), represents a clinical context in which retinoid use requires particular caution due to the potential for paradoxical disease exacerbation. Utilization of retinoids in Netherton syndrome and other disorders with skin fragility, peeling skin, atopic diathesis, or excessive desquamation may actually exacerbate disease manifestations and should be used with particular caution or potentially avoided altogether[15][15][15]. This contraindication reflects the underlying pathophysiology of Netherton syndrome, in which mutations in SPINK5 lead to lympho-epithelial Kazal-type-related inhibitor (LEKTI) deficiency with resulting excessive protease activity and skin fragility, such that the keratinocyte-stimulating effects of retinoids may prove counterproductive[1][42].

## Combination Therapy Approaches

### Established Combination Regimens

The combination of ammonium lactate 12% lotion with physiological lipid-based barrier repair cream (EpiCeram®) represents an established, evidence-supported combination therapy approach for ichthyosis vulgaris that addresses complementary mechanisms of disease pathology[30][5]. The rationale for this combination approach involves treating both structural components of the epidermal barrier: the corneocytes (the "bricks" of the barrier) are addressed through the keratolytic action of ammonium lactate, which is a lactic acid component of the skin's natural moisturizing factors, while the intercellular lipid bilayer (the "mortar") is restored through provision of a physiological lipid mixture containing ceramides, cholesterol, and free fatty acids in the proper 3:1:1 molar ratio that simulates the normal intercellular lipid bilayer architecture[30][5]. In a multicenter, randomized, investigator-blinded clinical trial of 121 patients with moderate-to-severe atopic dermatitis, this physiological lipid-based cream demonstrated efficacy comparable to mid-strength topical corticosteroids with a favorable safety profile, suggesting potential utility in the ichthyosis vulgaris context where barrier repair represents a key therapeutic goal[30].

### Topical Retinoids Combined with Systemic Therapy

Current consensus recommendations specifically endorse the continuation of topical emollients and topical retinoids if tolerated during systemic retinoid therapy, as this combination may add to therapeutic benefit, allow lower oral retinoid dosing requirements, and potentially permit "retinoid holidays" with maintained disease control[15][15][15]. This approach acknowledges that lower systemic retinoid doses may be achieved through additive topical therapy while reducing cumulative long-term systemic toxicity exposure. Optimization of topical modalities should be pursued before escalating to systemic therapy in patients with mild disease, and these topical approaches should be maintained even after systemic therapy initiation in patients requiring pharmacological systemic intervention[15][15][15].

### Future Combination Strategies with Biological Therapeutics

The emerging recognition of IL-17 and IL-23 pathway involvement in ichthyosis pathogenesis raises potential for future combination strategies pairing cytokine-targeting biological therapeutics with conventional topical emollients and keratolytic agents. Such an approach would theoretically address both the inflammatory driver of disease and the barrier dysfunction consequent to both genetic and inflammatory mechanisms. Notably, in the secukinumab clinical trial, topical moisturizers were standardized across treatment groups, suggesting that such combination approaches are already being tested in clinical investigation[10].

## Discussion and Synthesis of Current Evidence

The therapeutic landscape for autosomal dominant ichthyosis vulgaris has evolved substantially over recent decades, transitioning from symptomatic management focused solely on removing scales and providing hydration, to increasingly mechanism-directed approaches targeting underlying genetic and immunopathologic drivers of disease. The filaggrin gene deficiency that underlies ichthyosis vulgaris, identified in 2006, catalyzed a paradigm shift in understanding disease pathophysiology and opened therapeutic avenues previously unexplored in this disorder[20][24][25]. Filaggrin's critical multifunctional roles in stratum corneum biogenesis and its role as a precursor for natural moisturizing factors establish it as an essential protein for normal skin barrier function, such that its deficiency inevitably results in compromised barrier properties[20][24].

The current standard-of-care approach for ichthyosis vulgaris maintains the foundational role of frequent bathing, environmental humidification, and daily emollient use, with first-line pharmacologic therapy employing urea-based creams at 10% concentration in combination with ceramides and natural moisturizing factors[5][15][5]. This topical first-line approach reflects the excellent safety profile, low cost, and demonstrated efficacy of these agents, with the recognition that many patients, particularly those with milder disease or seasonal variation in severity, achieve adequate disease control without systemic pharmacotherapy[2][5].

For patients with inadequate response to topical therapy or those experiencing significant quality-of-life impairment from disease symptoms, topical retinoids represent a rational escalation that maintains relatively low systemic toxicity exposure while addressing abnormal keratinization directly[15][15][15]. Consensus recommendations emphasize the dose-dependent nature of retinoid efficacy, with the optimal systemic dose representing the lowest amount that achieves desired therapeutic effect while maintaining acceptable toxicity profiles[15][15][15]. Long-term adverse effects of systemic retinoids on bone and ocular health warrant careful consideration in patients requiring prolonged therapy, though consensus recommendations acknowledge that potential bone toxicity should not automatically preclude long-term systemic retinoid use if clear clinical benefit exists[15][15][15].

The recent investigational focus on immune-modulatory biological therapeutics targeting IL-17 and IL-23 pathways reflects accumulated evidence that Th17-skewed inflammation characterizes ichthyotic skin, resembling immunopathologic patterns observed in psoriasis[1][1][42]. This recognition has prompted repurposing of biologics originally developed for psoriasis and other inflammatory conditions toward ichthyosis, following a rational translational pathway from laboratory immunopathologic discovery to clinical investigation[42]. While the Phase III trial of secukinumab did not meet its primary efficacy endpoint across all ichthyosis subtypes studied[10], the positive clinical experience with IL-23 inhibition (particularly risankizumab) in case reports and early clinical observations suggests that subsets of ichthyosis patients may benefit from this therapeutic approach, warranting further investigation to identify patient subsets most likely to respond[5][42].

The recognition that drug-induced ichthyosis can occur with statin therapy, sorafenib, and other medications represents an important clinical consideration in the differential diagnosis of acquired ichthyosis presentations. Recognition of potential iatrogenic causes enables consideration of dose reduction or drug substitution when alternative therapeutic options exist, as demonstrated in the case of statin-induced ichthyosis where dose reduction led to disease resolution[21].

## Conclusion and Future Directions

Autosomal dominant ichthyosis vulgaris, affecting approximately one person per 250-300 individuals, represents a common inherited disorder of keratinization with significant potential for quality-of-life impairment through its effects on skin comfort, appearance, and physical functioning. The absence of curative therapy necessitates lifelong symptomatic and mechanistic management approaches tailored to individual disease severity and response patterns. Current approved therapeutic approaches include topical emollients containing urea and ceramides as first-line agents with excellent safety profiles, topical retinoids for patients inadequately controlled with emollients alone, and systemic retinoids (isotretinoin or acitretin) for patients with severe disease or inadequate response to topical approaches[2][5][15][15][15][5].

Investigational approaches currently in clinical development include topical isotretinoin formulations utilizing novel delivery systems to enhance efficacy while reducing systemic toxicity[11][18][23], biological therapeutics targeting interleukin pathways implicated in disease pathogenesis[1][1], and serine protease inhibitors with integrated barrier protection[23]. The evidence supporting repurposing of Janus kinase inhibitors, interleukin-23 inhibitors, and tumor necrosis factor-alpha inhibitors from other inflammatory conditions provides rationale for further investigation of these agents in ichthyosis vulgaris patients with insufficient response to conventional therapy[34][5][42].

Future therapeutic development should prioritize large-scale clinical trials adequately powered to assess efficacy of immune-modulatory biologics in genotype-confirmed ichthyosis vulgaris populations, systematic investigation of combination therapy approaches pairing topical and systemic agents, and longitudinal studies examining long-term safety and efficacy profiles of novel therapeutic approaches. Recognition of underlying skin barrier deficiency as a central pathophysiologic feature common to ichthyosis vulgaris and associated atopic conditions suggests potential value of identifying shared therapeutic approaches applicable across this spectrum of conditions[20][24]. Additionally, investigation of genetic and environmental modifiable factors that affect bone health, skin barrier integrity, and inflammatory responses may identify preventive or adjunctive interventions that enhance quality of life in ichthyosis vulgaris patients beyond direct disease-modifying effects.