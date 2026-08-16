---
provider: perplexity
model: sonar-deep-research
cached: false
start_time: '2026-04-04T10:28:15.724267'
end_time: '2026-04-04T10:30:24.051997'
duration_seconds: 128.33
template_file: templates/drug_disease_research.md
template_variables:
  disease_name: DiGeorge syndrome
  mondo_id: MONDO:0008564
provider_config:
  timeout: null
  max_retries: 3
  parameters:
    allowed_domains: []
    reasoning_effort: medium
    search_domain_filter: []
    return_citations: true
    temperature: 0.0
citation_count: 47
---

## Question

# Drug-Disease Association Research Template

## Target Disease
- **Disease Name:** DiGeorge syndrome
- **MONDO ID:** MONDO:0008564

## Research Objectives

Please provide a comprehensive research report on **drug therapies for DiGeorge syndrome**.
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

# Drug Therapies for DiGeorge Syndrome: Comprehensive Research Report on Approved, Investigational, and Repurposing Candidates

## Executive Summary

DiGeorge syndrome, clinically known as 22q11.2 deletion syndrome (22q11.2DS), represents one of the most common microdeletion syndromes affecting approximately one in 3,000 to 9,700 individuals[2]. This genetic disorder results from a hemizygous deletion on chromosome 22q11.2, leading to a constellation of congenital malformations and immunological deficiencies that profoundly impact therapeutic management[2][39]. The clinical heterogeneity of this syndrome—ranging from asymptomatic presentations to life-threatening complete athymia—necessitates a multifaceted pharmacological approach spanning immunological reconstitution, infection prevention, endocrine management, and psychiatric intervention. Recent advances in regenerative medicine have yielded the first FDA-approved therapy specifically targeting the immunological component of the disease, while emerging evidence suggests promising applications for repurposed medications addressing previously intractable manifestations such as neuropsychiatric disease. This comprehensive report synthesizes current knowledge on therapeutic options, examining approved pharmacological treatments, investigational agents in various stages of clinical development, off-label applications with supportive evidence, and the crucial contraindications that clinicians must recognize to ensure patient safety.

## Approved Pharmacological Therapies for DiGeorge Syndrome

### Thymic Regenerative Therapy: RETHYMIC (Allogeneic Processed Thymus Tissue-agdc)

The most significant therapeutic advance in DiGeorge syndrome has been the development and FDA approval of RETHYMIC, a regenerative medicine product representing allogeneic cultured postnatal thymus tissue derived from organ donors undergoing cardiac surgery[9][26]. On October 8, 2021, the FDA granted approval to RETHYMIC, designating it as the first and only FDA-approved treatment for immune reconstitution in pediatric patients with congenital athymia[9][26]. This regenerative tissue therapy culminated from decades of pioneering research initiated by Dr. M. Louise Markert and colleagues at Duke University, who developed intricate processes for processing thymic tissue to ensure immunogenicity while maintaining functional capacity[9].

The mechanism of action for RETHYMIC involves surgical implantation of processed thymic tissue into the quadriceps muscle, where the transplanted tissue selects and educates donor T-lymphocytes to recognize and attack pathogens[9][26]. Critically, this therapy does not require human leukocyte antigen (HLA) matching between donor and recipient, substantially expanding its applicability compared to traditional allogeneic transplantation approaches[39]. The tissue undergoes sophisticated laboratory processing that expands thymic epithelial cells and removes donor T cells, thereby minimizing the risk of graft-versus-host disease while preserving the thymic microenvironment necessary for T-cell education[13][26].

Clinical efficacy data supporting RETHYMIC approval derive from ten prospective single-arm, open-label studies conducted from 1993 to 2020, encompassing 105 surgically implanted patients[26]. The Efficacy Analysis Set included 95 patients, while 105 patients comprised the Safety Analysis Set, allowing comprehensive assessment across both dimensions[26]. Kaplan-Meier survival estimates at one year and two years post-implantation were 77% (95% confidence interval 0.670–0.841) and 76% (0.658–0.832), respectively[26]. Notably, for patients who survived the first year post-implantation, the long-term survival rate reached 94% at a median follow-up of 10.7 years, with some patients now in their 20s in good health demonstrating sustained immune competence[9][26].

T-cell reconstitution following RETHYMIC implantation demonstrates a characteristic temporal pattern whereby naïve T-cell levels, measured using flow cytometry at six, twelve, and twenty-four months post-implantation, begin reconstituting during the first year with durable increases persisting through year two[26]. Measurable reductions in infection frequency over time during the first two years after treatment achieved statistical significance (p<0.001)[26]. Among the 105 patients enrolled in clinical studies, 29 patients died, including 23 deaths occurring within the first year (< 365 days) post-implantation, predominantly from complications related to the underlying multisystem disorder rather than transplant-related factors[26].

Adverse events associated with RETHYMIC include hypertension, cytokine release syndrome, rash, hypomagnesemia, renal impairment or failure, thrombocytopenia, and graft-versus-host disease (GVHD)[26]. The product labeling specifically warns that RETHYMIC may cause or exacerbate pre-existing GVHD, with identified risk factors including atypical complete DiGeorge anomaly phenotype, prior hematopoietic cell transplantation, and maternal engraftment[26]. Approximately 10% to 15% of recipients develop autoimmune thyroiditis, while other autoimmune complications including immune cytopenias, autoimmune enteropathy, hepatitis, and nephrotic syndrome occur with lower frequencies[39][39].

Despite the substantial clinical benefit demonstrated by RETHYMIC, significant limitations persist regarding accessibility and donor tissue availability. The therapy requires specialized surgical expertise for implantation and relies on a limited supply of donor thymic tissue from infants undergoing cardiac surgery. Furthermore, the procedure carries operative risks inherent to surgical intervention, and the post-transplant period requires intensive immunological monitoring to detect and manage autoimmune complications.

### Immunoglobulin Replacement Therapy

While not a single-drug therapy developed specifically for DiGeorge syndrome, intravenous immunoglobulin (IVIG) and subcutaneous immunoglobulin (SCIG) represent crucial components of the standard pharmacological armamentarium for managing antibody deficiency in this population[17][39]. Approximately 2-3% of patients with DiGeorge syndrome have documented hypogammaglobulinemia warranting immunoglobulin replacement therapy[17]. Current guidelines recommend initiating replacement therapy at doses between 400 and 600 mg/kg administered intravenously every 3 to 4 weeks, or the equivalent dose distributed as once or twice weekly subcutaneous infusions, with the goal of achieving trough serum IgG levels around 600-800 mg/dL or greater than 500 mg/dL[39].

The immunological rationale for immunoglobulin replacement in DiGeorge syndrome derives from observations that patients with T-cell deficiency develop diminished B-cell function despite having near-normal peripheral B-cell numbers, resulting in reduced antibody production and impaired specific antibody responses to pathogens and vaccines[25][36]. The antibody deficiency in DiGeorge syndrome may be particularly pronounced for IgM, predisposing patients to increased susceptibility to gram-negative bacterial infections and those organisms against which complement-dependent opsonization is critical[36]. Studies have demonstrated that IVIG therapy, particularly when initiated in patients with recurrent sinopulmonary infections, deficits in serum immunoglobulin levels, and insufficient diphtheria and tetanus titers following prior immunization, results in decreased infection frequency, reduced antibiotic requirements, and decreased hospitalizations[17].

Two routes of administration are available in the United States: intravenous (IVIG, approved for use in 1979) and subcutaneous (SCIG, approved for use in 2006)[17]. Early studies demonstrated no significant differences in efficacy between IVIG and SCIG, and no difference in quality of life when either treatment is administered at home[17]. However, SCIG may provide advantages including more consistent IgG levels and patient preference for weekly subcutaneous infusions compared to hospital-based intravenous settings, offering greater independence and flexibility in dosing[17]. Emerging formulations incorporating recombinant human hyaluronidase (rHuPH20) facilitate subcutaneous absorption through increased tissue permeability, with one open-label multicenter study demonstrating that rHuPH20-facilitated SCIG was effective, safe, and pharmacokinetically equivalent to IVIG with fewer systemic reactions[17].

## Investigational Drugs and Clinical Trial Pipeline

### Thymic Implantation Enhancement Strategies

Beyond the approved RETHYMIC therapy, ongoing investigations are focused on enhancing thymic implantation outcomes through immunological optimization. Recent research has identified interleukin-7 (IL-7) as a promising adjunctive therapy to improve T-cell development following thymus implantation[41]. In murine models of congenital athymia (Foxn1-nude mice), treatment with recombinant IL-7 (rIL-7) dramatically enhanced T-cell reconstitution compared to vehicle-treated controls[41].

The mechanistic basis for IL-7's benefit involves promotion of homeostatic T-cell expansion through IL-7 receptor-mediated signaling. IL-7-treated mice demonstrated substantially higher percentages of newly developed T cells expressing the IL-7 receptor (IL-7R), rendering them more capable of homeostatic proliferation[41]. Absolute T-cell counts in rIL-7-treated animals increased more robustly than vehicle-treated controls, continuing to rise out to twelve weeks of observation[41]. Most significantly, rIL-7 expanded the T-cell receptor repertoire by increasing the number of unique clones within the TCRβ gene, a critical parameter for protective immunity given that restricted T-cell repertoires in DiGeorge patients contribute to increased infection susceptibility[41].

While these preclinical findings are compelling, clinical trials incorporating IL-7 into thymic implantation protocols have not yet been completed or published. The theoretical advantages of IL-7 supplementation include potential improvement in immune reconstitution kinetics, expansion of T-cell diversity, and normalization of the naïve T-cell compartment—parameters that remain suboptimal in current thymic implantation recipients. Future clinical investigation of this approach may substantially improve outcomes for patients receiving RETHYMIC or other thymic therapies.

### Bone Marrow and Hematopoietic Cell Transplantation

Hematopoietic cell transplantation (HCT) represents an alternative to thymic implantation for immune reconstitution in complete DiGeorge syndrome, particularly for patients with suitable HLA-matched sibling donors[18][33]. A multicenter survey examining outcomes of HCT in 17 patients with complete DiGeorge anomaly revealed overall survival of 41% at a median follow-up of 5.8 years (range 4–11.5 years)[18]. Among those who survived beyond the initial transplant period, median CD3 and CD4 counts were 806 (range 644–1,224) and 348 (range 225–782) cells/mm³, respectively, with normalization of mitogen responses despite persistently low CD4+/CD45RA+ naive T cells[18].

Long-term follow-up of two patients who received bone marrow transplants in the neonatal period from HLA-matched siblings demonstrated that both individuals, now in their 20s and in good health, exhibit continuous hematopoietic engraftment with mixed chimerism, normal T-cell function, and humoral immunity[33]. Circulating T cells demonstrated a memory phenotype with restricted repertoire and absence of T-cell receptor excision circles (TRECs), indicating T-cell reconstitution through expansion of the donors' mature T-cell pool rather than de novo thymic output[33]. Although their immune systems remained restricted in T-cell diversity, they maintained substantial protection against infection and responded appropriately to vaccines[33].

Comparative analysis of transplant donor sources revealed superior outcomes with HLA-matched sibling donors (62% overall survival when survey data are combined with published reports) compared to unrelated donor peripheral blood or marrow transplantations (33% survival) or unrelated donor cord blood transplantations (33% survival)[18]. The current consensus suggests that thymic transplantation offers superior immune reconstitution compared to HCT, particularly regarding naive T-cell development and T-cell receptor diversity, with 75% survival rates compared to 62% for sibling donor HCT in selected populations[18].

### Immunosuppressive Agents for Autoimmune Manifestations

Emerging evidence supports the therapeutic potential of biological immunosuppressive agents, particularly mTOR inhibitors and TNF-α inhibitors, for managing autoimmune complications in DiGeorge syndrome patients. A 2024 case series published in Frontiers in Pediatrics documented safety and efficacy of biologic disease-modifying antirheumatic drugs (bDMARDs) in patients with DiGeorge syndrome presenting with juvenile idiopathic arthritis (JIA) and inborn errors of immunity[6][6].

One case involved a 6-year-old boy with 22q11.2DS who developed oligoarticular JIA at age 2 years[6]. Initial treatment with nonsteroidal anti-inflammatory drugs (NSAIDs) and methotrexate proved ineffective, as did subsequent glucocorticoid therapy[6]. Treatment with etanercept (TNF-α inhibitor) at a dose of 0.8 mg/kg/week resulted in stable and persistent remission achieved only after 10 months, with the patient maintaining remission on continued etanercept while methotrexate was gradually discontinued[6].

A second case described a 6-year-old girl with 22q11.2DS who developed oligoarticular JIA at age 3 years 11 months, complicated by severe uveitis, cataract, and iridolenticular synechia[6]. She failed to respond to NSAIDs, methotrexate, and joint injections, subsequently demonstrating clinical remission following treatment with adalimumab and multiple joint injections[6].

A third case involved a patient with 22q11.2DS and JIA who failed to respond to anti-TNF-α therapy, tocilizumab, and abatacept but achieved remission after 4 months of combination therapy with sirolimus plus abatacept[6]. Over a follow-up period of at least 16 months (range 16–38 months), treatment with biological immunosuppressors did not precipitate significant adverse events or severe infections[6]. Importantly, none of the bDMARDs were discontinued due to adverse events; rather, discontinuation occurred due to inadequate efficacy in some cases[6].

A separate case report documented the efficacy of sirolimus monotherapy in treating a patient with partial DiGeorge syndrome presenting with refractory immune cytopenia and autoimmune lymphoproliferative syndrome (ALPS)-like manifestations[30]. The patient demonstrated decreased proportion of naive T cells and elevated double-negative T cells (DNTs)—a pathogenic population implicated in autoimmune dysregulation. After 3 months of sirolimus monotherapy, the patient achieved decreased spleen size and restrained lymph node expansion, with normalization of immunophenotypic abnormalities including decreased DNT cells (from 4.4% to 3.2%) and elevation of regulatory T cells (from 3.7% to 5.2%)[30]. Sirolimus appeared highly effective and safe, suggesting mTOR inhibition as a beneficial therapeutic strategy for managing immune dysregulation in DiGeorge syndrome.

The mechanistic basis for sirolimus efficacy in autoimmune DiGeorge syndrome relates to its inhibition of the mammalian target of rapamycin (mTOR) intracellular signaling pathway, which controls T-cell proliferation, differentiation, and metabolic function[30]. The mTOR inhibition corrects proinflammatory double-negative T-cell differentiation and promotes expansion of regulatory T cells, restoring immunological homeostasis[30].

## Drug Repurposing Candidates with Emerging Evidence

### Bezafibrate: Mitochondrial Activation for Neuropsychiatric Disease Prevention

A landmark study conducted jointly by researchers from the University of Pennsylvania School of Veterinary Medicine and Children's Hospital of Philadelphia, published in *Science Translational Medicine*, has identified a novel pharmacological approach to addressing the significant psychiatric burden in DiGeorge syndrome[1][1]. The researchers discovered that mitochondrial dysfunction in the blood-brain barrier (BBB) may drive neuropsychiatric disease in patients with 22q11.2DS, and that a class of FDA-approved cholesterol drugs could potentially be repurposed to treat this dysfunction[1][1].

The clinical context for this discovery is critical: patients with 22q11.2DS experience a 25-fold higher risk of developing psychosis, with one in four individuals developing schizophrenia[1][1]. Despite the profound psychiatric burden affecting quality of life and prognosis, current antipsychotic medications remain relatively ineffective in this population, with limited treatment options available[24][24].

Using stem cell-derived brain microvascular endothelial cells from patients with 22q11.2DS, researchers identified impairment in blood-brain barrier function, suggesting a leaky barrier phenotype[1][1]. The research team demonstrated that treatment with bezafibrate—a cholesterol-lowering drug that also stimulates mitochondrial activity—improved BBB function in both in vitro stem cell-derived systems and in preclinical models[1][1]. Mechanistically, bezafibrate appears to restore mitochondrial oxidative function, thereby ameliorating the energy deficiency that compromises blood-brain barrier integrity in these patients[1][1].

Notably, in preclinical models, bezafibrate treatment also corrected deficits in social memory, an abnormality intimately tied to both blood-brain barrier dysfunction and schizophrenia pathophysiology[1][1]. These findings suggest that this class of drugs could potentially be repurposed—pending validation in clinical trials—to address the neuropsychiatric manifestations of DiGeorge syndrome[1][1]. While the study focused specifically on 22q11.2DS, the researchers believe the implications could extend to other neuropsychiatric conditions characterized by blood-brain barrier dysfunction and mitochondrial energy deficiency[1][1].

The repurposing of bezafibrate presents several advantages for patients with DiGeorge syndrome. First, the drug is already FDA-approved and available, reducing the timeline to clinical application. Second, the mechanism of action—activating mitochondrial biogenesis through PGC-1α activation—targets a fundamental pathophysiological abnormality rather than merely suppressing symptoms. Third, preliminary evidence suggests correction of social memory deficits, potentially addressing a core feature of the schizophrenia spectrum presentations seen in this population.

### Prophylactic Antimicrobial Agents

Given the significant T-cell deficiency in many DiGeorge syndrome patients, particularly those with CD3 counts below 500 cells/mm³, prophylactic antimicrobial therapy constitutes an essential pharmacological component of disease management[7][7][39]. Trimethoprim-sulfamethoxazole (TMP-SMX), administered twice daily three days per week, remains the preferred agent for *Pneumocystis jirovecii* pneumonia (PCP) prophylaxis, with reported prevention rates of 93-100%[15]. However, alternative agents are available and increasingly utilized.

Pentamidine represents an effective second-line alternative for PCP prophylaxis, available as either intravenous or inhaled formulations[15]. A retrospective study analyzing pediatric patients receiving pentamidine for PCP prophylaxis identified breakthrough PCP in only 0.5% of patients (0.03 cases per 1,000 patient days), with no episodes of probable or proven PCP despite long-term prophylaxis in patients receiving hematopoietic stem cell transplantation or solid organ transplantation[15]. These breakthrough rates (0% to 1.3% overall) are comparable to published rates for TMP-SMX prophylaxis, establishing pentamidine as a reasonable alternative for patients with TMP-SMX intolerance or allergy[15]. Reported adverse effects of pentamidine include dysglycemia, hypotension, phlebitis, fatigue, dysgeusia, nephrotoxicity, electrolyte imbalances, allergic reactions, hepatotoxicity, and pancreatitis[15].

For patients with advanced immunodeficiency (CD4 counts <50 cells/mm³, though less commonly seen in DiGeorge syndrome than in acquired immunodeficiency syndrome), azithromycin prophylaxis at a dose of 1,200 mg once weekly significantly reduces the risk of disseminated *Mycobacterium avium* complex (MAC) infection[34]. In a randomized, double-blind, placebo-controlled multicenter trial, azithromycin prophylaxis reduced MAC infection development from 24.7% in placebo recipients to 10.6% in azithromycin-treated patients (hazard ratio 0.34, p=0.004)[34]. Additionally, episodes of non-MAC bacterial infection per 100 patient-years were significantly reduced in azithromycin recipients (43 per 100 patient-years) compared to placebo recipients (88 per 100 patient-years; relative risk 0.49)[34]. The most common adverse effect was gastrointestinal, reported by 78.9% of azithromycin recipients compared to 27.5% of placebo recipients[34].

Valacyclovir provides prophylaxis against herpes simplex virus (HSV) and varicella-zoster virus (VZV) reactivation in severely immunocompromised patients[16]. Dosing for chickenpox treatment in children ranges from 20 mg/kg administered three times daily for five days[16]. The medication's pharmacokinetics involve conversion in the body to acyclovir, the active anti-herpes agent[16].

Fluconazole serves as a crucial antifungal prophylactic agent for preventing *Candida albicans* infections in severely immunocompromised patients[21]. However, clinicians must be cognizant of its extensive drug-drug interactions, as fluconazole inhibits hepatic cytochrome P450 enzymes, potentially elevating levels of medications including warfarin, clopidogrel, calcium channel blockers, NSAIDs, and many others[21]. Concomitant use of fluconazole with warfarin can significantly elevate INR, requiring increased monitoring and potential warfarin dose reduction[21].

## Contraindications and Problematic Drug Interactions

### Live Attenuated Vaccines: Nuanced Safety Profile Based on T-Cell Status

The contraindication of live attenuated vaccines in DiGeorge syndrome represents one of the most important pharmacological safety considerations, yet the degree of contraindication depends critically on residual T-cell function. Traditional guidelines had universally contraindicated live vaccines in patients with T-cell deficiencies, including those with DiGeorge syndrome[8]. However, accumulating evidence has prompted modification of these recommendations.

Current guidance from the Infectious Disease Society of America (IDSA) and American Academy of Pediatrics (AAP) recommends that MMR and varicella (VAR) vaccines should be considered in patients with DiGeorge syndrome who demonstrate adequate T-cell counts (≥500 and ≥200 CD3 and CD8 T cells/mm³, respectively) and exhibit normal mitogen responses[8]. Conversely, patients with DiGeorge syndrome displaying CD3 T-cell counts <500 cells/mm³ should not receive any live vaccine[8].

A retrospective analysis of live immunization safety in patients with DiGeorge syndrome in Korea examined adverse events following live vaccine administration and found that live vaccines were well tolerated by patients with partial DiGeorge syndrome[8]. Serious adverse events, including intensive care unit hospitalization, death, or diseases attributable to vaccine strains, were not observed[8]. This study emphasized that immune screening tests should be conducted before all live vaccinations in DiGeorge patients to evaluate T-cell immunity, and that live vaccines are beneficial in patients with partial DiGeorge syndrome demonstrating reasonable T-cell function[8].

The rotavirus vaccine, however, represents a special contraindication even in partial DiGeorge syndrome due to the risk of prolonged viral shedding in severely immunocompromised patients, potentially transmitting vaccine virus to close contacts[39].

### Antipsychotics: Seizure Risk in 22q11.2 Deletion Syndrome

The use of antipsychotic medications in DiGeorge syndrome patients with concurrent psychiatric illness presents a particular challenge given the elevated seizure risk associated with these agents in this population. The prevalence of both epilepsy and psychosis in 22q11.2DS substantially exceeds that in the general population[24][24]. A recent study on adults with 22q11.2DS identified the use of antipsychotics and antidepressants as the most common trigger for provoked seizures, despite a history of hypocalcemia being similarly common[24][24].

Antipsychotics demonstrate varying seizure risk profiles in the context of DiGeorge syndrome. Clozapine demonstrates the highest incidence of seizure-related adverse effects when used in patients with schizophrenia spectrum disorders, including generalized tonic-clonic seizures, focal seizures, myoclonus, rigidity, and tremors, with seizure being the most severe and most common neurological complication[24][24]. Olanzapine and quetiapine similarly carry substantial seizure risk in DiGeorge patients, though somewhat lower than clozapine[24][24]. Risperidone carries lower risk of inducing seizure activity in epileptic patients compared to other antipsychotics[24][24]. Aripiprazole, paliperidone, and ziprasidone demonstrate only slight increases in seizure incidence compared to placebo[24][24].

Clinical management of psychosis in DiGeorge syndrome requires careful drug selection with detailed neurological assessment and consultation. High doses of psychotic drugs represent a particular risk factor for convulsion[24][24]. Following seizure events, antipsychotics should be switched to alternatives or discontinued, with concurrent initiation of anticonvulsant therapy when appropriate[24][24]. Antiepileptic drugs such as lamotrigine have demonstrated efficacy in controlling seizures while permitting ongoing antipsychotic therapy for psychotic symptom management[24][24]. Additional monitoring including electrocardiography and electroencephalography should be repeated at consecutive intervals during antipsychotic treatment[24][24].

Quetiapine and olanzapine appear more efficacious than risperidone for treating schizophrenia in DiGeorge syndrome patients, demonstrating greater effectiveness despite the seizure risk[24][24]. Clozapine, while highly effective for schizophrenia in the general population and DiGeorge syndrome, carries the highest seizure risk and therefore should be reserved for treatment-resistant cases where potential benefit outweighs substantial risk[24][24]. These agents require lower starting doses compared to those used in idiopathic schizophrenia, with careful observation for neurological side effects[39].

## Management of Specific Metabolic and Endocrine Complications

### Hypoparathyroidism and Hypocalcemia Management

Hypoparathyroidism, affecting approximately 0.5-6.6% of the general population but considerably more common in DiGeorge syndrome, presents unique pharmacological management challenges[22][22]. Women of childbearing age with DiGeorge syndrome and hypoparathyroidism face additional complexities during pregnancy when calcium and vitamin D supplementation requirements change substantially due to altered maternal physiology[22][22].

Standard treatment involves calcium supplementation with elemental calcium at doses of 30 to 75 mg/kg/day divided into three to four daily doses, combined with active vitamin D (calcitriol) at doses of 0.25 to 2 µg/day[39]. Current recommendations target maintenance of corrected serum calcium between 9.0 mg/dL and 9.5 mg/dL, though clinical experience suggests that maintaining calcium in the low-normal range improves outcomes[22]. In vitro studies indicate that untreated hypocalcemia may increase uterine irritability and the risk of spontaneous miscarriage[22].

Management during pregnancy requires heightened vigilance due to multiple factors: increased 1,25-dihydroxyvitamin D and PTHrP partially compensate for absent parathyroid hormone, but these adaptations are often insufficient, requiring supplementation to maintain normocalcemia[22]. A case report documented successful management of hypoparathyroidism in pregnancy following delayed DiGeorge syndrome diagnosis, wherein the patient received moderate-dose cholecalciferol (vitamin D3 5,000 units daily) and oral calcium citrate (500 mg twice daily), with an additional 1,200 mg of dietary calcium to maintain serum calcium between 2.25 mmol/L and 2.38 mmol/L[22][22].

Importantly, calcitriol (activated vitamin D) carries substantial risk of hypercalcemia in pregnancy due to multiple activating pathways for 1,25(OH)₂D, requiring close monitoring for dose titration[22]. The case report opted for moderate-dose cholecalciferol rather than calcitriol due to the patient's elevated 1,25(OH)₂D levels and absence of symptoms, effectively correcting the underlying storage deficiency while avoiding hypercalcemia risk[22][22].

During inpatient management at 37 weeks gestation, calcium monitoring every 6 hours maintained corrected calcium between 2.25 mmol/L and 2.35 mmol/L on a regimen of calcium carbonate 1,500 mg/day and reduced vitamin D3 at 4,000 units[22]. Post-partum management required further dose adjustments to achieve sustained normalization[22].

## Management of Immune Dysregulation and Autoimmune Complications

### Plasma Exchange for Refractory Autoimmune Hemolytic Anemia

DiGeorge syndrome patients demonstrate elevated risk for autoimmune cytopenias, including autoimmune hemolytic anemia (AIHA) and immune thrombocytopenia (ITP)[35]. While first-line therapy for AIHA consists of corticosteroids to which most patients show response, relapses occur frequently and are typically managed with splenectomy or rituximab[35]. However, a small proportion of patients develop severe, refractory disease unresponsive to conventional therapeutic strategies[35].

A case report described a 20-year-old female with DiGeorge syndrome who presented with life-threatening autoimmune hemolytic anemia despite a prior history of immune thrombocytopenia[35]. Standard first-line and second-line therapeutic modalities proved ineffective in controlling her disease, leading to consideration of plasma exchange therapy[35]. Plasma exchange successfully resolved hemolysis, and at one-year follow-up, the patient remained clinically well without signs of hemolysis[35]. This case demonstrates that plasma exchange therapy, though not typically employed for AIHA management, may represent a valuable therapeutic tool in truly refractory cases where standard immunosuppressive strategies have failed[35].

### Rituximab B-Cell Depletion for Autoimmune Manifestations

While not specifically studied as monotherapy in DiGeorge syndrome, rituximab—a chimeric monoclonal antibody that specifically targets CD20 antigen and induces B-cell depletion—may offer therapeutic potential for autoimmune manifestations. Published case series in general autoimmune populations demonstrated efficacy in autoimmune hemolytic anemia and chronic thrombocytopenia, both complications observed in DiGeorge syndrome[29]. All seven patients examined demonstrated marked B-cell depletion, with three patients achieving complete hematologic response[29]. The hematologic improvement appeared prompt, becoming evident by the second or third infusion of rituximab at 375 mg/m², with treatment tolerance being satisfactory and no infections or other late events registered during the observation period[29]. Response duration in these patients ranged from 13 to 96+ weeks, suggesting durable benefit[29].

## Combination Therapy Regimens and Complex Management Strategies

### Antiepileptic and Antipsychotic Combination Therapy

Complex management strategies combining antiepileptic drugs with antipsychotics have emerged as clinically necessary given the high prevalence of both psychosis and seizure disorders in DiGeorge syndrome[24][24]. A case report documented a patient with 22q11.2DS and concurrent psychotic disorder and seizure disorder who was initially treated with olanzapine but experienced convulsion on the fourth day of administration[24]. After convulsion cessation, the patient was restarted on olanzapine at 30 mg, but experienced recurrent seizure[24]. Lamotrigine, an anticonvulsant with mood-stabilizing properties, was initiated and titrated to 75 mg twice daily, after which no further seizures occurred and psychotic symptoms improved[24]. The patient was ultimately discharged seizure-free on a regimen of olanzapine 30 mg daily and lamotrigine 150 mg daily[24].

This case illustrates the principle that concurrent antiepileptic therapy may permit safe administration of antipsychotics in DiGeorge patients at risk for seizure-related adverse events. The combination strategy addresses both the primary psychiatric pathology requiring antipsychotic therapy and the seizure susceptibility inherent to 22q11.2DS populations.

### Multimodal Immunosuppression for ALPS-Like Manifestations

As documented in the case report of sirolimus monotherapy achieving remission in partial DiGeorge syndrome with autoimmune lymphoproliferative syndrome-like features, combination immunosuppressive regimens may be employed when monotherapy proves inadequate[30]. A third patient in the pediatric rheumatology series failed to respond to anti-TNF-α inhibitors, tocilizumab, and abatacept monotherapy but achieved remission after 4 months of combination therapy with sirolimus plus abatacept[6]. This observation suggests that combining mechanistically distinct immunosuppressive approaches—mTOR inhibition via sirolimus and IL-6 receptor antagonism via abatacept—can achieve synergistic benefit when individual agents prove insufficient[6].

## Emerging Research Directions and Future Therapeutic Opportunities

### Complement System Modulation

Recent immunological investigation has identified dysregulation of the complement system as a potentially targetable pathophysiological mechanism in DiGeorge syndrome[12]. A study of 64 patients with 22q11.2 deletion syndrome found significantly raised plasma levels of C3bc (complement activation product) compared with 45 healthy controls, with median values of 9.3 CAU/mL in patients versus 7.3 CAU/mL in controls (p=0.007)[12]. This increase in complement activation was specifically associated with the presence of psychiatric disorders in patients[12].

Patients with neuropsychiatric disorders (n=6) demonstrated significantly raised serum levels of C3bc with median value of 12 CAU/mL (interquartile range 9–16 CAU/mL) compared to healthy individuals (p=0.010)[12]. Patients with neurodevelopmental disorders specifically had significantly raised plasma levels of C3bc with median value of 16 CAU/mL (interquartile range 12–17 CAU/mL) compared to patients without psychiatric disorders (p=0.019)[12].

These findings suggest that complement-modulating therapies, including complement cascade inhibitors, might represent novel therapeutic targets for addressing the psychiatric burden in DiGeorge syndrome. However, clinical trials exploring this approach have not yet been initiated or published.

### Microbiome Manipulation Strategies

Emerging evidence suggests that microbiome dysbiosis contributes to immune dysregulation in patients with inborn errors of immunity, potentially including DiGeorge syndrome[44]. In various inborn errors of immunity, distinct gastrointestinal, respiratory, and cutaneous symptoms linked to dysbiosis are observed, emphasizing the importance of microbiome identification and manipulation[44].

Probiotics, prebiotics, postbiotics, and fecal microbial transplantation represent promising strategies to restore the microbiota and potentially decrease disease pathology in patients with immunological disorders[44]. Research has demonstrated that certain dietary patterns such as high-fiber, plant-based diets promote growth of beneficial gut bacteria and reduce gut inflammation[44]. Probiotics promote connections between intestinal epithelial cells, mucosal immune cells, and the gut microbiota, increasing production of bioactive peptides and safeguarding intestinal epithelial barriers[44].

While microbiome manipulation strategies have not yet been specifically studied in DiGeorge syndrome populations, the emerging understanding of microbiota-immune system interactions suggests potential future applications for probiotic or prebiotic therapy to modulate immune dysregulation and potentially reduce infection susceptibility in this population.

## Summary and Synthesis of Pharmacological Therapeutic Landscape

The pharmacological management of DiGeorge syndrome has undergone substantial evolution, progressing from purely supportive care to regenerative medicine approaches addressing fundamental pathophysiological defects. The FDA approval of RETHYMIC in 2021 represents a watershed moment, providing the first definitive therapy for the life-threatening immunological manifestation of complete athymia. This allogeneic processed thymus tissue therapy, derived from three decades of pioneering research, achieves survival rates of 73-75% at two years and 94% at median ten-year follow-up in patients who survive the first post-transplant year—a remarkable achievement compared to the uniformly fatal prognosis without treatment[9][26][39].

However, RETHYMIC availability remains limited by the finite supply of donor thymic tissue and the requirement for specialized surgical expertise in implantation. Alternative approaches including hematopoietic cell transplantation offer comparable or superior immune reconstitution in selected populations with HLA-matched sibling donors, though currently achieving lower overall survival rates. Emerging investigational strategies to enhance thymic implantation outcomes through interleukin-7 supplementation may further improve future results.

For the substantial proportion of DiGeorge syndrome patients with partial rather than complete thymic deficiency, preventive pharmacological strategies remain essential. Prophylactic antimicrobial therapy with trimethoprim-sulfamethoxazole, pentamidine, valacyclovir, and fluconazole effectively prevents opportunistic infections that would otherwise cause substantial morbidity. Immunoglobulin replacement therapy addresses the antibody deficiency seen in approximately 2-3% of patients, reducing infection frequency and improving quality of life.

The significant psychiatric burden affecting 25-30% of DiGeorge syndrome patients with schizophrenia spectrum disorders has historically represented an intractable therapeutic challenge, with antipsychotics demonstrating limited efficacy and substantial seizure risk in this population. The discovery that bezafibrate—a repurposed cholesterol drug that activates mitochondrial biogenesis—can restore blood-brain barrier function and correct social memory deficits in preclinical models opens promising new therapeutic avenues currently requiring clinical validation.

Emerging biological immunosuppressive agents, particularly TNF-α inhibitors and mTOR inhibitors, demonstrate efficacy in managing autoimmune complications including juvenile idiopathic arthritis, immune cytopenias, and ALPS-like manifestations that complicate a substantial minority of DiGeorge syndrome patients. These agents permit restoration of immune balance when conventional corticosteroid therapy proves inadequate.

Careful attention to contraindications and drug-drug interactions remains essential in managing this complex population. Live attenuated vaccines require precise T-cell counting prior to administration, with safety considerations depending on CD3 count thresholds. Antipsychotics demand concurrent neurological assessment and anticonvulsant consideration given the elevated seizure risk. Management of hypoparathyroidism requires nuanced attention to calcium and vitamin D supplementation, particularly during pregnancy when maternal physiology substantially alters pharmacokinetics.

The development of a comprehensive, multidisciplinary approach integrating regenerative medicine, infection prevention, immunological modulation, endocrine management, and psychiatric care offers realistic hope for substantially improving outcomes in this previously devastating genetic disorder. Continued research into complement system modulation, microbiota manipulation, and enhanced thymic implantation strategies promises further advances in coming years.

## Conclusion

DiGeorge syndrome represents a complex multisystem genetic disorder requiring coordinated pharmacological intervention across multiple organ systems and pathophysiological domains. The 2021 FDA approval of RETHYMIC has transformed the prognosis for patients with complete athymia from uniformly fatal to potentially survivable, though with substantial ongoing morbidity from autoimmune complications requiring lifelong management. For the larger population of patients with partial disease, current pharmacological strategies focus on infection prevention, immunological support, endocrine stabilization, and emerging approaches to psychiatric disease management.

The repurposing of bezafibrate to address mitochondrial dysfunction-driven neuropsychiatric disease exemplifies the translational research pathway from mechanistic understanding of pathophysiology to potential clinical application. Ongoing clinical trials will determine whether this approach can substantially improve outcomes for the majority of DiGeorge patients with schizophrenia spectrum disorders.

Comprehensive multidisciplinary management, informed by understanding of drug mechanisms, appropriate indications, and critical contraindications, offers the best opportunity for optimizing quality of life and long-term outcomes in this genetically defined population. Future advances in thymic enhancement strategies, complement modulation, and microbiota manipulation promise further improvements in the therapeutic armamentarium available to clinicians caring for patients with this complex genetic disorder.