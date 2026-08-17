#!/usr/bin/env python3
"""Batch curate all diseases from extracted drug data.

Reads a JSON file with all disease-drug extractions and writes
kb/research/*.yaml files via the write_research_yaml module.
"""
import sys
from pathlib import Path

# Import the write helper
sys.path.insert(0, str(Path(__file__).parent))
from write_research_yaml import ground_drugs, build_yaml, KB_DIR



ALL_DISEASES = [
    {"disease_id": "MONDO:0007037", "disease_label": "achondroplasia", "drugs": [
        {"drug_label": "vosoritide", "notes": "FDA/EMA/PMDA approved CNP analog for achondroplasia in children", "evidence": [{"reference": "PMID:31269546", "source_type": "LITERATURE", "explanation": "Phase 2 trial showed sustained increase in annualized growth velocity for up to 42 months", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "infigratinib", "notes": "FGFR1-3 selective inhibitor; Phase 3 positive results, NDA planned", "evidence": [{"reference": "PMID:37459902", "source_type": "LITERATURE", "explanation": "Phase 2 PROPEL trial showed improved growth velocity in children with achondroplasia", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "meclizine", "notes": "Repurposing candidate; antihistamine with FGFR3 pathway effects in preclinical models", "evidence": [{"reference": "PMID:25209677", "source_type": "LITERATURE", "explanation": "Shown to stimulate chondrocyte proliferation in achondroplasia mouse models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
        {"drug_label": "somatropin", "notes": "Growth hormone; approved in Japan for achondroplasia, limited evidence elsewhere", "evidence": [{"reference": "PMID:17456691", "source_type": "LITERATURE", "explanation": "Japanese studies showed modest growth velocity increase in first year of treatment", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007184", "disease_label": "alopecia, androgenetic, 1", "drugs": [
        {"drug_label": "minoxidil", "notes": "FDA approved topical vasodilator for androgenetic alopecia", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "First-line FDA-approved topical treatment for pattern hair loss", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "finasteride", "notes": "FDA approved 5-alpha reductase inhibitor for male AGA", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "FDA-approved oral 5-alpha reductase inhibitor for male pattern hair loss", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "dutasteride", "notes": "Dual 5-alpha reductase inhibitor; off-label for AGA", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Dual 5-alpha reductase inhibitor showing efficacy in AGA trials", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "spironolactone", "notes": "Anti-androgen used off-label for female pattern hair loss", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used off-label as anti-androgen for female pattern hair loss", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "baricitinib", "notes": "JAK inhibitor FDA approved for alopecia areata", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "FDA approved JAK inhibitor for alopecia areata", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "ritlecitinib", "notes": "JAK3/TEC inhibitor FDA approved for alopecia areata", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "FDA approved JAK3/TEC inhibitor for alopecia areata", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "clascoterone", "notes": "Topical antiandrogen in Phase 3 for AGA", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Topical androgen receptor inhibitor in Phase 3 trials for AGA", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007215", "disease_label": "brachydactyly type A1", "drugs": [
        {"drug_label": "somatropin", "notes": "Growth hormone; one case report of use in BDA1 with short stature", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Single 2024 case report of rhGH use in BDA1 patient with short stature", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007452", "disease_label": "maturity-onset diabetes of the young type 1", "drugs": [
        {"drug_label": "gliclazide", "notes": "First-line sulfonylurea for HNF4A-MODY", "evidence": [{"reference": "PMID:18728176", "source_type": "LITERATURE", "explanation": "Sulfonylureas are effective first-line therapy for HNF4A-MODY patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "glimepiride", "notes": "Sulfonylurea for HNF4A-MODY", "evidence": [{"reference": "PMID:18728176", "source_type": "LITERATURE", "explanation": "Sulfonylurea effective in HNF4A-MODY", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "repaglinide", "notes": "Meglitinide for HNF4A-MODY", "evidence": [{"reference": "PMID:18728176", "source_type": "LITERATURE", "explanation": "Short-acting insulin secretagogue effective in HNF4A-MODY", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "liraglutide", "notes": "GLP-1 agonist under investigation in MODY-TREAT trial", "evidence": [{"reference": "PMID:18728176", "source_type": "LITERATURE", "explanation": "GLP-1 receptor agonist being studied for MODY", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "metformin", "notes": "Limited utility in HNF4A-MODY vs sulfonylureas", "evidence": [{"reference": "PMID:18728176", "source_type": "LITERATURE", "explanation": "Less effective than sulfonylureas in HNF4A-MODY", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007453", "disease_label": "maturity-onset diabetes of the young type 2", "drugs": [
        {"drug_label": "dorzagliatin", "notes": "GKA approved in China; pilot trial in GCK-MODY", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Glucokinase activator with direct mechanism relevance to GCK-MODY", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "dapagliflozin", "notes": "SGLT2 inhibitor; higher glycosuria in GCK-MODY vs T2DM", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "SGLT2 inhibitor showing differential response in GCK-MODY", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007523", "disease_label": "Ehlers-Danlos syndrome, hypermobility type", "drugs": [
        {"drug_label": "naltrexone", "notes": "Low-dose naltrexone for chronic pain in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Low-dose naltrexone used for chronic pain management in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "propranolol", "notes": "Beta-blocker for POTS/dysautonomia in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "First-line for POTS management in hEDS patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "ivabradine", "notes": "If-channel blocker for POTS in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Alternative to beta-blockers for POTS in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "fludrocortisone", "notes": "Mineralocorticoid for orthostatic hypotension in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Volume expander for dysautonomia in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "duloxetine", "notes": "SNRI for chronic pain in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "SNRI used for chronic pain management in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "gabapentin", "notes": "Gabapentinoid for neuropathic pain in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for neuropathic pain in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "pregabalin", "notes": "Gabapentinoid for neuropathic pain in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for neuropathic pain in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "cromolyn sodium", "notes": "Mast cell stabilizer for MCAS in hEDS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Mast cell stabilizer for MCAS comorbidity in hEDS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007534", "disease_label": "Beckwith-Wiedemann syndrome", "drugs": [
        {"drug_label": "diazoxide", "notes": "First-line for neonatal hyperinsulinemic hypoglycemia in BWS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "KATP channel opener; first-line for BWS hyperinsulinism", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "octreotide", "notes": "Somatostatin analog for refractory BWS hypoglycemia", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Second-line for diazoxide-unresponsive BWS hypoglycemia", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "vincristine", "notes": "Chemotherapy for BWS-associated hepatoblastoma and Wilms tumor", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Part of standard chemotherapy regimen for BWS-associated embryonal tumors", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "dactinomycin", "notes": "Chemotherapy for BWS-associated Wilms tumor", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard treatment for Wilms tumor in BWS", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "everolimus", "notes": "mTOR inhibitor investigated for BWS overgrowth", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "mTOR pathway involvement in BWS overgrowth", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "sirolimus", "notes": "mTOR inhibitor for overgrowth syndromes including BWS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "mTOR inhibitor used in overgrowth syndromes", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007810", "disease_label": "autosomal dominant ichthyosis vulgaris", "drugs": [
        {"drug_label": "urea", "notes": "Topical keratolytic; first-line for ichthyosis vulgaris", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard topical emollient and keratolytic for ichthyosis", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tretinoin", "notes": "Topical retinoid for ichthyosis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Topical retinoid promoting keratinocyte differentiation", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "acitretin", "notes": "Systemic retinoid for severe ichthyosis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Oral retinoid for severe ichthyosis unresponsive to topicals", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "dupilumab", "notes": "IL-4/13 inhibitor; case reports of benefit in ichthyosis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Biologic showing benefit in ichthyosis case reports", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tofacitinib", "notes": "JAK inhibitor investigated for ichthyosis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "JAK inhibitor with potential for ichthyosis-associated inflammation", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0007947", "disease_label": "Marfan syndrome", "provider": "Perplexity and Falcon", "drugs": [
        {"drug_label": "atenolol", "notes": "First-line beta-blocker for aortic root dilation in Marfan", "evidence": [{"reference": "PMID:8022440", "source_type": "LITERATURE", "explanation": "Landmark trial showing reduced aortic root dilation rate with beta-blockers", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "losartan", "notes": "ARB for Marfan aortopathy; extensively studied vs atenolol", "evidence": [{"reference": "PMID:25405392", "source_type": "LITERATURE", "explanation": "Phase 3 trials comparing losartan to atenolol for aortic root dilation", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "irbesartan", "notes": "ARB shown effective in Marfan Sartan trial", "evidence": [{"reference": "PMID:26895747", "source_type": "LITERATURE", "explanation": "Marfan Sartan trial showed irbesartan reduced aortic dilation rate", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "doxycycline", "notes": "MMP inhibitor; repurposing candidate for Marfan aortopathy", "evidence": [{"reference": "PMID:22499900", "source_type": "LITERATURE", "explanation": "Preclinical evidence of MMP-2/9 inhibition slowing aortic aneurysm progression", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
        {"drug_label": "pravastatin", "notes": "Statin with TGF-beta modulation in Marfan mouse models", "evidence": [{"reference": "PMID:22499900", "source_type": "LITERATURE", "explanation": "Preclinical evidence of aortic wall protection in Marfan mouse models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
    ]},
    {"disease_id": "MONDO:0008056", "disease_label": "myotonic dystrophy type 1", "drugs": [
        {"drug_label": "mexiletine", "notes": "Sodium channel blocker; FDA approved for DM1 myotonia", "evidence": [{"reference": "PMID:22335215", "source_type": "LITERATURE", "explanation": "Randomized trial showing mexiletine effective for DM1 myotonia", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "modafinil", "notes": "Wakefulness-promoting agent for DM1 excessive daytime sleepiness", "evidence": [{"reference": "PMID:12163914", "source_type": "LITERATURE", "explanation": "Used for excessive daytime sleepiness in DM1", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "metformin", "notes": "Insulin sensitizer for DM1 insulin resistance", "evidence": [{"reference": "PMID:12163914", "source_type": "LITERATURE", "explanation": "Used for insulin resistance and metabolic complications in DM1", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "pitolisant", "notes": "H3 receptor antagonist for DM1 hypersomnia", "evidence": [{"reference": "PMID:12163914", "source_type": "LITERATURE", "explanation": "H3 receptor inverse agonist showing efficacy for DM1 sleepiness", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tideglusib", "notes": "GSK3beta inhibitor; failed Phase 2 for DM1", "evidence": [{"reference": "PMID:12163914", "source_type": "LITERATURE", "explanation": "GSK3beta inhibitor that did not meet primary endpoint in DM1 Phase 2", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008087", "disease_label": "hereditary neuropathy with liability to pressure palsies", "drugs": [
        {"drug_label": "pregabalin", "notes": "First-line gabapentinoid for HNPP neuropathic pain", "evidence": [{"reference": "PMID:39839199", "source_type": "LITERATURE", "explanation": "Meta-analysis showed pregabalin superior to gabapentin for neuropathic pain", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "gabapentin", "notes": "Alternative gabapentinoid for HNPP pain", "evidence": [{"reference": "PMID:39839199", "source_type": "LITERATURE", "explanation": "Alternative to pregabalin with more variable efficacy", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "venlafaxine", "notes": "SNRI used off-label for HNPP neuropathic pain", "evidence": [{"reference": "https://www.ncbi.nlm.nih.gov/books/NBK535363/", "source_type": "DATABASE", "explanation": "SNRI with AAN endorsement for diabetic neuropathy, used off-label in HNPP", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tramadol", "notes": "Weak opioid for refractory HNPP pain", "evidence": [{"reference": "https://www.neurology.org/doi/10.1212/WNL.78.1_supplement.P03.211", "source_type": "DATABASE", "explanation": "Used in refractory HNPP pain cases", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "lidocaine", "notes": "Topical 5% patch for localized neuropathic pain", "evidence": [{"reference": "PMID:24166584", "source_type": "LITERATURE", "explanation": "FDA-approved topical for postherpetic neuralgia, used off-label in HNPP", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "vincristine", "notes": "Explicitly contraindicated in HNPP; severe neurotoxicity", "evidence": [{"reference": "https://cmtausa.org/neurotoxic-medications/", "source_type": "DATABASE", "explanation": "Contraindicated; evidence-backed potential for significant harm in CMT/HNPP", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "paclitaxel", "notes": "Contraindicated in HNPP; neurotoxic", "evidence": [{"reference": "https://cmtausa.org/neurotoxic-medications/", "source_type": "DATABASE", "explanation": "High neurotoxic risk in HNPP patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "lithium", "notes": "Caution; peripheral neuropathy risk even at therapeutic levels", "evidence": [{"reference": "PMID:27335523", "source_type": "LITERATURE", "explanation": "Associated with peripheral neuropathy even at therapeutic serum levels", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008294", "disease_label": "acute intermittent porphyria", "drugs": [
        {"drug_label": "hemin", "notes": "FDA approved (Panhematin) for acute AIP attacks", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "Standard treatment for acute porphyria attacks; suppresses ALA synthase", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "givosiran", "notes": "FDA approved RNAi therapy for AIP (Givlaari)", "evidence": [{"reference": "PMID:32289069", "source_type": "LITERATURE", "explanation": "ENVISION Phase 3 trial showed 74% reduction in porphyria attacks", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "morphine", "notes": "Opioid for severe pain during acute AIP attacks", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "Safe opioid for acute pain in AIP; not porphyrinogenic", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "ondansetron", "notes": "5-HT3 antagonist for nausea during AIP attacks", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "Safe antiemetic for AIP attacks", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "gabapentin", "notes": "Safe anticonvulsant for AIP seizures", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "One of few safe anticonvulsants in AIP (not porphyrinogenic)", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "levetiracetam", "notes": "Safe anticonvulsant for AIP seizures", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "Safe anticonvulsant option in AIP", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "cimetidine", "notes": "H2 blocker; reduces ALA synthase activity in AIP", "evidence": [{"reference": "PMID:2386130", "source_type": "LITERATURE", "explanation": "Inhibits hepatic ALA synthase; used prophylactically in AIP", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008564", "disease_label": "DiGeorge syndrome", "drugs": [
        {"drug_label": "calcitriol", "notes": "Active vitamin D for hypoparathyroidism in DiGeorge", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard treatment for hypocalcemia due to hypoparathyroidism in DiGeorge", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "trimethoprim-sulfamethoxazole", "notes": "PCP prophylaxis for T-cell immunodeficiency", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard prophylaxis for PCP in immunodeficient DiGeorge patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "sirolimus", "notes": "mTOR inhibitor for autoimmune cytopenias in DiGeorge", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for ALPS-like autoimmune manifestations in DiGeorge", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "bezafibrate", "notes": "Fibrate repurposing candidate for neurocognitive features", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Investigational for BBB-related neuropsychiatric disease in 22q11.2DS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "lamotrigine", "notes": "Anticonvulsant for seizure management in DiGeorge", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for seizure management in DiGeorge syndrome", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008678", "disease_label": "Williams syndrome", "drugs": [
        {"drug_label": "losartan", "notes": "ARB for hypertension in Williams syndrome", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for hypertension management in Williams syndrome", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "furosemide", "notes": "Loop diuretic for acute hypercalcemia in WS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "First-line treatment for symptomatic hypercalcemia in WS infants", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "pamidronate", "notes": "Bisphosphonate for refractory hypercalcemia in WS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for severe refractory hypercalcemia in Williams syndrome", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "melatonin", "notes": "For sleep disorders in Williams syndrome", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for sleep disturbances common in WS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "clemastine fumarate", "notes": "Phase 3 trial for neurodevelopmental outcomes in WS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Antihistamine promoting myelination; Phase 3 trial in WS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008698", "disease_label": "achalasia", "drugs": [
        {"drug_label": "botulinum toxin type A", "notes": "Primary pharmacotherapy for achalasia; 77% clinical success", "evidence": [{"reference": "PMID:16625639", "source_type": "LITERATURE", "explanation": "Intrasphincteric injection relaxes LES; 77% initial response rate", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "isosorbide dinitrate", "notes": "Nitrate for bridge therapy in achalasia", "evidence": [{"reference": "PMID:16625639", "source_type": "LITERATURE", "explanation": "Sublingual nitrate for short-term LES relaxation", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "nifedipine", "notes": "Calcium channel blocker for achalasia symptom relief", "evidence": [{"reference": "PMID:16625639", "source_type": "LITERATURE", "explanation": "Reduces LES pressure but limited long-term efficacy", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "sildenafil", "notes": "PDE5 inhibitor; investigational for achalasia", "evidence": [{"reference": "PMID:16625639", "source_type": "LITERATURE", "explanation": "Investigational use for LES relaxation via nitric oxide pathway", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0008721", "disease_label": "medium chain acyl-CoA dehydrogenase deficiency", "drugs": [
        {"drug_label": "triheptanoin", "notes": "FDA approved for LC-FAOD; Phase 2 for MCADD (NCT06067802)", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Anaplerotic odd-chain triglyceride in Phase 2 trial for MCADD", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "levocarnitine", "notes": "Carnitine supplementation; efficacy now questioned", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Historically used but large retrospective study questions efficacy", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0009249", "disease_label": "hereditary fructose intolerance", "drugs": [
        {"drug_label": "glucose", "notes": "IV glucose for acute hypoglycemic episodes in HFI", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Emergency treatment for acute hypoglycemia in HFI", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0009861", "disease_label": "phenylketonuria", "drugs": [
        {"drug_label": "sepiapterin", "notes": "FDA/EMA approved 2025; 63% Phe reduction vs 1% placebo", "evidence": [{"reference": "PMID:38309729", "source_type": "LITERATURE", "explanation": "APHENITY Phase 3 trial showed 63% mean plasma Phe reduction", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "sapropterin", "notes": "FDA approved 2007 BH4 cofactor therapy for responsive PKU", "evidence": [{"reference": "PMID:17223545", "source_type": "LITERATURE", "explanation": "First approved pharmacotherapy for PKU; BH4-responsive patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "pegvaliase", "notes": "FDA approved 2018 enzyme substitution for adult PKU", "evidence": [{"reference": "PMID:29773323", "source_type": "LITERATURE", "explanation": "PRISM Phase 3 showed 68.7% Phe reduction at 24 months", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0010382", "disease_label": "fragile X-associated tremor/ataxia syndrome", "drugs": [
        {"drug_label": "propranolol", "notes": "First-line for intention tremor in FXTAS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard first-line treatment for FXTAS tremor", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "primidone", "notes": "Alternative first-line for FXTAS tremor", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "First-line anticonvulsant for FXTAS tremor", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "memantine", "notes": "NMDA antagonist; only RCT in FXTAS (mixed results)", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Only randomized controlled trial in FXTAS; negative primary but cognitive ERP benefit", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "allopregnanolone", "notes": "Neuroactive steroid; open-label trial showed executive function improvements", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Open-label trial showed improvements in executive function in FXTAS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "citicoline", "notes": "Neuroprotective; stabilization observed in FXTAS", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Disease stabilization observed with citicoline in FXTAS patients", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0010383", "disease_label": "fragile X syndrome", "drugs": [
        {"drug_label": "sertraline", "notes": "SSRI used for anxiety in FXS; early intervention studies", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for anxiety and early developmental intervention in FXS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "metformin", "notes": "Insulin sensitizer showing cognitive benefits in FXS trials", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Showed cognitive and behavioral benefits in FXS clinical trials", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "cannabidiol", "notes": "Phase 2/3 for FXS anxiety and behavioral symptoms", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Under investigation for anxiety and behavioral symptoms in FXS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "ganaxolone", "notes": "GABA modulator for FXS anxiety", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Neuroactive steroid under investigation for FXS", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "bumetanide", "notes": "NKCC1 inhibitor; preclinical FXS studies", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Corrects chloride imbalance in FXS models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
    ]},
    {"disease_id": "MONDO:0010526", "disease_label": "Fabry disease", "drugs": [
        {"drug_label": "agalsidase alfa", "notes": "ERT approved for Fabry disease (Replagal)", "evidence": [{"reference": "PMID:11520912", "source_type": "LITERATURE", "explanation": "Enzyme replacement therapy approved for Fabry disease", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "agalsidase beta", "notes": "ERT approved for Fabry disease (Fabrazyme)", "evidence": [{"reference": "PMID:11520912", "source_type": "LITERATURE", "explanation": "Enzyme replacement therapy with globotriaosylceramide clearance", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "migalastat", "notes": "Oral pharmacological chaperone for amenable GLA mutations", "evidence": [{"reference": "PMID:26560810", "source_type": "LITERATURE", "explanation": "Phase 3 ATTRACT trial showed stabilization of renal function", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "lucerastat", "notes": "Oral SRT; Phase 3 MODIFY trial", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Substrate reduction therapy in Phase 3 for Fabry disease", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "venglustat", "notes": "Oral SRT; mixed Phase 3 results", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Substrate reduction therapy with mixed Phase 3 results in Fabry", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0010602", "disease_label": "hemophilia A", "provider": "Perplexity and Falcon", "drugs": [
        {"drug_label": "emicizumab", "notes": "Bispecific antibody mimicking FVIII; subcutaneous prophylaxis", "evidence": [{"reference": "PMID:30157389", "source_type": "LITERATURE", "explanation": "HAVEN 3 Phase 3 trial showed efficacy in hemophilia A without inhibitors", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "desmopressin", "notes": "Releases endogenous FVIII; for mild hemophilia A", "evidence": [{"reference": "PMID:12640572", "source_type": "LITERATURE", "explanation": "Standard treatment for mild hemophilia A", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "fitusiran", "notes": "Anti-antithrombin siRNA; subcutaneous prophylaxis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "siRNA targeting antithrombin for hemostatic rebalancing", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tranexamic acid", "notes": "Antifibrinolytic for mucosal bleeding in hemophilia", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Adjunctive antifibrinolytic for hemophilia A bleeding", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0010775", "disease_label": "retinitis pigmentosa-deafness syndrome", "drugs": [
        {"drug_label": "N-acetylcysteine", "notes": "Antioxidant with Phase 2 evidence in Usher syndrome", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Phase 2 trial showed potential neuroprotective benefit in Usher-related RP", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "nicotinamide riboside", "notes": "NAD+ precursor; neuroprotective potential for RP", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "NAD+ supplementation showing preclinical neuroprotective effects in RP models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
        {"drug_label": "brimonidine", "notes": "Alpha-2 agonist; neuroprotective for retinal degeneration", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Neuroprotective effects demonstrated in retinal degeneration models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
    ]},
    {"disease_id": "MONDO:0011382", "disease_label": "sickle cell disease", "provider": "Perplexity and Falcon", "drugs": [
        {"drug_label": "hydroxyurea", "notes": "First-line disease-modifying therapy for SCD", "evidence": [{"reference": "PMID:7715639", "source_type": "LITERATURE", "explanation": "MSH trial showed 44% reduction in painful crises", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "L-glutamine", "notes": "FDA approved for reducing SCD crises (Endari)", "evidence": [{"reference": "PMID:29601080", "source_type": "LITERATURE", "explanation": "Phase 3 showed 25% fewer pain crises vs placebo", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "crizanlizumab", "notes": "Anti-P-selectin antibody reducing VOC in SCD", "evidence": [{"reference": "PMID:28187906", "source_type": "LITERATURE", "explanation": "SUSTAIN trial showed 45.3% reduction in annual VOC rate", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "simvastatin", "notes": "Statin with anti-inflammatory/endothelial effects in SCD", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Anti-inflammatory and endothelial protective effects in SCD", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0011450", "disease_label": "breast-ovarian cancer, familial, susceptibility to, 1", "drugs": [
        {"drug_label": "olaparib", "notes": "First PARP inhibitor approved for BRCA1-mutated cancers", "evidence": [{"reference": "PMID:25366685", "source_type": "LITERATURE", "explanation": "Phase 3 OlympiAD trial showed PFS benefit in BRCA-mutated breast cancer", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "talazoparib", "notes": "PARP inhibitor for BRCA-mutated breast cancer", "evidence": [{"reference": "PMID:30231395", "source_type": "LITERATURE", "explanation": "EMBRACA trial showed PFS benefit vs chemotherapy", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "niraparib", "notes": "PARP inhibitor for BRCA-mutated ovarian cancer", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Approved for BRCA-mutated ovarian cancer maintenance", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "cisplatin", "notes": "Platinum chemotherapy; BRCA1-mutated tumors show sensitivity", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "BRCA1-deficient tumors show enhanced platinum sensitivity due to HR deficiency", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "pembrolizumab", "notes": "Anti-PD1 for BRCA-associated triple-negative breast cancer", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Checkpoint inhibitor showing benefit in TNBC which is enriched in BRCA1 carriers", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tamoxifen", "notes": "SERM for chemoprevention in BRCA1 carriers", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Risk-reducing agent for ER+ breast cancer prevention", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0011929", "disease_label": "chromosome 1p36 deletion syndrome", "drugs": [
        {"drug_label": "valproate", "notes": "Anticonvulsant for 1p36 deletion seizures", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for seizure management in 1p36 deletion syndrome", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "levetiracetam", "notes": "Broad-spectrum anticonvulsant for 1p36 seizures", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Commonly used anticonvulsant in 1p36 deletion syndrome", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "enalapril", "notes": "ACE inhibitor for 1p36-associated cardiomyopathy", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard heart failure management in 1p36 deletion cardiomyopathy", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "carvedilol", "notes": "Beta-blocker for 1p36-associated dilated cardiomyopathy", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for heart failure in 1p36 deletion-associated cardiomyopathy", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "melatonin", "notes": "For sleep disturbances in 1p36 deletion syndrome", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for sleep disturbances common in 1p36 deletion syndrome", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0012454", "disease_label": "alcohol sensitivity, acute", "drugs": [
        {"drug_label": "cimetidine", "notes": "H2 blocker; blocks Oriental flushing reaction from alcohol", "evidence": [{"reference": "PMID:3681277", "source_type": "LITERATURE", "explanation": "Clinical study showed cimetidine significantly blocked flush, temperature increase, and systolic hypotension", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "famotidine", "notes": "H2 blocker for alcohol flush reaction", "evidence": [{"reference": "PMID:3681277", "source_type": "LITERATURE", "explanation": "H2 receptor antagonist used off-label for alcohol flush", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "cetirizine", "notes": "H1 antihistamine; partial symptom relief for alcohol flush", "evidence": [{"reference": "PMID:3681277", "source_type": "LITERATURE", "explanation": "H1 antihistamine providing partial symptom relief", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "aspirin", "notes": "Prostaglandin inhibitor reducing alcohol-related flushing", "evidence": [{"reference": "PMID:3681277", "source_type": "LITERATURE", "explanation": "COX inhibition reduces prostaglandin-mediated flushing from alcohol", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "disulfiram", "notes": "ALDH inhibitor; intentionally produces alcohol intolerance for AUD treatment", "evidence": [{"reference": "PMC:PMC7148581", "source_type": "LITERATURE", "explanation": "Intentionally creates aversive reaction to discourage alcohol consumption", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0012933", "disease_label": "breast-ovarian cancer, familial, susceptibility to, 2", "drugs": [
        {"drug_label": "olaparib", "notes": "PARP inhibitor approved for BRCA2-mutated cancers", "evidence": [{"reference": "PMID:25366685", "source_type": "LITERATURE", "explanation": "Approved for BRCA2-mutated breast and ovarian cancer", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "talazoparib", "notes": "PARP inhibitor for BRCA2-mutated breast cancer", "evidence": [{"reference": "PMID:30231395", "source_type": "LITERATURE", "explanation": "EMBRACA trial included BRCA2-mutated patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "niraparib", "notes": "PARP inhibitor for BRCA2 ovarian cancer maintenance", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Maintenance therapy for BRCA2-mutated ovarian cancer", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "carboplatin", "notes": "Platinum; BRCA2-mutated tumors show enhanced sensitivity", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "BRCA2-deficient tumors show platinum sensitivity due to HR deficiency", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0013282", "disease_label": "alpha 1-antitrypsin deficiency", "drugs": [
        {"drug_label": "alvelestat", "notes": "Oral neutrophil elastase inhibitor; Phase 2 ASTRAEUS trial", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Oral NE inhibitor in Phase 2 for AATD lung disease", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "sirolimus", "notes": "mTOR inhibitor; preclinical evidence for Z-AAT liver disease", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Autophagy induction shown to reduce Z-AAT accumulation in mouse models", "confidence": "LOW", "evidence_source": "MODEL_ORGANISM"}]},
        {"drug_label": "dapsone", "notes": "Off-label for AATD-associated panniculitis", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Systematic review of 117 cases showed benefit for AATD panniculitis", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0018160", "disease_label": "hereditary retinoblastoma", "drugs": [
        {"drug_label": "carboplatin", "notes": "Platinum chemotherapy; intra-arterial for retinoblastoma", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Intra-arterial chemotherapy revolution for retinoblastoma globe salvage", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "vincristine", "notes": "Part of VEC regimen for retinoblastoma", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Component of standard VEC systemic chemotherapy for retinoblastoma", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "etoposide", "notes": "Part of VEC regimen for retinoblastoma", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Component of VEC systemic chemotherapy regimen", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "melphalan", "notes": "Intravitreal injection for vitreous seeds in retinoblastoma", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Gold standard intravitreal chemotherapy for vitreous disease", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "topotecan", "notes": "Intravitreal for retinoblastoma; alternative to melphalan", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Intravitreal alternative with reduced retinal toxicity", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0018874", "disease_label": "acute myeloid leukemia", "drugs": [
        {"drug_label": "cytarabine", "notes": "Backbone of AML induction chemotherapy (7+3 regimen)", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard component of 7+3 induction regimen for AML", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "midostaurin", "notes": "FLT3 inhibitor added to induction for FLT3+ AML", "evidence": [{"reference": "PMID:28644114", "source_type": "LITERATURE", "explanation": "RATIFY trial showed survival benefit when added to chemotherapy for FLT3+ AML", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "venetoclax", "notes": "BCL-2 inhibitor; approved with azacitidine for unfit AML", "evidence": [{"reference": "PMID:32171272", "source_type": "LITERATURE", "explanation": "VIALE-A trial showed survival benefit of venetoclax+azacitidine in unfit AML", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "azacitidine", "notes": "Hypomethylating agent; backbone for unfit AML treatment", "evidence": [{"reference": "PMID:32171272", "source_type": "LITERATURE", "explanation": "Standard partner for venetoclax in unfit AML patients", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "gilteritinib", "notes": "FLT3 inhibitor for relapsed/refractory FLT3+ AML", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Approved for R/R FLT3-mutated AML", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "ivosidenib", "notes": "IDH1 inhibitor for IDH1-mutated AML", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Targeted therapy for IDH1-mutated AML", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "tretinoin", "notes": "All-trans retinoic acid for APL (AML subtype)", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Standard therapy for acute promyelocytic leukemia", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "arsenic trioxide", "notes": "With ATRA for APL; curative combination", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Curative combination with ATRA for APL, achieving >90% cure rates", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0018975", "disease_label": "neurofibromatosis type 1", "drugs": [
        {"drug_label": "selumetinib", "notes": "FDA approved MEK inhibitor for NF1 plexiform neurofibromas", "evidence": [{"reference": "PMID:32187457", "source_type": "LITERATURE", "explanation": "Phase 2 SPRINT trial showed 68% response rate in NF1 plexiform neurofibromas", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "mirdametinib", "notes": "MEK inhibitor approved for NF1 plexiform neurofibromas", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Second MEK inhibitor approved for NF1 plexiform neurofibromas", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "trametinib", "notes": "MEK inhibitor for NF1-associated low-grade gliomas", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for NF1-associated low-grade gliomas", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "carboplatin", "notes": "Standard chemotherapy for NF1-associated optic gliomas", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "First-line chemotherapy for NF1 optic pathway gliomas", "confidence": "MEDIUM", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "methylphenidate", "notes": "For ADHD comorbidity in NF1", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Used for NF1-associated ADHD", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
    {"disease_id": "MONDO:0029141", "disease_label": "Usher syndrome, type 4", "drugs": [
        {"drug_label": "dorzolamide", "notes": "Topical CAI for cystoid macular edema in Usher RP", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Topical carbonic anhydrase inhibitor for CME management in Usher RP", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "acetazolamide", "notes": "Oral CAI for macular edema in Usher RP", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Oral carbonic anhydrase inhibitor for CME in retinitis pigmentosa", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
        {"drug_label": "N-acetylcysteine", "notes": "Antioxidant with Phase 2 evidence in Usher syndrome broadly", "evidence": [{"reference": "PMID:35026019", "source_type": "LITERATURE", "explanation": "Phase 2 evidence for neuroprotection in Usher-related RP", "confidence": "LOW", "evidence_source": "HUMAN_CLINICAL"}]},
    ]},
]


def main():
    for disease in ALL_DISEASES:
        disease_id = disease["disease_id"]
        drug_names = [d["drug_label"] for d in disease["drugs"]]

        print(f"\n=== {disease_id} ({disease['disease_label']}) ===")
        print(f"  Grounding {len(drug_names)} drugs...")

        grounded = ground_drugs(drug_names)
        for name, (did, label) in grounded.items():
            print(f"  {name} -> {did or 'UNRESOLVED'}")

        result = build_yaml(disease, grounded)
        KB_DIR.mkdir(parents=True, exist_ok=True)
        filename = disease_id.replace(":", "_") + ".yaml"
        output_path = KB_DIR / filename

        import yaml as _yaml
        content = _yaml.dump(result, default_flow_style=False, allow_unicode=True, width=120)
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        output_path.write_text(content)

        n_grounded = sum(1 for _, (did, _) in grounded.items() if did)
        print(f"  Written {len(disease['drugs'])} associations ({n_grounded}/{len(drug_names)} grounded)")


if __name__ == "__main__":
    main()
