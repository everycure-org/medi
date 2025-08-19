
from kedro.pipeline import node, Pipeline, pipeline
from . import nodes, extract_ob, get_marketing, get_earliest_approval_date_ob
import os
from medi.utils import nameres, normalize
from medi.utils import openai_tags
from medi.utils import preprocess_lists, get_atc, get_smiles
from . import convert_dates_pb

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

##########################################################################################################
########### OB ###########################################################################################
##########################################################################################################
        node(
            func = nodes.standardize_dataframe,
            inputs = [
                "orange-book-products",
                "params:standardization_mapping_ob",

            ],
            outputs = "ob-standardized",
            name = "standardize-cols-ob"
        ),
        node(
            func=get_marketing.add_most_permissive_marketing_tags_fda,
            inputs = "ob-standardized",
            outputs = "ob-with-marketing-tags",
            name = "get-marketing-tags-ob"
        ),
        node(
            func=get_earliest_approval_date_ob.acquire_earliest_approval_dates,
            inputs = "ob-with-marketing-tags",
            outputs = "ob-reformatted-dates",
            name = "reformat-dates-ob"
        ),

        node(
            func=nodes.deduplicate_dataframe,
            inputs = [
                "ob-reformatted-dates",
                "params:deduplication_columns_usa",
            ],
            outputs = "ob-deduplicated",
            name = "deduplicate-ob"
        ),
        node(
            func = nameres.nameres_column,
            inputs = [
                "ob-deduplicated",
                "params:standardization_mapping_ob.Ingredient",
                "params:name_resolver_params"
            ],
            outputs = "ob-nameresolved",
            name = "nameres-ob"
        ),
        node(
            func = nodes.qc_id_llm,
            inputs = [
                "ob-nameresolved",
                "params:id_correct_incorrect_tag",
            ],
            outputs = "ob-llm-id-qc",
            name="qc-id-llm-ob"
        ),
        node(
            func=nodes.improve_ids,
            inputs = [
                "ob-llm-id-qc",
                "params:name_resolver_params_llm_improve",
                "params:llm_best_id_tag_drug_prompt"
            ],
            outputs =[
                "ob-corrected-ids",
                "ob-nameres-errors"
            ],
            name = "select-best-ids-ob"
        ),
        node(
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "ob-corrected-ids",
                "params:approval_tags.usa",
                "params:true_bool",
            ],
            outputs="ob-usa-approved-tags",
            name = "add-approval-tags-ob"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "ob-usa-approved-tags",
                "params:combo_therapy_tags",
                "params:standardization_mapping_ob.Ingredient"
            ],
            outputs = "ob-combo-therapy-tags",
            name = 'tag-combo-therapies-ob'
        ),
        node(
            func=nodes.split_combination_therapies,
            inputs=[
                "ob-combo-therapy-tags",
                "params:combination_therapy_split_drug"
            ],
            outputs = "ob-split-ingredients",
            name = "split-ingredients-ob"
        ),
        node(
            func=nameres.nameres_column_combination_therapy_ingredients,
            inputs=[
                "ob-split-ingredients",
                "params:combination_therapy_split_drug.output_col",
                "params:name_resolver_params",
            ],
            outputs = "ob-component-ids",
            name = "identify-ingredients-ob"
        ),
        node(
            func=nodes.add_unlisted_ingredients,
            inputs = "ob-component-ids",
            outputs = "ob-unlisted-single-ingredients",
            name = "add-unlisted-ingredients-ob"
        ),
        node(
            func=normalize.normalize_column,
            inputs = [
                "ob-unlisted-single-ingredients",
                "params:best_id_column"
            ],
            outputs = "ob-norm",
            name = "normalize-ob"
        ),


##########################################################################################################
########### PB ###########################################################################################
##########################################################################################################
        node(
            func = nodes.standardize_dataframe,
            inputs = [
                "purple-book-products",
                "params:standardization_mapping_purple_book",

            ],
            outputs = "pb-standardized",
            name = "standardize-cols-pb"
        ),
        node(
            func=get_marketing.add_most_permissive_marketing_tags_fda,
            inputs = "pb-standardized",
            outputs = "pb-marketing-tags",
            name = "get-marketing-tags-pb"
        ),
        node(
            func=get_earliest_approval_date_ob.convert_date_format,
            inputs = "pb-marketing-tags",
            outputs = "pb-reformatted-dates",
            name = "reformat-dates-pb"
        ),
        node(
            func=get_earliest_approval_date_ob.transform_dates_to_earliest,
            inputs = "pb-reformatted-dates",
            outputs = "pb-earliest-dates",
            name = "get-earliest-dates-pb"
        ),
        node(
            func=nodes.deduplicate_dataframe,
            inputs = [
                "pb-earliest-dates",
                "params:deduplication_columns_usa",
            ],
            outputs = "pb-deduplicated",
            name = "deduplicate-pb"
        ),
        node(
            func = nameres.nameres_column,
            inputs = [
                "pb-deduplicated",
                "params:standardization_mapping_ob.Ingredient",
                "params:name_resolver_params"
            ],
            outputs = "pb-nameresolved",
            name = "nameres-pb"
        ),
        node(
            func = nodes.qc_id_llm,
            inputs = [
                "pb-nameresolved",
                "params:id_correct_incorrect_tag",
            ],
            outputs = "pb-llm-id-qc",
            name="qc-id-llm-pb"
        ),
        node(
            func=nodes.improve_ids,
            inputs = [
                "pb-llm-id-qc",
                "params:name_resolver_params_llm_improve",
                "params:llm_best_id_tag_drug_prompt"
            ],
            outputs = [
                "pb-corrected-ids",
                "pb-nameres-errors"
            ], 
            name = "select-best-ids-pb"
        ),
        node(
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "pb-corrected-ids",
                "params:approval_tags.usa",
                "params:true_bool",
            ],
            outputs="pb-usa-approved-tags",
            name = "add-approval-tags-pb"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "pb-usa-approved-tags",
                "params:combo_therapy_tags",
                "params:source_ingredients_column"
            ],
            outputs = "pb-combo-therapy-tags",
            name = 'tag-combo-therapies-pb'
        ),
        node(
            func=nodes.split_combination_therapies,
            inputs=[
                "pb-combo-therapy-tags",
                "params:combination_therapy_split_drug"
            ],
            outputs = "pb-split-ingredients",
            name = "split-ingredients-pb"
        ),
        node(
            func=nameres.nameres_column_combination_therapy_ingredients,
            inputs=[
                "pb-split-ingredients",
                "params:combination_therapy_split_drug.output_col",
                "params:name_resolver_params",
            ],
            outputs = "pb-component-ids",
            name = "identify-ingredients-pb"
        ),
        node(
            func=nodes.add_unlisted_ingredients,
            inputs = "pb-component-ids",
            outputs = "pb-unlisted-single-ingredients",
            name = "add-unlisted-ingredients-pb"
        ),
        node(
            func=normalize.normalize_column,
            inputs = [
                "pb-unlisted-single-ingredients",
                "params:best_id_column"
            ],
            outputs = "pb-norm",
            name = "normalize-pb"
        ),


##########################################################################################################
########### EMA ##########################################################################################
##########################################################################################################
        node(
            func=preprocess_lists.preprocess_ema,
            inputs = "ema-raw",
            outputs = "ema-preprocessed",
            name = "preprocess-ema"
        ),
        node(
            func = nodes.standardize_dataframe,
            inputs = [
                "ema-preprocessed",
                "params:standardization_mapping_ema",

            ],
            outputs = "ema-standardized",
            name = "standardize-cols-ema"
        ),
        node(
            func=preprocess_lists.reformat_dates_ema,
            inputs = "ema-standardized",
            outputs = "ema-reformatted-dates",
            name = "reformat-dates-ema"
        ),
        node(
            func=nodes.deduplicate_dataframe,
            inputs = [
                "ema-reformatted-dates",
                "params:deduplication_columns",
            ],
            outputs = "ema-deduplicated",
            name = "deduplicate-ema"
        ),
        node(
            func = nameres.nameres_column,
            inputs = [
                "ema-deduplicated",
                "params:standardization_mapping_ob.Ingredient",
                "params:name_resolver_params"
            ],
            outputs = "ema-nameresolved",
            name = "nameres-ema"
        ),
        node(
            func = nodes.qc_id_llm,
            inputs = [
                "ema-nameresolved",
                "params:id_correct_incorrect_tag",
            ],
            outputs = "ema-llm-id-qc",
            name="qc-id-llm-ema"
        ),
        node(
            func=nodes.improve_ids,
            inputs = [
                "ema-llm-id-qc",
                "params:name_resolver_params_llm_improve",
                "params:llm_best_id_tag_drug_prompt"
            ],
            outputs = [
                "ema-corrected-ids",
                "ema-nameres-errors"
            ],
            name = "select-best-ids-ema"
        ),
        node(
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "ema-corrected-ids",
                "params:approval_tags.eur",
                "params:true_bool",
            ],
            outputs="ema-approved-tags",
            name = "add-approval-tags-ema"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "ema-approved-tags",
                "params:combo_therapy_tags",
                "params:source_ingredients_column"
            ],
            outputs = "ema-combo-therapy-tags",
            name = 'tag-combo-therapies-ema'
        ),
        node(
            func=nodes.split_combination_therapies,
            inputs=[
                "ema-combo-therapy-tags",
                "params:combination_therapy_split_drug"
            ],
            outputs = "ema-split-ingredients",
            name = "split-ingredients-ema"
        ),
        node(
            func=nameres.nameres_column_combination_therapy_ingredients,
            inputs = [
                "ema-split-ingredients",
                "params:combination_therapy_split_drug.output_col",
                "params:name_resolver_params"
            ],
            outputs = "ema-component-ids",
            name = "id-ingredients-ema"
        ),
        node(
            func=nodes.add_unlisted_ingredients,
            inputs = "ema-component-ids",
            outputs = "ema-unlisted-single-ingredients",
            name = "add-unlisted-ingredients-ema"
        ),
        node(
            func=normalize.normalize_column,
            inputs = [
                "ema-unlisted-single-ingredients",
                "params:best_id_column"
            ],
            outputs = "ema-norm",
            name = "normalize-ema"
        ),


##########################################################################################################
########### PMDA #########################################################################################
##########################################################################################################

        node(
            func=preprocess_lists.preprocess_pmda,
            inputs = "pmda-products",
            outputs = "pmda-preprocessed",
            name = "preprocess-pmda"
        ),
        node(
            func = nodes.standardize_dataframe,
            inputs = [
                "pmda-preprocessed",
                "params:standardization_mapping_pmda",

            ],
            outputs = "pmda-standardized",
            name = "standardize-cols-pmda"
        ),
        node(
            func=nodes.deduplicate_dataframe,
            inputs = [
                "pmda-standardized",
                "params:deduplication_columns",
            ],
            outputs = "pmda-deduplicated",
            name = "deduplicate-pmda"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "pmda-deduplicated",
                "params:date_reformatting_tag",
                "params:source_ingredients_column"
            ],
            outputs = "pmda-reformatted-dates",
            name = 'reformat-dates-pmda'
        ),
        node(
            func = nameres.nameres_column,
            inputs = [
                "pmda-reformatted-dates",
                "params:standardization_mapping_ob.Ingredient",
                "params:name_resolver_params"
            ],
            outputs = "pmda-nameresolved",
            name = "nameres-pmda"
        ),
        node(
            func = nodes.qc_id_llm,
            inputs = [
                "pmda-nameresolved",
                "params:id_correct_incorrect_tag",
            ],
            outputs = "pmda-llm-id-qc",
            name="qc-id-llm-pmda"
        ),
        node(
            func=nodes.improve_ids,
            inputs = [
                "pmda-llm-id-qc",
                "params:name_resolver_params_llm_improve",
                "params:llm_best_id_tag_drug_prompt"
            ],
            outputs = [
                "pmda-corrected-ids",
                "pmda-nameres-errors"
            ],
            name = "select-best-ids-pmda"
        ),
        node(
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "pmda-corrected-ids",
                "params:approval_tags.jpn",
                "params:true_bool",
            ],
            outputs="pmda-approved-tags",
            name = "add-approval-tags-pmda"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "pmda-approved-tags",
                "params:combo_therapy_tags",
                "params:source_ingredients_column"
            ],
            outputs = "pmda-combo-therapy-tags",
            name = 'tag-combo-therapies-pmda'
        ),
        node(
            func=nodes.split_combination_therapies,
            inputs=[
                "pmda-combo-therapy-tags",
                "params:combination_therapy_split_drug"
            ],
            outputs = "pmda-split-ingredients",
            name = "split-ingredients-pmda"
        ),
        node(
            func=nameres.nameres_column_combination_therapy_ingredients,
            inputs = [
                "pmda-split-ingredients",
                "params:combination_therapy_split_drug.output_col",
                "params:name_resolver_params"
            ],
            outputs = "pmda-component-ids",
            name = "id-ingredients-pmda"
        ),
        node(
            func=nodes.add_unlisted_ingredients,
            inputs = "pmda-component-ids",
            outputs = "pmda-unlisted-single-ingredients",
            name = "add-unlisted-ingredients-pmda"
        ),
        node(
            func=normalize.normalize_column,
            inputs = [
                "pmda-approved-tags",
                "params:best_id_column"
            ],
            outputs = "pmda-norm",
            name = "normalize-pmda"
        ),

##########################################################################################################
########### ALL LISTS ####################################################################################
##########################################################################################################

        node(
            func=nodes.join_lists,
            inputs = [
                "ob-norm",
                "pb-norm",
                "ema-norm",
                "pmda-norm"
            ],
            outputs = "joined-list",
            name = "join-lists"
        ),
        node(
            func=openai_tags.add_tags,
            inputs = [
                "joined-list",
                "params:enrichment_tags",
                "params:label_column",
            ],
            outputs = "list-with-tags",
            name = "add-drug-tags"
        ),
        node(
            func=get_atc.get_atc_codes_for_dataframe,
            inputs = [
                "list-with-tags",
                "atc-codes",
            ],
            outputs="list-with-atc",
            name = "get-atc"
        ),
        node(
            func=get_smiles.add_SMILES_strings,
            inputs = "list-with-atc",
            outputs = "list-with-smiles",
            name = "get-smiles"
        )
    ])
