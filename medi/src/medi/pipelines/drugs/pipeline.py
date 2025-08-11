
from kedro.pipeline import node, Pipeline, pipeline
from . import nodes, extract_ob, get_marketing, get_earliest_approval_date_ob

import os
#print(str(os.getcwd()))
from medi.utils import nameres
from medi.utils import openai_tags
from medi.utils import preprocess_lists




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
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "ob-nameresolved",
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
            ],
            outputs = "ob-combo-therapy-tags",
            name = 'tag-combo-therapies-ob'
        ),
        # node(
        #     func=nameres.identify_components,
        #     inputs=[
        #         "ob-combo-therapy-tags",
        #         "params:combo_therapy_tags.combination_therapy_split_drug.output_col",
        #         "params:component_ids_colname",
        #         "params:name_resolver_params",
        #     ],
        #     outputs = "ob-component-ids",
        #     name = "identify-ingredients-ob"
        # )


##########################################################################################################
########### PB ###########################################################################################
##########################################################################################################
        # node(
        #     func=get_earliest_approval_date_ob.acquire_earliest_approval_dates,
        #     inputs = "ob-with-marketing-tags",
        #     outputs = "ob-reformatted-dates",
        #     name = "reformat-dates-ob"
        # ),
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
            func=get_earliest_approval_date_ob.acquire_earliest_approval_dates,
            inputs = "pb-marketing-tags",
            outputs = "pb-reformatted-dates",
            name = "reformat-dates-pb"
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
            func=nodes.add_full_column_identical_strings,
            inputs = [
                "ema-nameresolved",
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
            ],
            outputs = "ema-combo-therapy-tags",
            name = 'tag-combo-therapies-ema'
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
            func = nameres.nameres_column,
            inputs = [
                "pmda-standardized",
                "params:standardization_mapping_ob.Ingredient",
                "params:name_resolver_params"
            ],
            outputs = "pmda-nameresolved",
            name = "nameres-pmda"
        ),

    ])
