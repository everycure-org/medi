
from kedro.pipeline import node, Pipeline, pipeline
from . import nodes, extract_ob, get_marketing, get_earliest_approval_date_ob

import os
#print(str(os.getcwd()))
from medi.utils import nameres
from medi.utils import openai_tags




def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=get_marketing.add_most_permissive_marketing_tags_fda,
            inputs = "orange-book-products",
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
            func = nodes.standardize_dataframe,
            inputs = [
                "ob-reformatted-dates",
                "params:standardization_mapping_ob",

            ],
            outputs = "ob-standardized",
            name = "standardize-ob"
        ),
        node(
            func=nodes.deduplicate_dataframe,
            inputs = [
                "ob-standardized",
                "params:deduplication_columns",
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
        node(
            func=nameres.identify_components,
            inputs=[
                "ob-combo-therapy-tags",
                "params:combo_therapy_tags.combination_therapy_split_drug.output_col",
                "params:component_ids_colname",
                "params:name_resolver_params",
            ],
            outputs = "ob-component-ids",
            name = "identify-ingredients-ob"
        )

        # node(
        #     func=nodes.create_standardized_columns,
        #     inputs=[
        #         'orangebook_list_with_marketing_status',
        #         'params:orangebook_drug_name_column',
        #         'params:orangebook_approval_date_column',
        #     ],
        #     outputs = 'orange_book_list_standardized',
        #     name = 'standardize-orangebook'
        # ),
        
    ])
