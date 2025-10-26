"""
This is a boilerplate pipeline 'on_label'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from . import nodes, mine_fda_indications, gemini_batch_prompt

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # INDICATIONS
        node(
            func = mine_fda_indications.mine_indications,
            inputs = "params:path_to_fda_labels",
            outputs = "dailymed_indications_raw",
            name = "mine-indications-fda" 
        ),
        # node(
        #     func=nodes.extract_named_diseases,
        #     inputs = [
        #         "dailymed_indications_raw",
        #         "params:column_names.indications_active_ingredients",
        #         "params:column_names.indications_text_column",
        #         "params:column_names.indications_structured_list_column",
        #         "params:indications_structured_list_prompt",
        #     ],
        #     outputs = "dailymed_indications_named_diseases",
        #     name = "extract-indications-lists-fda",
        # ),
        # node(
        #     func=nodes.extract_named_diseases,
        #     inputs = [
        #         "dailymed_indications_named_diseases",
        #         "params:column_names.indications_active_ingredients",
        #         "params:column_names.indications_text_column",
        #         "params:column_names.indications_structured_list_column",
        #         "params:indications_hyperrelations_prompt",
        #     ],
        #     outputs = "dailymed_indications_hyperrelations",
        #     name = "extract-hyperrelations-fda",
        # ),

        node(
            func=nodes.mine_contraindications,
            inputs = [
               "params:path_to_fda_labels",
            ],
            outputs = "dailymed_contraindications_raw",
            name = "mine-contraindications-fda",
        ),
        node(
            func=gemini_batch_prompt.process_batch_with_gemini_api,
            inputs = [
                "dailymed_indications_raw",
                "params:indications_structured_list_prompt",
                "params:gemini_env_var",
                "params:column_names.indications_text_column",
                "params:column_names.indications_structured_list_column"
            ],
            outputs = "dailymed_indications_named_diseases",
            name = "extract-indications-lists-fda"
        ),
        node(
            func=gemini_batch_prompt.process_batch_with_gemini_api,
            inputs = [
                "dailymed_indications_named_diseases",
                "params:indications_hyperrelations_prompt",
                "params:gemini_env_var",
                "params:column_names.indications_text_column",
                "params:column_names.hyperrelations_column"
            ],
            outputs = "dailymed_indications_hyperrelations",
            name = "extract-hyperrelations-fda"
        )
        


    ])
