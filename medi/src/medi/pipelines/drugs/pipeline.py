
from kedro.pipeline import node, Pipeline, pipeline
from . import nodes, extract_ob, get_marketing, get_earliest_approval_date_ob




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
                "params:standardization_cols_in_ob",
                "params:standardization_cols_out_ob",
            ],
            outputs = "ob-standardized",
            name = "standardize-orangebook"
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
