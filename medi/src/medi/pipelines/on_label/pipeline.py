"""
This is a boilerplate pipeline 'on_label'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from . import nodes, mine_fda_indications

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # INDICATIONS
        node(
            func = mine_fda_indications.mine_indications,
            inputs = "params:path_to_fda_labels",
            outputs = "dailymed_raw",
            name = "mine-indications-fda" 
        ),
        node(
            func=nodes.mine_contraindications,
            inputs = [
               "params:path_to_fda_labels",
            ],
            outputs = "dailymed_contraindications_raw",
            name = "mine-contraindications-fda",
        ),


    ])
