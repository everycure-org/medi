
from kedro.pipeline import node, Pipeline, pipeline
from . import nodes
from utils import normalize 



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=nodes.
        )
    ])
