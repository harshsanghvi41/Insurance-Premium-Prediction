import yaml
from premium.exception import PremiumException
import os,sys
import numpy as np
import dill
import pandas as pd
from premium.constant import *


def read_yaml_file(file_path:str) -> dict:
    """
    Reads a YAML file and returns the contents as a dictionary.
    file_path: str
    """
    try:
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)

    except Exception as e:
        raise PremiumException(e,sys) from e

