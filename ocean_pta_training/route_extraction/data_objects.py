from dataclasses import dataclass
from numpy import array as np_array, ndarray as np_ndarray
from typing import Dict

@dataclass
class VesselPortSequence:
    """
    Data class version of the named tuple VesselPortSequence
    from the original Jupyter notebook
    """
    port_str: str
    port_map: Dict
    row_pos: np_ndarray

    def __init__(self, port_str: str, port_map: Dict, row_pos: np_ndarray):
        self.port_str = port_str
        self.port_map = port_map
        self.row_pos = row_pos
