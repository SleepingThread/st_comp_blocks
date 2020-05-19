import json

import numpy as np


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        return json.JSONEncoder.default(self, obj)
