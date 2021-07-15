import numpy as np
import pandas as pd

class Enrich:

    def enrich(self, dffinal):
        chartAxis = []
        for row in dffinal['is_categoricalColumn']:
            if row == False:
                chartAxis.append('group')
            else:
                chartAxis.append('dimension')
        dffinal["chartAxis"] = chartAxis

        return dffinal