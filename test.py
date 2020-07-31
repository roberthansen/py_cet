from run import CET_Scenario
from models import EDCS_Query_Results
from tables import user

import pandas as pd

cet_scenario = CET_Scenario(acc_version=2018, first_year=2018, market_effects=0.05, match_sql=True)
cet_scenario.run_cet()

py_cet_output = cet_scenario.OutputCE

sql_cet_output = EDCS_Query_Results('SELECT * FROM OutputCE WHERE JobID=0 ORDER BY CET_ID',user['id'],user['passwd']).data
sql_cet_output.CET_ID = sql_cet_output.CET_ID.map(int)

comparison = sql_cet_output[[
    'CET_ID',
    'ElecBen',
    'GasBen',
    'TRCCost',
    'TRCRatio'
]].merge(
    py_cet_output[[
        'CET_ID',
        'ElecBenNet',
        'GasBenNet',
        'TotalResourceCostNet',
        'TotalResourceCostRatio'
    ]], on='CET_ID', suffixes=('_sql','_py')
)
