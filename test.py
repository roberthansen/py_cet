from run import CET_Scenario
from models import EDCS_Query_Results

from login import user

import pandas as pd

cet_scenario = CET_Scenario(user=user, acc_version=2018, first_year=2018, market_effects=0.05, match_sql=True)
cet_scenario.run_cet()

py_cet_output = cet_scenario.OutputMeasures.data

sql_cet_output = EDCS_Query_Results('SELECT * FROM OutputCE WHERE JobID=10000 ORDER BY CET_ID',user['id'],user['passwd']).data
sql_cet_output.CET_ID = sql_cet_output.CET_ID.map(int)

comparison = sql_cet_output[[
    'CET_ID',
    'ElecBen',
    'GasBen',
    'TRCCost',
    'TRCCostNoAdmin',
    'TRCRatio',
    'PACCost',
    'PACRatio',
]].merge(
    py_cet_output[[
        'CET_ID',
        'ElectricBenefitsNet',
        'GasBenefitsNet',
        'TotalResourceCostNet',
        'TotalResourceCostNetNoAdmin',
        'TotalResourceCostRatio',
        'ProgramAdministratorCost',
        'ProgramAdministratorCostRatio',
    ]], on='CET_ID', suffixes=('_sql','_py')
)
