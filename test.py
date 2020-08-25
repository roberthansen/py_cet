from run import CET_Scenario
from models import EDCS_Query_Results

from login import user

import pandas as pd

sample_data_directory = '/home/rhansen/src/python/sqlserver/SCE_July2020_Claims/'

#cet_scenario = CET_Scenario(user=user, inputs_source='csv', input_measures_source_name=sample_data_directory+'MeasureSubset.csv', input_programs_source_name=sample_data_directory+'ProgramCost.csv', acc_version=2019, first_year=2019, market_effects_benefits=0.05, market_effects_costs=0.05, match_sql=True)
cet_scenario = CET_Scenario(user=user, inputs_source='database', input_measures_source_name='InputMeasureCEDARS', input_programs_source_name='InputProgramCEDARS', acc_version=2018, first_year=2018, market_effects_benefits=0.05, market_effects_costs=0.05, parallelize=False, match_sql=True)
cet_scenario_mp = CET_Scenario(user=user, inputs_source='database', input_measures_source_name='InputMeasureCEDARS', input_programs_source_name='InputProgramCEDARS', acc_version=2018, first_year=2018, market_effects_benefits=0.05, market_effects_costs=0.05, parallelize=True, match_sql=True)

cet_scenario.run_cet()
cet_scenario_mp.run_cet()

py_cet_output = cet_scenario.OutputMeasures.data

sql_cet_output = EDCS_Query_Results('SELECT * FROM OutputCE WHERE JobID=10000 ORDER BY CET_ID',user['id'],user['passwd']).data

comparison = sql_cet_output[[
    'CET_ID',
    'ElecBen',
    'GasBen',
    'TRCCost',
    'TRCCostNoAdmin',
    'TRCRatio',
    'TRCRatioNoAdmin',
    'PACCost',
    'PACCostNoAdmin',
    'PACRatio',
    'PACRatioNoAdmin',
    'BillReducElec',
    'BillReducGas',
    'RIMCost',
]].merge(
    py_cet_output[[
        'CET_ID',
        'ElectricBenefitsNet',
        'GasBenefitsNet',
        'TotalResourceCostNet',
        'TotalResourceCostNetNoAdmin',
        'TotalResourceCostRatio',
        'TotalResourceCostRatioNoAdmin',
        'ProgramAdministratorCost',
        'ProgramAdministratorCostNoAdmin',
        'ProgramAdministratorCostRatio',
        'ProgramAdministratorCostRatioNoAdmin',
        'BillReductionElectric',
        'BillReductionGas',
        'RatepayerImpactMeasureCost',
    ]], on='CET_ID', suffixes=('_sql','_py')
)

column_name_map = [
    ['ElecBen','ElecBen_SQL'],
    ['GasBen','GasBen_SQL'],
    ['TRCCost','TRCCost_SQL'],
    ['TRCCostNoAdmin','TRCCostNoAdmin_SQL'],
    ['TRCRatio','TRCRatio_SQL'],
    ['TRCRatioNoAdmin','TRCRatioNoAdmin_SQL'],
    ['PACCost','PACCost_SQL'],
    ['PACCostNoAdmin','PACCostNoAdmin_SQL'],
    ['PACRatio','PACRatio_SQL'],
    ['PACRatioNoAdmin','PACRatioNoAdmin_SQL'],
    ['BillReducElec','BillReducElec_SQL'],
    ['BillReducGas','BillReducGas_SQL'],
    ['RIMCost','RIMCost_SQL'],
    ['ElectricBenefitsNet','ElecBen_PY'],
    ['GasBenefitsNet','GasBen_PY'],
    ['TotalResourceCostNet','TRCCost_PY'],
    ['TotalResourceCostNetNoAdmin','TRCCostNoAdmin_PY'],
    ['TotalResourceCostRatio','TRCRatio_PY'],
    ['TotalResourceCostRatioNoAdmin','TRCRatioNoAdmin_PY'],
    ['ProgramAdministratorCost','PACCost_PY'],
    ['ProgramAdministratorCostNoAdmin','PACCostNoAdmin_PY'],
    ['ProgramAdministratorCostRatio','PACRatio_PY'],
    ['ProgramAdministratorCostRatioNoAdmin','PACRatioNoAdmin_PY'],
    ['BillReductionElectric','BillReducElec_PY'],
    ['BillReductionGas','BillReducGas_PY'],
    ['RatepayerImpactMeasureCost','RIMCost_PY'],
]

for old_column,new_column in column_name_map:
    comparison = comparison.rename(columns={old_column:new_column},index={})

comparison = pd.DataFrame({
    'CET_ID' : comparison.CET_ID,
    'ElecBen_SQL': comparison.ElecBen_SQL,
    'ElecBen_PY' : comparison.ElecBen_PY,
    'ElecBen_Diff' : comparison.ElecBen_SQL - comparison.ElecBen_PY,
    'ElecBen_PercentDiff' : (comparison.ElecBen_SQL - comparison.ElecBen_PY) / comparison.ElecBen_SQL,
    'GasBen_SQL' : comparison.GasBen_SQL,
    'GasBen_PY' : comparison.GasBen_PY,
    'GasBen_Diff' : comparison.GasBen_SQL - comparison.GasBen_PY,
    'GasBen_PercentDiff' : (comparison.GasBen_SQL - comparison.GasBen_PY) / comparison.GasBen_SQL,
    'TRCCost_SQL' : comparison.TRCCost_SQL,
    'TRCCost_PY' : comparison.TRCCost_PY,
    'TRCCost_Diff' : comparison.TRCCost_SQL - comparison.TRCCost_PY,
    'TRCCost_PercentDiff' : (comparison.TRCCost_SQL - comparison.TRCCost_PY) / comparison.TRCCost_SQL,
    'TRCCostNoAdmin_SQL' : comparison.TRCCostNoAdmin_SQL,
    'TRCCostNoAdmin_PY' : comparison.TRCCostNoAdmin_PY,
    'TRCCostNoAdmin_Diff' : comparison.TRCCostNoAdmin_SQL - comparison.TRCCostNoAdmin_PY,
    'TRCCostNoAdmin_PercentDiff' : (comparison.TRCCostNoAdmin_SQL - comparison.TRCCostNoAdmin_PY) / comparison.TRCCostNoAdmin_SQL,
    'TRCRatio_SQL' : comparison.TRCRatio_SQL,
    'TRCRatio_PY' : comparison.TRCRatio_PY,
    'TRCRatio_Diff' : comparison.TRCRatio_SQL - comparison.TRCRatio_PY,
    'TRCRatio_PercentDiff' : (comparison.TRCRatio_SQL - comparison.TRCRatio_PY) / comparison.TRCRatio_SQL,
    'TRCRatioNoAdmin_SQL' : comparison.TRCRatioNoAdmin_SQL,
    'TRCRatioNoAdmin_PY' : comparison.TRCRatioNoAdmin_PY,
    'TRCRatioNoAdmin_Diff' : comparison.TRCRatioNoAdmin_SQL - comparison.TRCRatioNoAdmin_PY,
    'TRCRatioNoAdmin_PercentDiff' : (comparison.TRCRatioNoAdmin_SQL - comparison.TRCRatioNoAdmin_PY) / comparison.TRCRatioNoAdmin_SQL,
    'PACCost_SQL' : comparison.PACCost_SQL,
    'PACCost_PY' : comparison.PACCost_PY,
    'PACCost_Diff' : comparison.PACCost_SQL - comparison.PACCost_PY,
    'PACCost_PercentDiff' : (comparison.PACCost_SQL - comparison.PACCost_PY) / comparison.PACCost_SQL,
    'PACCostNoAdmin_SQL' : comparison.PACCostNoAdmin_SQL,
    'PACCostNoAdmin_PY' : comparison.PACCostNoAdmin_PY,
    'PACCostNoAdmin_Diff' : comparison.PACCostNoAdmin_SQL - comparison.PACCostNoAdmin_PY,
    'PACCostNoAdmin_PercentDiff' : (comparison.PACCostNoAdmin_SQL - comparison.PACCostNoAdmin_PY) / comparison.PACCostNoAdmin_SQL,
    'PACRatio_SQL' : comparison.PACRatio_SQL,
    'PACRatio_PY' : comparison.PACRatio_PY,
    'PACRatio_Diff' : comparison.PACRatio_SQL - comparison.PACRatio_PY,
    'PACRatio_PercentDiff' : (comparison.PACRatio_SQL - comparison.PACRatio_PY) / comparison.PACRatio_SQL,
    'PACRatioNoAdmin_SQL' : comparison.PACRatioNoAdmin_SQL,
    'PACRatioNoAdmin_PY' : comparison.PACRatioNoAdmin_PY,
    'PACRatioNoAdmin_Diff' : comparison.PACRatioNoAdmin_SQL - comparison.PACRatioNoAdmin_PY,
    'PACRatioNoAdmin_PercentDiff' : (comparison.PACRatioNoAdmin_SQL - comparison.PACRatioNoAdmin_PY) / comparison.PACRatioNoAdmin_SQL,
    'BillReducElec_SQL' : comparison.BillReducElec_SQL,
    'BillReducElec_PY' : comparison.BillReducElec_PY,
    'BillReducElec_Diff' : comparison.BillReducElec_SQL - comparison.BillReducElec_PY,
    'BillReducElec_PercentDiff' : (comparison.BillReducElec_SQL - comparison.BillReducElec_PY) / comparison.BillReducElec_SQL,
    'BillReducGas_SQL' : comparison.BillReducGas_SQL,
    'BillReducGas_PY' : comparison.BillReducGas_PY,
    'BillReducGas_Diff' : comparison.BillReducGas_SQL - comparison.BillReducGas_PY,
    'BillReducGas_PercentDiff' : (comparison.BillReducGas_SQL - comparison.BillReducGas_PY) / comparison.BillReducGas_SQL,
    'RIMCost_SQL' : comparison.RIMCost_SQL,
    'RIMCost_PY' : comparison.RIMCost_PY,
    'RIMCost_Diff' : comparison.RIMCost_SQL - comparison.RIMCost_PY,
    'RIMCost_PercentDiff' : (comparison.RIMCost_SQL - comparison.RIMCost_PY) / comparison.RIMCost_SQL,
})
