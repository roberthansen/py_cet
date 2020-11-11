from run import CET_Scenario
from models import EDCS_Query_Results

from login import user

import pandas as pd

sample_data_directory = '/home/rhansen/src/python/sqlserver/py_cet/Tables/'
acc_source_directory = '/home/rhansen/src/python/sqlserver/py_cet/Tables/'
'''
cet_scenario = CET_Scenario(
    user = {'id':'','passwd':''},
    inputs_source='csv',
    input_measures_source_name=sample_data_directory + 'InputMeasureCEDARS.csv',
    input_programs_source_name=sample_data_directory + 'InputProgramCEDARS.csv',
    acc_source='csv',
    acc_source_directory=sample_data_directory,
    acc_version=2018,
    first_year=2018,
    market_effects_benefits=0.05,
    market_effects_costs=0.05,
    parallelize=True,
    match_sql=True
)
cet_scenario = CET_Scenario(
    user=user,
    inputs_source='database',
    input_measures_source_name='InputMeasureCEDARS',
    input_programs_source_name='InputProgramCEDARS',
    acc_source='database',
    acc_version=2018,
    first_year=2018,
    market_effects_benefits=0.05,
    market_effects_costs=0.05,
    parallelize=False,
    match_sql=True
)
'''
cet_scenario = CET_Scenario(
    user=user,
    inputs_source='database',
    input_measures_source_name='InputMeasureCEDARS',
    input_programs_source_name='InputProgramCEDARS',
    acc_source='database',
    acc_version=2018,
    first_year=2018,
    market_effects_benefits=0.05,
    market_effects_costs=0.05,
    parallelize=True,
    match_sql=True
)

cet_scenario.run_cet()

py_cet_output = cet_scenario.OutputMeasures.data

sql_cet_output = EDCS_Query_Results('''
SELECT *
FROM OutputCE AS A
LEFT JOIN (
    SELECT
        CET_ID AS CET_ID_2,
        JobID,
        NetElecCO2,
        NetGasCO2,
        GrossElecCO2,
        GrossGasCO2,
        NetElecCO2Lifecycle,
        NetGasCO2Lifecycle,
        GrossElecCO2Lifecycle,
        GrossGasCO2Lifecycle,
        NetElecNOx,
        NetGasNOx,
        GrossElecNOx,
        GrossGasNOx,
        NetElecNOxLifecycle,
        NetGasNOxLifecycle,
        GrossElecNOxLifecycle,
        GrossGasNOxLifecycle,
        NetPM10,
        GrossPM10,
        NetPM10Lifecycle,
        GrossPM10Lifecycle
    FROM OutputEmissions
    WHERE JobID=10000
) AS B
ON A.CET_ID=B.CET_ID_2
AND A.JobID=B.JobID
WHERE A.JobID=10000
ORDER BY
    A.CET_ID''',
    user['id'],
    user['passwd']
).data

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
    'NetElecCO2',
    'NetGasCO2',
    'NetElecCO2Lifecycle',
    'NetGasCO2Lifecycle',
    'NetElecNOx',
    'NetGasNOx',
    'NetElecNOxLifecycle',
    'NetGasNOxLifecycle',
    'NetPM10',
    'NetPM10Lifecycle',
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
        'CO2NetElectricFirstYear',
        'CO2NetGasFirstYear',
        'CO2NetElectricLifecycle',
        'CO2NetGasLifecycle',
        'NOxNetElectricFirstYear',
        'NOxNetGasFirstYear',
        'NOxNetElectricLifecycle',
        'NOxNetGasLifecycle',
        'PM10NetFirstYear',
        'PM10NetLifecycle',
    ]], how='left', on='CET_ID', suffixes=('_sql','_py')
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
    ['NetElecCO2','CO2NetElectricFirstYear_SQL'],
    ['NetGasCO2','CO2NetGasFirstYear_SQL'],
    ['NetElecCO2Lifecycle','CO2NetElectricLifecycle_SQL'],
    ['NetGasCO2Lifecycle','CO2NetGasLifecycle_SQL'],
    ['NetElecNOx','NOxNetElectricFirstYear_SQL'],
    ['NetGasNOx','NOxNetGasFirstYear_SQL'],
    ['NetElecNOxLifecycle','NOxNetElectricLifecycle_SQL'],
    ['NetGasNOxLifecycle','NOxNetGasLifecycle_SQL'],
    ['NetPM10','PM10NetFirstYear_SQL'],
    ['NetPM10Lifecycle','PM10NetLifecycle_SQL'],
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
    ['CO2NetElectricFirstYear','CO2NetElectricFirstYear_PY'],
    ['CO2NetGasFirstYear','CO2NetGasFirstYear_PY'],
    ['CO2NetElectricLifecycle','CO2NetElectricLifecycle_PY'],
    ['CO2NetGasLifecycle','CO2NetGasLifecycle_PY'],
    ['NOxNetElectricFirstYear','NOxNetElectricFirstYear_PY'],
    ['NOxNetGasFirstYear','NOxNetGasFirstYear_PY'],
    ['NOxNetElectricLifecycle','NOxNetElectricLifecycle_PY'],
    ['NOxNetGasLifecycle','NOxNetGasLifecycle_PY'],
    ['PM10NetFirstYear','PM10NetFirstYear_PY'],
    ['PM10NetLifecycle','PM10NetLifecycle_PY'],
]

for old_column,new_column in column_name_map:
    comparison = comparison.rename(columns={old_column:new_column},index={})

comparison = pd.DataFrame({
    'CET_ID' : comparison.CET_ID,
    'ElecBen_SQL': comparison.ElecBen_SQL,
    'ElecBen_PY' : comparison.ElecBen_PY,
    'ElecBen_Diff' : comparison.ElecBen_PY - comparison.ElecBen_SQL,
    'ElecBen_PercentDiff' : (comparison.ElecBen_PY - comparison.ElecBen_SQL) / comparison.ElecBen_SQL,
    'GasBen_SQL' : comparison.GasBen_SQL,
    'GasBen_PY' : comparison.GasBen_PY,
    'GasBen_Diff' : comparison.GasBen_PY - comparison.GasBen_SQL,
    'GasBen_PercentDiff' : (comparison.GasBen_PY - comparison.GasBen_SQL) / comparison.GasBen_SQL,
    'TRCCost_SQL' : comparison.TRCCost_SQL,
    'TRCCost_PY' : comparison.TRCCost_PY,
    'TRCCost_Diff' : comparison.TRCCost_PY - comparison.TRCCost_SQL,
    'TRCCost_PercentDiff' : (comparison.TRCCost_PY - comparison.TRCCost_SQL) / comparison.TRCCost_SQL,
    'TRCCostNoAdmin_SQL' : comparison.TRCCostNoAdmin_SQL,
    'TRCCostNoAdmin_PY' : comparison.TRCCostNoAdmin_PY,
    'TRCCostNoAdmin_Diff' : comparison.TRCCostNoAdmin_PY - comparison.TRCCostNoAdmin_SQL,
    'TRCCostNoAdmin_PercentDiff' : (comparison.TRCCostNoAdmin_PY - comparison.TRCCostNoAdmin_SQL) / comparison.TRCCostNoAdmin_SQL,
    'TRCRatio_SQL' : comparison.TRCRatio_SQL,
    'TRCRatio_PY' : comparison.TRCRatio_PY,
    'TRCRatio_Diff' : comparison.TRCRatio_PY - comparison.TRCRatio_SQL,
    'TRCRatio_PercentDiff' : (comparison.TRCRatio_PY - comparison.TRCRatio_SQL) / comparison.TRCRatio_SQL,
    'TRCRatioNoAdmin_SQL' : comparison.TRCRatioNoAdmin_SQL,
    'TRCRatioNoAdmin_PY' : comparison.TRCRatioNoAdmin_PY,
    'TRCRatioNoAdmin_Diff' : comparison.TRCRatioNoAdmin_PY - comparison.TRCRatioNoAdmin_SQL,
    'TRCRatioNoAdmin_PercentDiff' : (comparison.TRCRatioNoAdmin_PY - comparison.TRCRatioNoAdmin_SQL) / comparison.TRCRatioNoAdmin_SQL,
    'PACCost_SQL' : comparison.PACCost_SQL,
    'PACCost_PY' : comparison.PACCost_PY,
    'PACCost_Diff' : comparison.PACCost_PY - comparison.PACCost_SQL,
    'PACCost_PercentDiff' : (comparison.PACCost_PY - comparison.PACCost_SQL) / comparison.PACCost_SQL,
    'PACCostNoAdmin_SQL' : comparison.PACCostNoAdmin_SQL,
    'PACCostNoAdmin_PY' : comparison.PACCostNoAdmin_PY,
    'PACCostNoAdmin_Diff' : comparison.PACCostNoAdmin_PY - comparison.PACCostNoAdmin_SQL,
    'PACCostNoAdmin_PercentDiff' : (comparison.PACCostNoAdmin_PY - comparison.PACCostNoAdmin_SQL) / comparison.PACCostNoAdmin_SQL,
    'PACRatio_SQL' : comparison.PACRatio_SQL,
    'PACRatio_PY' : comparison.PACRatio_PY,
    'PACRatio_Diff' : comparison.PACRatio_PY - comparison.PACRatio_SQL,
    'PACRatio_PercentDiff' : (comparison.PACRatio_PY - comparison.PACRatio_SQL) / comparison.PACRatio_SQL,
    'PACRatioNoAdmin_SQL' : comparison.PACRatioNoAdmin_SQL,
    'PACRatioNoAdmin_PY' : comparison.PACRatioNoAdmin_PY,
    'PACRatioNoAdmin_Diff' : comparison.PACRatioNoAdmin_PY - comparison.PACRatioNoAdmin_SQL,
    'PACRatioNoAdmin_PercentDiff' : (comparison.PACRatioNoAdmin_PY - comparison.PACRatioNoAdmin_SQL) / comparison.PACRatioNoAdmin_SQL,
    'BillReducElec_SQL' : comparison.BillReducElec_SQL,
    'BillReducElec_PY' : comparison.BillReducElec_PY,
    'BillReducElec_Diff' : comparison.BillReducElec_PY - comparison.BillReducElec_SQL,
    'BillReducElec_PercentDiff' : (comparison.BillReducElec_PY - comparison.BillReducElec_SQL) / comparison.BillReducElec_SQL,
    'BillReducGas_SQL' : comparison.BillReducGas_SQL,
    'BillReducGas_PY' : comparison.BillReducGas_PY,
    'BillReducGas_Diff' : comparison.BillReducGas_PY - comparison.BillReducGas_SQL,
    'BillReducGas_PercentDiff' : (comparison.BillReducGas_PY - comparison.BillReducGas_SQL) / comparison.BillReducGas_SQL,
    'RIMCost_SQL' : comparison.RIMCost_SQL,
    'RIMCost_PY' : comparison.RIMCost_PY,
    'RIMCost_Diff' : comparison.RIMCost_PY - comparison.RIMCost_SQL,
    'RIMCost_PercentDiff' : (comparison.RIMCost_PY - comparison.RIMCost_SQL) / comparison.RIMCost_SQL,
    'CO2NetElectricFirstYear_SQL' : comparison.CO2NetElectricFirstYear_SQL,
    'CO2NetElectricFirstYear_PY' : comparison.CO2NetElectricFirstYear_PY,
    'CO2NetElectricFirstYear_Diff' : comparison.CO2NetElectricFirstYear_PY - comparison.CO2NetElectricFirstYear_SQL,
    'CO2NetElectricFirstYear_PercentDiff' : (comparison.CO2NetElectricFirstYear_PY - comparison.CO2NetElectricFirstYear_SQL) / comparison.CO2NetElectricFirstYear_SQL,
    'CO2NetGasFirstYear_SQL': comparison.CO2NetGasFirstYear_SQL,
    'CO2NetGasFirstYear_PY': comparison.CO2NetGasFirstYear_PY,
    'CO2NetGasFirstYear_Diff' : comparison.CO2NetGasFirstYear_PY - comparison.CO2NetGasFirstYear_SQL,
    'CO2NetGasFirstYear_PercentDiff' : (comparison.CO2NetGasFirstYear_PY - comparison.CO2NetGasFirstYear_SQL) / comparison.CO2NetGasFirstYear_SQL,
    'CO2NetElectricLifecycle_SQL': comparison.CO2NetElectricLifecycle_SQL,
    'CO2NetElectricLifecycle_PY': comparison.CO2NetElectricLifecycle_PY,
    'CO2NetElectricLifecycle_Diff' : comparison.CO2NetElectricLifecycle_PY - comparison.CO2NetElectricLifecycle_SQL,
    'CO2NetElectricLifecycle_PercentDiff' : (comparison.CO2NetElectricLifecycle_PY - comparison.CO2NetElectricLifecycle_SQL) / comparison.CO2NetElectricLifecycle_SQL,
    'CO2NetGasLifecycle_SQL': comparison.CO2NetGasLifecycle_SQL,
    'CO2NetGasLifecycle_PY': comparison.CO2NetGasLifecycle_PY,
    'CO2NetGasLifecycle_Diff' : comparison.CO2NetGasLifecycle_PY - comparison.CO2NetGasLifecycle_SQL,
    'CO2NetGasLifecycle_PercentDiff' : (comparison.CO2NetGasLifecycle_PY - comparison.CO2NetGasLifecycle_SQL) / comparison.CO2NetGasLifecycle_SQL,
    'NOxNetElectricFirstYear_SQL': comparison.NOxNetElectricFirstYear_SQL,
    'NOxNetElectricFirstYear_PY': comparison.NOxNetElectricFirstYear_PY,
    'NOxNetElectricFirstYear_Diff' : comparison.NOxNetElectricFirstYear_PY - comparison.NOxNetElectricFirstYear_SQL,
    'NOxNetElectricFirstYear_PercentDiff' : (comparison.NOxNetElectricFirstYear_PY - comparison.NOxNetElectricFirstYear_SQL) / comparison.NOxNetElectricFirstYear_SQL,
    'NOxNetGasFirstYear_SQL': comparison.NOxNetGasFirstYear_SQL,
    'NOxNetGasFirstYear_PY': comparison.NOxNetGasFirstYear_PY,
    'NOxNetGasFirstYear_Diff' : comparison.NOxNetGasFirstYear_PY - comparison.NOxNetGasFirstYear_SQL,
    'NOxNetGasFirstYear_PercentDiff' : (comparison.NOxNetGasFirstYear_PY - comparison.NOxNetGasFirstYear_SQL) / comparison.NOxNetGasFirstYear_SQL,
    'NOxNetElectricLifecycle_SQL': comparison.NOxNetElectricLifecycle_SQL,
    'NOxNetElectricLifecycle_PY': comparison.NOxNetElectricLifecycle_PY,
    'NOxNetElectricLifecycle_Diff' : comparison.NOxNetElectricLifecycle_PY - comparison.NOxNetElectricLifecycle_SQL,
    'NOxNetElectricLifecycle_PercentDiff' : (comparison.NOxNetElectricLifecycle_PY - comparison.NOxNetElectricLifecycle_SQL) / comparison.NOxNetElectricLifecycle_SQL,
    'NOxNetGasLifecycle_SQL': comparison.NOxNetGasLifecycle_SQL,
    'NOxNetGasLifecycle_PY': comparison.NOxNetGasLifecycle_PY,
    'NOxNetGasLifecycle_Diff' : comparison.NOxNetGasLifecycle_PY - comparison.NOxNetGasLifecycle_SQL,
    'NOxNetGasLifecycle_PercentDiff' : (comparison.NOxNetGasLifecycle_PY - comparison.NOxNetGasLifecycle_SQL) / comparison.NOxNetGasLifecycle_SQL,
    'PM10NetFirstYear_SQL': comparison.PM10NetFirstYear_SQL,
    'PM10NetFirstYear_PY': comparison.PM10NetFirstYear_PY,
    'PM10NetFirstYear_Diff' : comparison.PM10NetFirstYear_PY - comparison.PM10NetFirstYear_SQL,
    'PM10NetFirstYear_PercentDiff' : (comparison.PM10NetFirstYear_PY - comparison.PM10NetFirstYear_SQL) / comparison.PM10NetFirstYear_SQL,
    'PM10NetLifecycle_SQL': comparison.PM10NetLifecycle_SQL,
    'PM10NetLifecycle_PY': comparison.PM10NetLifecycle_PY,
    'PM10NetLifecycle_Diff' : comparison.PM10NetLifecycle_PY - comparison.PM10NetLifecycle_SQL,
    'PM10NetLifecycle_PercentDiff' : (comparison.PM10NetLifecycle_PY - comparison.PM10NetLifecycle_SQL) / comparison.PM10NetLifecycle_SQL,
})

comparison.to_csv('Comparison.csv',index=False)
cet_scenario.InputMeasures.data.to_csv('InputMeasures.csv')
cet_scenario.InputPrograms.data.to_csv('InputPrograms.csv')
cet_scenario.Settings.data.to_csv('Settings.csv')
