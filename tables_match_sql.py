import numpy as np
import types
from models import EDCS_Connection, EDCS_Table, EDCS_Query_Results, Local_CSV

from tables import setup_input_programs, setup_settings, \
    setup_emissions, \
    setup_combustion_types, \
    setup_output_measures, setup_output_programs, setup_output_portfolio,\
    setup_rate_schedule_electric, setup_rate_schedule_gas

from login import user

def setup_input_measures(source, source_name, first_year, market_effects_benefits, market_effects_costs, user):
    if source == 'csv':
        if source_name == 'Measure.csv':
            InputMeasures = Local_CSV(source_name, delimiter='|')
        else:
            InputMeasures = Local_CSV(source_name, delimiter=',')
    elif source == 'database':
        InputMeasures = EDCS_Table(source_name,user['id'],user['passwd'])
    else:
        InputMeasures = EDCS_Table('InputMeasureCEDARS',user['id'],user['passwd'])

    # fix input measure column name and type issues:
    column_name_map = [
        ['BldgType','BuildingType'],
        ['CEInputID','CET_ID'],
        ['ClaimYearQuarter','InstallationQuarter'],
        ['ClimateZone','ClimateZone'],
        ['DILaborCost','UnitLaborCost'],
        ['DIMaterialCost','UnitMaterialsCost'],
        ['E3ClimateZone','ClimateZone'],
        ['E3GasSavProfile','GasSavingsProfile'],
        ['E3GasSector','GasTargetSector'],
        ['E3MeaElecEndUseShape','ElectricEndUse'],
        ['E3TargetSector','ElectricTargetSector'],
        ['EUL_Yrs','EUL'],
        ['ElecEndUseShape','ElectricEndUse'],
        ['ElecRateSchedule','ElectricRateSchedule'],
        ['ElecTargetSector','ElectricTargetSector'],
        ['EndUserRebate','UnitEndUserRebate'],
        ['GasSector','GasTargetSector'],
        ['IRThm','IRTherm'],
        ['IncentiveToOthers','UnitIncentiveToOthers'],
        ['InstallationRateTherm','IRTherm'],
        ['InstallationRatekW','IRkW'],
        ['InstallationRatekWh','IRkWh'],
        ['MarketEffectBens','MarketEffectsBenefits'],
        ['MarketEffectCost','MarketEffectsCosts'],
        ['MeasAppType','MeasureApplicationType'],
        ['MeasCode','MeasureCode'],
        ['MeasDescription','MeasureDescription'],
        ['MeasImpactType','ImpactType'],
        ['MeasInflation','AnnualInflationRate'],
        ['NTGRThm','NTGRTherm'],
        ['NormUnit','NormalizingUnits'],
        ['NumUnits','Quantity'],
        ['PA','ProgramAdministrator'],
        ['PreDesc','PreInterventionDescription'],
        ['PrgID','ProgramID'],
        ['Qty','Quantity'],
        ['RRThm','RRTherm'],
        ['RUL_Yrs','RUL'],
        ['RateScheduleElec','ElectricRateSchedule'],
        ['RateScheduleGas','GasRateSchedule'],
        ['RealizationRateTherm','RRTherm'],
        ['RealizationRatekW','RRkW'],
        ['RealizationRatekWh','RRkWh'],
        ['Residential_Flag','ResidentialFlag'],
        ['SourceDesc','ExAnteSourceDescription'],
        ['StdDesc','StandardDescription'],
        ['TechGroup','TechnologyGroup'],
        ['TechType','TechnologyType'],
        ['UESThm','Therm1'],
        ['UESThm_ER','Therm2'],
        ['UESkW','kW1'],
        ['UESkW_ER','kW2'],
        ['UESkWh','kWh1'],
        ['UESkWh_ER','kWh2'],
        ['UnitDirectInstallLab','UnitLaborCost'],
        ['UnitDirectInstallMat','UnitMaterialsCost'],
        ['UnitMeaCost1stBaseline','UnitGrossCost1'],
        ['UnitMeaCost2ndBaseline','UnitGrossCost2'],
        ['UnitMeasureGrossCost','UnitGrossCost1'],
        ['UnitMeasureGrossCost_ER','UnitGrossCost2'],
        ['UnitTherm1stBaseline','Therm1'],
        ['UnitTherm2ndBaseline','Therm2'],
        ['UnitkW1stBaseline','kW1'],
        ['UnitkW2ndBaseline','kW2'],
        ['UnitkWh1stBaseline','kWh1'],
        ['UnitkWh2ndBaseline','kWh2'],
        ['Upstream_Flag','UpstreamFlag'],
        ['Version','DEERVersion'],
    ]
    for old_name,new_name in column_name_map:
        InputMeasures.rename_column(old_name,new_name)

    # Set values for missing columns:
    if 'ImpactType' not in InputMeasures.data.columns:
        new_column = {'ImpactType':[''] * InputMeasures.data.CET_ID.count()}
        InputMeasures.append_columns(new_column)
    if 'AnnualInflationRate' not in InputMeasures.data.columns:
        new_column = {'AnnualInflationRate':[0] * InputMeasures.data.CET_ID.count()}
        InputMeasures.append_columns(new_column)

    InputMeasures.column_map('ClimateZone',lambda s: str(s).upper())
    InputMeasures.column_map('ElectricEndUse',lambda s: s.upper())
    InputMeasures.column_map('ElectricTargetSector',lambda s: s.upper())
    InputMeasures.column_map('GasSavingsProfile',lambda s: s or '')
    InputMeasures.column_map('GasSavingsProfile',lambda s: s.upper())
    InputMeasures.column_map('GasTargetSector',lambda s: s or '')
    InputMeasures.column_map('GasTargetSector',lambda s: s.upper())

    #INCORRECT OVERWRITE MEASURE-LEVEL MARKET EFFECTS BENEFITS TO MATCH SQL:
    InputMeasures.data.MarketEffectsBenefits = market_effects_benefits

    f = lambda x: market_effects_costs if x is None else x
    InputMeasures.column_map('MarketEffectsCosts', f)

    # helper function to calculate additional columns for input measure table:
    def input_measure_calculated_columns(data_frame_row):
        year,quarter = list(map(int,data_frame_row.InstallationQuarter.split('Q')))
        quarter_index = 4 * year + quarter - 1
        rul_quarters = 4 * data_frame_row.RUL
        eul_quarters = 4 * data_frame_row.EUL
        if rul_quarters == 0:
            EUL = [data_frame_row.EUL,0]
            EULq = [eul_quarters,0]
        else:
            EUL = [data_frame_row.RUL,data_frame_row.EUL]
            EULq = [rul_quarters,eul_quarters]
        calculated_columns = {
            'Qi'    : quarter_index,
            'EUL1'  : EUL[0],
            'EUL2'  : EUL[1],
            'EULq1' : EULq[0],
            'EULq2' : EULq[1],
            'RULq'  : rul_quarters,
            'EULq'  : eul_quarters,
        }
        return calculated_columns

    # append input measures with calculated columns:
    InputMeasures.append_columns(InputMeasures.data.apply(input_measure_calculated_columns, axis='columns', result_type='expand'))

    nan_to_num_columns = ['Quantity','EUL','RUL','NTGRkW','NTGRkWh','NTGRTherm',
        'NTGRCost','IRkW','IRkWh','IRTherm','AnnualInflationRate','RRkW',
        'RRkWh','RRTherm','MarketEffectsBenefits','MarketEffectsCosts','kW1',
        'kW2','kWh1','kWh2','Therm1','Therm2','UnitGrossCost1','UnitGrossCost2',
        'UnitLaborCost','UnitMaterialsCost','UnitEndUserRebate',
        'UnitIncentiveToOthers','EULq1','EULq2','RULq','EULq']
    for column in nan_to_num_columns:
        InputMeasures.column_map(column,np.nan_to_num)

    nan_to_str_columns = [
        'ElectricTargetSector',
        'GasSavingsProfile',
        'GasTargetSector',
        'ElectricEndUse',
        'CombustionType',
    ]
    for column in nan_to_str_columns:
        InputMeasures.column_map(column,lambda x: '' if x is np.nan else x)

    return InputMeasures

def setup_avoided_cost_electric(acc_source, source_name, InputMeasures, user):
    ### parameters:
    ###     acc_source : a string, either 'csv' or 'database', indicating whether
    ###         the avoided cost electric table should be retrieved from a 
    ###         comma separated value text file or a Microsoft SQL Server
    ###         database
    ###     source_name : a string containing the file path if source is 'csv'
    ###         or the database object name if source is 'database'
    ###     InputMeasures : an instance of an 'InputMeasures' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
    ###     user : a dictionary containing items labelled 'id' and 'passwd'
    ###         which provide login credentials for the database if needed

    if acc_source == 'csv':
        if InputMeasures.source == 'database':
            if InputMeasures.table_name == 'InputMeasure':
                sql_str = '\n\tSELECT DISTINCT\n\t\tPA + \'|\' + ' \
                    'UPPER(ElecTargetSector) + \'|\' + ' \
                    'UPPER(ElecEndUseShape) + \'|\' + ' \
                    'UPPER(ClimateZone) AS LookupKey' \
                    '\n\tFROM InputMeasure\n'
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = '\n\tSELECT DISTINCT\n\t\tPA + \'|\' + ' \
                    'UPPER(E3TargetSector) + \'|\' + ' \
                    'UPPER(E3MeaElecEndUseShape) + \'|\' + ' \
                    'UPPER(E3ClimateZone) AS LookupKey' \
                    '\n\tFROM InputMeasureCEDARS\n'
            lookup_keys = list(
                EDCS_Query_Results(
                    sql_str,
                    user['id'],
                    user['passwd']
                ).data.LookupKey
            )
        else:
            lookup_keys = list(dict.fromkeys([
                '|'.join(r[1][[
                    'ProgramAdministrator',
                    'ElectricTargetSector',
                    'ElectricEndUse',
                    'ClimateZone'
                ]]) for r in InputMeasures.data.iterrows()
            ]))
        def filter_function(dataframe_chunk):
            f = lambda r: r['PA'] + '|' + r['TS'].upper() + '|' + r['EU'].upper() + '|' + str(r['CZ']) in lookup_keys
            return dataframe_chunk.apply(f,axis='columns')
        AvoidedCostElectric = Local_CSV(
            source_name,
            delimiter=',',
            filter_csv=True,
            filter_function=filter_function
        )
    else:
        if InputMeasures.source == 'database':
            # use the following query string when input measures are loaded into database:
            if InputMeasures.table_name == 'InputMeasure':
                sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)\n\t' \
                    'IN (\n\t\tSELECT PA + \'|\' + UPPER(ElecTargetSector) + ' \
                    '\'|\' + UPPER(ElecEndUseShape) + \'|\' + ' \
                    'UPPER(ClimateZone)\n\t\t' \
                    'FROM InputMeasure\n\t)\n'.format(source_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)\n\t' \
                    'IN (\n\t\tSELECT' \
                    '\n\t\t\tPA + \'|\' + UPPER(E3TargetSector) + \'|\' + ' \
                    'UPPER(E3MeaElecEndUseShape) + \'|\' + ' \
                    'UPPER(E3ClimateZone) AS LookupKey\n\t\t' \
                    'FROM InputMeasureCEDARS\n\t)\n'.format(source_name)
        else:
            # use the following query string when input measures are from a file:
            lookup_keys = ',\n\t\t'.join(
                list(
                    dict.fromkeys([
                        '\''+'|'.join(
                            r[1][[
                                'ProgramAdministrator',
                                'ElectricTargetSector',
                                'ElectricEndUse',
                                'ClimateZone'
                            ]]
                        )+'\'' for r in InputMeasures.data.iterrows()
                    ])
                )
            )
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + '\
                'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)' \
                '\n\tIN (\n\t\t{}\n\t)\n'.format(source_name,lookup_keys)

        AvoidedCostElectric = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns = [
        ['PA','ProgramAdministrator'],
        ['TS','ElectricTargetSector'],
        ['EU','ElectricEndUse'],
        ['CZ','ClimateZone'],
        ['Qtr','UsageQuarter'],
    ]
    for old_name,new_name in rename_columns:
        AvoidedCostElectric.rename_column(old_name,new_name)

    # fix column formatting:
    AvoidedCostElectric.column_map('ElectricTargetSector',lambda s: s.upper())
    AvoidedCostElectric.column_map('ElectricEndUse',lambda s: s.upper())
    AvoidedCostElectric.column_map('ClimateZone',lambda s: str(s).upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.UsageQuarter.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostElectric.append_columns(AvoidedCostElectric.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost electric data filtered by a single input measure:
    def filter_by_measure(self, measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == measure.ElectricEndUse) & \
            (self.data.ClimateZone == measure.ClimateZone) & \
            (self.data.Qi >= measure.Qi) & \
            #INCLUDE EXTRA QUARTER TO MATCH SQL:
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_source, source_name, InputMeasures, user):
    ### parameters:
    ###     acc_source : a string, either 'csv' or 'database', indicating whether
    ###         the avoided cost electric table should be retrieved from a 
    ###         comma separated value text file or a Microsoft SQL Server
    ###         database
    ###     source_name : a string containing the file path if source is 'csv'
    ###         or the database object name if source is 'database'
    ###     InputMeasures : an instance of an 'InputMeasures' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
    ###     user : a dictionary containing items labelled 'id' and 'passwd'
    ###         which provide login credentials for the database if needed

    if acc_source == 'csv':
        if InputMeasures.source == 'database':
            if InputMeasures.table_name == 'InputMeasure':
                sql_str = '\n\tSELECT DISTINCT\n\t\tPA + \'|\' + ' \
                    'UPPER(GasSector) + \'|\' + UPPER(GasSavingsProfile) ' \
                    'AS LookupKey\n\tFROM InputMeasure\n'
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = '\n\tSELECT DISTINCT\n\t\tPA + \'|\' + ' \
                    'UPPER(E3GasSector) + \'|\' + UPPER(GasSavProfile) AS ' \
                    'LookupKey\n\tFROM InputMeasureCEDARS\n'
            lookup_keys = list(
                EDCS_Query_Results(
                    sql_str,
                    user['id'],
                    user['passwd']
                ).data.LookupKey
            )
        else:
            lookup_keys = list(dict.fromkeys([
                '|'.join(r[1][[
                    'ProgramAdministrator',
                    'GasTargetSector',
                    'GasSavingsProfile',
                ]]) for r in InputMeasures.data.iterrows()
            ]))
        def filter_function(dataframe_chunk):
            f = lambda r: r['PA'] + '|' + r['GS'].upper() + '|' + r['GP'].upper() in lookup_keys
            return dataframe_chunk.apply(f, axis='columns')
        AvoidedCostGas = Local_CSV(
            source_name,
            delimiter=',',
            filter_csv=True,
            filter_function=filter_function
        )
    else:
        if InputMeasures.source == 'database':
            # use the following query string when input measures are loaded into database:
            if InputMeasures.table_name == 'InputMeasure':
                sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(GS) + \'|\' + UPPER(GP)\n\t' \
                    'IN (\n\t\tSELECT PA + UPPER(GasSector) + \'|\' + ' \
                    'UPPER(GasSavingsProfile)\n\t\t' \
                    'FROM InputMeasure\n\t)\n'.format(source_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(GS) + \'|\' + UPPER(GP)\n\t' \
                    'IN (\n\t\tSELECT PA + \'|\' + UPPER(E3GasSector) + ' \
                    '\'|\' + UPPER(E3GasSavProfile)\n\t\t' \
                    'FROM InputMeasureCEDARS\n\t)\n'.format(source_name)
        else:
            lookup_keys = ',\n\t\t'.join(
                list(
                    dict.fromkeys(
                        ['\''+'|'.join(
                            r[1][[
                                'ProgramAdministrator',
                                'GasTargetSector',
                                'GasSavingsProfile',
                            ]]
                        )+'\'' for r in InputMeasures.data.iterrows()]
                    )
                )
            )
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(GS) + \'|\' + UPPER(GP)' \
                '\n\tIN (\n\t\t{}\n\t)\n'.format(source_name,lookup_keys)
        AvoidedCostGas = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns = [
        ['PA','ProgramAdministrator'],
        ['GS','GasTargetSector'],
        ['GP','GasSavingsProfile'],
        ['Qtr','UsageQuarter'],
        ['Total','Cost'],
    ]
    for old_name,new_name in rename_columns:
        AvoidedCostGas.rename_column(old_name,new_name)

    # fix column formatting:
    AvoidedCostGas.column_map('GasTargetSector',lambda s: s.upper())
    AvoidedCostGas.column_map('GasSavingsProfile',lambda s: s.upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.UsageQuarter.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostGas.append_columns(AvoidedCostGas.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost gas data filtered by a single input measure:
    def filter_by_measure(self, measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == measure.GasSavingsProfile) & \
            (self.data.Qi >= measure.Qi) & \
            #INCLUDE EXTRA QUARTER TO MATCH SQL:
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostGas)

    return AvoidedCostGas
