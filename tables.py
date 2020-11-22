import numpy as np
import types
from models import EDCS_Connection,EDCS_Table,EDCS_Query_Results,Local_CSV

def setup_input_measures(source, source_name, first_year, market_effects_benefits, market_effects_costs, user={}):
    if source == 'csv':
        InputMeasures = Local_CSV(source_name, delimiter=',')
    elif source == 'database':
        InputMeasures = EDCS_Table(source_name,user['id'],user['passwd'])
    else:
        InputMeasures = EDCS_Table('InputMeasureCEDARS',user['id'],user['passwd'])

    # fix input measure column name and type issues:
    if source == 'csv':
        if source_name.find('CEDARS') > 0:
            column_name_map = [
                ['CEInputID','CET_ID'],
                ['PrgID','ProgramID'],
                ['PA','ProgramAdministrator'],
                ['MeasDescription','MeasureName'],
                ['MeasImpactType','MeasureImpactType'],
                ['E3TargetSector','ElectricTargetSector'],
                ['E3MeaElecEndUseShape','ElectricEndUse'],
                ['E3GasSavProfile','GasSavingsProfile'],
                ['E3GasSector','GasTargetSector'],
                ['E3ClimateZone','ClimateZone'],
                ['RateScheduleElec','ElectricRateSchedule'],
                ['RateScheduleGas','GasRateSchedule'],
                ['ClaimYearQuarter','InstallationQuarter'],
                ['NumUnits','Quantity'],
                ['UnitkW1stBaseline','kW1'],
                ['UnitkW2ndBaseline','kW2'],
                ['UnitkWh1stBaseline','kWh1'],
                ['UnitkWh2ndBaseline','kWh2'],
                ['UnitTherm1stBaseline','Therm1'],
                ['UnitTherm2ndBaseline','Therm2'],
                ['EUL_Yrs','EUL'],
                ['RUL_Yrs','RUL'],
                ['InstallationRatekW','IRkW'],
                ['InstallationRatekWh','IRkWh'],
                ['InstallationRateTherm','IRTherm'],
                ['RealizationRatekW','RRkW'],
                ['RealizationRatekWh','RRkWh'],
                ['RealizationRateTherm','RRTherm'],
                ['MeasInflation','AnnualInflationRate'],
                ['UnitMeaCost1stBaseline','UnitGrossCost1'],
                ['UnitMeaCost2ndBaseline','UnitGrossCost2'],
                ['UnitDirectInstallLab','UnitLaborCost'],
                ['UnitDirectInstallMat','UnitMaterialsCost'],
            ]
        else:
            column_name_map = [
                ['CEInputID','CET_ID'],
                ['PrgID','ProgramID'],
                ['ClaimYearQuarter','InstallationQuarter'],
                ['Sector','ElectricTargetSector'],
                ['BldgType','BuildingType'],
                ['E3ClimateZone','ClimateZone'],
                ['E3GasSavProfile','GasSavingsProfile'],
                ['E3GasSector','GasTargetSector'],
                ['E3MeaElecEndUseShape','ElectricEndUse'],
                ['MeasAppType','MeasureApplicationType'],
                ['MeasCode','MeasureCode'],
                ['TechGroup','TechnologyGroup'],
                ['TechType','TechnologyType'],
                ['SourceDesc','SourceDescription'],
                ['NumUnits','Quantity'],
                ['UnitkW1stBaseline','kW1'],
                ['UnitkWh1stBaseline','kWh1'],
                ['UnitTherm1stBaseline','Therm1'],
                ['UnitkW2ndBaseline','kW2'],
                ['UnitkWh2ndBaseline','kWh2'],
                ['UnitTherm2ndBaseline','Therm2'],
                ['EUL_Yrs','EUL'],
                ['RUL_Yrs','RUL'],
                ['RealizationRatekW','RRkW'],
                ['RealizationRatekWh','RRkWh'],
                ['RealizationRateTherm','RRTherm'],
                ['InstallationRatekW','IRkW'],
                ['InstallationRatekWh','IRkWh'],
                ['InstallationRateTherm','IRTherm'],
                ['UnitMeaCost1stBaseline','UnitGrossCost1'],
                ['UnitMeaCost2ndBaseline','UnitGrossCost2'],
                ['UnitDirectInstallLab','UnitLaborCost'],
                ['UnitDirectInstallMat','UnitMaterialsCost'],
                ['PA','ProgramAdministrator'],
                ['MeasInflation','AnnualInflationRate'],
            ]
        for old_name,new_name in column_name_map:
            InputMeasures.rename_column(old_name,new_name)

        nan_columns = [
            'ElectricTargetSector',
            'GasSavingsProfile',
            'GasTargetSector',
            'ElectricEndUse',
            'CombustionType',
        ]
        for nan_column in nan_columns:
            InputMeasures.column_map(nan_column,lambda x: '' if x is np.nan else x)

    # fix input measure column name and type issues:
    else:
        if source_name == 'InputMeasure':
            column_name_map = [
                ['PrgID','ProgramID'],
                ['PA','ProgramAdministrator'],
                ['ElecTargetSector','ElectricTargetSector'],
                ['GasSector','GasTargetSector'],
                ['ElecEndUseShape','ElectricEndUse'],
                ['ClimateZone','ClimateZone'],
                ['ElecRateSchedule','ElectricRateSchedule'],
                ['ClaimYearQuarter','InstallationQuarter'],
                ['Qty','Quantity'],
                ['UESkW','kW1'],
                ['UESkW_ER','kW2'],
                ['UESkWh','kWh1'],
                ['UESkWh_ER','kWh2'],
                ['UESThm','Therm1'],
                ['UESThm_ER','Therm2'],
                ['IRThm','IRTherm'],
                ['RRThm','RRTherm'],
                ['NTGRThm','NTGRTherm'],
                ['MarketEffectBens','MarketEffectsBenefits'],
                ['MarketEffectCost','MarketEffectsCosts'],
                ['UnitMeasureGrossCost','UnitGrossCost1'],
                ['UnitMeasureGrossCost_ER','UnitGrossCost2'],
                ['EndUserRebate','UnitEndUserRebate'],
                ['IncentiveToOthers','UnitIncentiveToOthers'],
                ['DILaborCost','UnitLaborCost'],
                ['DIMaterialCost','UnitMaterialsCost'],
                ['UnitMeaCost1stBaseline','UnitGrossCost1'],
                ['UnitMeaCost2ndBaseline','UnitGrossCost2'],
                ['UnitDirectInstallLab','UnitLaborCost'],
                ['UnitDirectInstallMat','UnitMaterialsCost'],
            ]
            for old_name,new_name in column_name_map:
                InputMeasures.rename_column(old_name,new_name)
            new_columns = pd.DataFrame({
                'ImpactType'          : [''] * InputMeasures.data.CET_ID.count(),
                'AnnualInflationRate' : [0] * InputMeasures.data.CET_ID.count(),
            })
            InputMeasures.append_columns(new_columns)

        elif source_name.find('CEDARS') > 0:
            column_name_map = [
                ['CEInputID','CET_ID'],
                ['PrgID','ProgramID'],
                ['PA','ProgramAdministrator'],
                ['MeasDescription','MeasureName'],
                ['MeasImpactType','MeasureImpactType'],
                ['E3TargetSector','ElectricTargetSector'],
                ['E3MeaElecEndUseShape','ElectricEndUse'],
                ['E3GasSavProfile','GasSavingsProfile'],
                ['E3GasSector','GasTargetSector'],
                ['E3ClimateZone','ClimateZone'],
                ['RateScheduleElec','ElectricRateSchedule'],
                ['RateScheduleGas','GasRateSchedule'],
                ['ClaimYearQuarter','InstallationQuarter'],
                ['NumUnits','Quantity'],
                ['UnitkW1stBaseline','kW1'],
                ['UnitkW2ndBaseline','kW2'],
                ['UnitkWh1stBaseline','kWh1'],
                ['UnitkWh2ndBaseline','kWh2'],
                ['UnitTherm1stBaseline','Therm1'],
                ['UnitTherm2ndBaseline','Therm2'],
                ['EUL_Yrs','EUL'],
                ['RUL_Yrs','RUL'],
                ['InstallationRatekW','IRkW'],
                ['InstallationRatekWh','IRkWh'],
                ['InstallationRateTherm','IRTherm'],
                ['RealizationRatekW','RRkW'],
                ['RealizationRatekWh','RRkWh'],
                ['RealizationRateTherm','RRTherm'],
                ['MeasInflation','AnnualInflationRate'],
                ['UnitMeaCost1stBaseline','UnitGrossCost1'],
                ['UnitMeaCost2ndBaseline','UnitGrossCost2'],
                ['UnitDirectInstallLab','UnitLaborCost'],
                ['UnitDirectInstallMat','UnitMaterialsCost'],
            ]
            for old_name,new_name in column_name_map:
                InputMeasures.rename_column(old_name,new_name)

    InputMeasures.column_map('ElectricTargetSector',lambda s: s.upper())
    InputMeasures.column_map('GasTargetSector',lambda s: s or '')
    InputMeasures.column_map('GasTargetSector',lambda s: s.upper())
    InputMeasures.column_map('GasSavingsProfile',lambda s: s or '')
    InputMeasures.column_map('GasSavingsProfile',lambda s: s.upper())
    InputMeasures.column_map('ElectricEndUse',lambda s: s.upper())
    InputMeasures.column_map('ClimateZone',lambda s: str(s).upper())

    # helper functions to replace missing measure-level market effects with default values:
    def set_market_effects_benefits(value):
        if value is None:
            return market_effects_benefits
        else:
            return value
    InputMeasures.column_map('MarketEffectsBenefits',set_market_effects_benefits)

    def set_market_effects_costs(value):
        if value is None:
            return market_effects_costs
        else:
            return value
    InputMeasures.column_map('MarketEffectsCosts',set_market_effects_costs)

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

    nan_columns = ['Quantity','EUL','RUL','NTGRkW','NTGRkWh','NTGRTherm',
        'NTGRCost','IRkW','IRkWh','IRTherm','AnnualInflationRate','RRkW',
        'RRkWh','RRTherm','MarketEffectsBenefits','MarketEffectsCosts','kW1',
        'kW2','kWh1','kWh2','Therm1','Therm2','UnitGrossCost1','UnitGrossCost2',
        'UnitLaborCost','UnitMaterialsCost','UnitEndUserRebate',
        'UnitIncentiveToOthers','EULq1','EULq2','RULq','EULq']
    for nan_column in nan_columns:
        InputMeasures.column_map(nan_column,np.nan_to_num)

    return InputMeasures

def setup_input_programs(source,source_name,user={}):
    if source == 'csv':
        InputPrograms = Local_CSV(source_name, delimiter=',')
    elif source == 'database':
        InputPrograms = EDCS_Table(source_name,user['id'],user['passwd'])
    else:
        InputPrograms = EDCS_Table('InputProgramCEDARS',user['id'],user['passwd'])

    # fix input program column names:
    if source == 'csv':
        column_name_map = [
            ['PrgID','ProgramID'],
            ['PrgYear','ProgramYear'],
            ['ClaimYearQuarter','InstallationQuarter'],
            ['PA','ProgramAdministrator'],
        ]
    else:
        column_name_map = [
            ['PrgID','ProgramID'],
            ['PA','ProgramAdministrator'],
            ['ClaimYearQuarter','InstallationQuarter'],
        ]
    for old_name,new_name in column_name_map:
        InputPrograms.rename_column(old_name,new_name)

    # add column with universal quarter index:
    def input_program_calculated_columns(data_frame_row):
        year,quarter = list(map(int,data_frame_row.InstallationQuarter.split('Q')))
        quarter_index = 4 * year + quarter - 1
        calculated_columns = {
            'Qi' : quarter_index,
        }
        return calculated_columns

    InputPrograms.append_columns(InputPrograms.data.apply(input_program_calculated_columns, axis='columns', result_type='expand'))

    # add method to get input program data filtered by a single input measure:
    def filter_by_measure(self, measure):
        filtered_input_programs = self.data.get(
            (self.data.ProgramID == measure.ProgramID)
        )
        return filtered_input_programs
    InputPrograms.filter_by_measure = \
        types.MethodType(filter_by_measure,InputPrograms)

    return InputPrograms

def setup_settings(source, source_name, avoided_cost_calculator_version, InputMeasures, user={}):
    if source == 'csv':
        Settings = Local_CSV(source_name, delimiter=',')
    else:
        sql_str = 'SELECT * FROM E3Settings WHERE Version={}'.format(avoided_cost_calculator_version)
        Settings = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map = [
        ['PA','ProgramAdministrator'],
    ]
    for old_name,new_name in column_name_map:
        Settings.rename_column(old_name,new_name)

    Settings.column_map('ProgramAdministrator',lambda s: s.strip())

    # add method to get settings data filtered by a single input measure:
    def filter_by_measure(self, measure):
        filtered_settings = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator)
        )
        return filtered_settings
    Settings.filter_by_measure = types.MethodType(filter_by_measure,Settings)

    return Settings

def setup_emissions(source, source_name, avoided_cost_calculator_version, InputMeasures, user={}):
    if source=='csv':
        Emissions = Local_CSV(source_name, ',')
    else:
        if InputMeasures.source == 'database':
            if InputMeasures.table_name == 'InputMeasure':
                sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)' \
                    '\n\tIN (\n\t\tSELECT PA + \'|\' + UPPER(ElecTargetSector) + ' \
                    '\'|\' + UPPER(ElecEndUseShape) + \'|\' + UPPER(ClimateZone) ' \
                    'AS LookupKey\n\t\tFROM InputMeasure\n\t)\n\tAND Version={}' \
                    '\n'.format(source_name, avoided_cost_calculator_version)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)' \
                    '\n\tIN (\n\t\tSELECT PA + \'|\' + UPPER(E3TargetSector) + ' \
                    '\'|\' + UPPER(E3MeaElecEndUseShape) + \'|\' + ' \
                    'UPPER(E3ClimateZone) AS LookupKey' \
                    '\n\t\tFROM InputMeasureCEDARS\n\t)\n\tAND Version={}' \
                    '\n'.format(source_name, avoided_cost_calculator_version)
        else:
            lookup_keys = ',\n\t\t'.join(list(dict.fromkeys([
                '\''+'|'.join(r[1][[
                    'ProgramAdministrator',
                    'ElectricTargetSector',
                    'ElectricEndUse',
                    'ClimateZone'
                ]])+'\'' for r in InputMeasures.data.iterrows()
            ])))
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ' \
                '\n\tIN (\n\t\t{}\n\t)\n\tAND Version={}' \
                '\n'.format(source_name,lookup_keys,avoided_cost_calculator_version)

        Emissions = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map = [
        ['PA','ProgramAdministrator'],
        ['TS','ElectricTargetSector'],
        ['EU','ElectricEndUse'],
        ['CZ','ClimateZone'],
    ]

    for old_name,new_name in column_name_map:
        Emissions.rename_column(old_name,new_name)

    # fix column formatting:
    Emissions.column_map('ElectricTargetSector',lambda s: s.upper())
    Emissions.column_map('ElectricEndUse',lambda s: s.upper())
    Emissions.column_map('ClimateZone',lambda s: s.upper())

    # add method to get emissions data filtered by single input measure:
    def filter_by_measure(self, measure):
        filtered_emissions = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == measure.ElectricEndUse) & \
            (self.data.ClimateZone == measure.ClimateZone) \
        )
        return filtered_emissions
    Emissions.filter_by_measure = \
        types.MethodType(filter_by_measure,Emissions)

    return Emissions

def setup_combustion_types(source, source_name, InputMeasures, user={}):
    if source == 'csv':
        CombustionTypes = Local_CSV(source_name, delimiter=',')
    else:
        if InputMeasures.source == 'database':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE LookupCode IN' \
                '\n\t\t( SELECT CombustionType FROM {} )'.format(
                    source_name,
                    InputMeasures.table_name
                )
        else:
            lookup_keys = ',\n\t\t'.join(list(dict.fromkeys(
                ['\'' + r[1].CombustionType + '\'' for r in InputMeasures.data.iterrows()]
            )))
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE LookupCode IN' \
                '\n\t\t( {} )'.format(source_name,lookup_keys)

        CombustionTypes = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    def filter_by_measure(self, measure):
        filtered_combustion_type = self.data.get(
            self.data.LookupCode == measure.CombustionType
        )
        return filtered_combustion_type

    CombustionTypes.filter_by_measure = \
        types.MethodType(filter_by_measure,CombustionTypes)

    return CombustionTypes

def setup_avoided_cost_electric(acc_source, source_name, InputMeasures, user={}):
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
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)' \
                    '\n\tIN (\n\t\tSELECT DISTINCT PA + \'|\' + ' \
                    'UPPER(ElecTargetSector) + \'|\' + ' \
                    'UPPER(ElecEndUseShape) + \'|\' + UPPER(ClimateZone) ' \
                    'AS LookupKey\n\t\t' \
                    'FROM InputMeasure\n\t)\n'.format(source_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)' \
                    '\n\tIN (\n\t\tSELECT DISTINCT PA + \'|\' + ' \
                    'UPPER(E3TargetSector) + \'|\' + ' \
                    'UPPER(E3MeaElecEndUseShape) + \'|\' + ' \
                    'UPPER(E3ClimateZone) AS LookupKey\n\t\t' \
                    'FROM InputMeasureCEDARS\n\t)\n'.format(source_name)
            else:
                sql_str = 'SELECT * FROM {}'.format(source_name)
        else:
            # use the following query string when input measures are from a file:
            lookup_keys = ',\n\t\t'.join(list(dict.fromkeys([
                '\''+'|'.join(r[1][[
                    'ProgramAdministrator',
                    'ElectricTargetSector',
                    'ElectricEndUse',
                    'ClimateZone'
                ]])+'\'' for r in InputMeasures.data.iterrows()
            ])))
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ' \
                '\n\tIN (\n\t\t{}\n\t)\n'.format(source_name,lookup_keys)
        AvoidedCostElectric = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map = [
        ['PA','ProgramAdministrator'],
        ['TS','ElectricTargetSector'],
        ['EU','ElectricEndUse'],
        ['CZ','ClimateZone'],
        ['Qtr','ApplicableQuarter'],
    ]
    for old_name,new_name in column_name_map:
        AvoidedCostElectric.rename_column(old_name,new_name)

    # standardize column formatting:
    AvoidedCostElectric.column_map('ElectricTargetSector',lambda s: s.upper())
    AvoidedCostElectric.column_map('ElectricEndUse',lambda s: s.upper())
    AvoidedCostElectric.column_map('ClimateZone',lambda s: str(s).upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.ApplicableQuarter.split('Q')))
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
            (self.data.Qi < measure.Qi + measure.EULq)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_source, source_name, InputMeasures, user={}):
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
                sql_str = 'SELECT *\n\tFROM {}\n\tWHERE '\
                    'PA + \'|\' + UPPER(GS) + \'|\' + UPPER(GP)\n\tIN (' \
                    '\n\t\tSELECT DISTINCT PA + \'|\' + UPPER(GasSector) + ' \
                    '\'|\' + UPPER(GasSavingsProfile)\n\t\t' \
                    'FROM InputMeasure\n\t)\n'.format(source_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                    'UPPER(GS) + \'|\' + UPPER(GP)\n\tIN (\n\t\t' \
                    'SELECT DISTINCT PA + \'|\' + UPPER(E3GasSector) + ' \
                    '\'|\' + UPPER(E3GasSavProfile)\n\t\t' \
                    'FROM InputMeasureCEDARS\n\t)\n'.format(source_name)
            else:
                sql_str = 'SELECT * FROM {}'.format(source_name)
        else:
            lookup_keys = ',\n\t\t'.join(list(dict.fromkeys([
                '\''+'|'.join(r[1][[
                    'ProgramAdministrator',
                    'GasTargetSector',
                    'GasSavingsProfile',
                ]])+'\'' for r in InputMeasures.data.iterrows()
            ])))
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + UPPER(GS) + ' \
                '\'|\' + UPPER(GP)\n\tIN ' \
                '(\n\t\t{}\n\t)\n'.format(source_name,lookup_keys)
        AvoidedCostGas = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map = [
        ['PA','ProgramAdministrator'],
        ['GS','GasTargetSector'],
        ['GP','GasSavingsProfile'],
        ['Qtr','ApplicableQuarter'],
        ['Total','Cost'],
    ]
    for old_name,new_name in column_name_map:
        AvoidedCostGas.rename_column(old_name,new_name)

    # fix column formatting:
    AvoidedCostGas.column_map('GasTargetSector',lambda s: s.upper())
    AvoidedCostGas.column_map('GasSavingsProfile',lambda s: s.upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.ApplicableQuarter.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostGas.append_columns(AvoidedCostGas.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost gas data filtered by a single input measure:
    def filter_by_measure(self, measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == measure.GasSavingsProfile) & \
            (self.data.Qi >= measure.Qi) & \
            (self.data.Qi < measure.Qi + measure.EULq)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostGas)

    return AvoidedCostGas

def setup_rate_schedule_electric(source, source_name, rate_schedule_version, InputMeasures, user={}):
    if source == 'csv':
        RateScheduleElectric = Local_CSV(source_name, delimiter=',')

        mapping_source_name = source_name.split('.csv')[0] + 'Mapping.csv'
        RateScheduleElectricMapping = Local_CSV(mapping_source_name, delimiter=',')
        
        RateScheduleElectric.data = \
            RateScheduleElectric.data[[
                'PA',
                'Version',
                'Schedule',
                'Year',
                'RateE'
            ]].merge(
                RateScheduleElectricMapping.data[[
                    'PA',
                    'Version',
                    'Schedule',
                    'TargetSector'
                ]],
                on=['PA','Version','Schedule']
            )
    else:
        if InputMeasures.source == 'database':
            if InputMeasures.table_name == 'InputMeasure':
                rate_schedule_metadata = '\n\tSELECT DISTINCT\n\t\tCASE' + \
                    '\n\t\t\tWHEN PA = \'PGE\' OR PA = \'SDGE\'' + \
                    '\n\t\t\tTHEN PA + \'|\' + UPPER(ElecTargetSector)' + \
                    '\n\t\t\tELSE PA + \'|ALL\'' + \
                    '\n\t\tEND AS LookupKey' + \
                    '\n\tFROM {}'
                rate_schedule_metadata = \
                        rate_schedule_metadata.format(InputMeasures.table_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                rate_schedule_metadata = '\n\tSELECT DISTINCT\n\t\tCASE' + \
                    '\n\t\t\tWHEN PA = \'PGE\' OR PA = \'SDGE\'' + \
                    '\n\t\t\tTHEN PA + \'|\' + UPPER(E3TargetSector)' + \
                    '\n\t\t\tELSE PA + \'|ALL\'' + \
                    '\n\t\tEND AS LookupKey' + \
                    '\n\tFROM {}'
                rate_schedule_metadata = \
                    rate_schedule_metadata.format(InputMeasures.table_name)
        else:
            def get_lookup_key(r):
                if r.ProgramAdministrator == 'PGE' or r.ProgramAdministrator == 'SDGE':
                    lookup_key = '\'{}|{}\''.format(r.ProgramAdministrator,r.ElectricTargetSector) 
                else:
                    lookup_key = '\'{}|ALL\''.format(r.ProgramAdministrator)
                return lookup_key
            rate_schedule_metadata = \
                ','.join(list(dict.fromkeys(
                    InputMeasures.data.apply(get_lookup_key,axis='columns')
                )))
        sql_str = '\nSELECT' \
            '\n\tRates.PA,\n\tRates.Version,\n\tRates.Schedule,' \
            '\n\tMap.TargetSector,\n\tRates.Year,\n\tRates.RateE' \
            '\nFROM {} AS Rates\nLEFT JOIN {} AS Map' \
            '\n\tON Rates.PA = Map.PA\n\tAND Rates.Version = Map.Version' \
            '\n\tAND Rates.Schedule = Map.Schedule' \
            '\nWHERE\n\tRates.PA + \'|\' + UPPER(Map.TargetSector) IN ({}\n\t)' \
            '\nAND Rates.Version={}'.format(
                source_name,
                source_name + 'Mapping',
                rate_schedule_metadata,
                rate_schedule_version
            )
        RateScheduleElectric = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map= [
        ['PA','ProgramAdministrator'],
        ['Schedule','CustomerType'],
        ['TargetSector','ElectricTargetSector'],
        ['Year','ApplicableYear'],
        ['RateE','ElectricRate'],
    ]
    for old_name,new_name in column_name_map:
            RateScheduleElectric.rename_column(old_name,new_name)

    # fix column formatting:
    RateScheduleElectric.column_map('ApplicableYear',int)
    RateScheduleElectric.column_map('CustomerType',lambda s: s.upper())
    RateScheduleElectric.column_map('ElectricTargetSector',lambda s: s.upper())

    def filter_by_measure(self, measure):
        filtered_electric_rates = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ApplicableYear * 4 >= measure.Qi) & \
            (self.data.ApplicableYear * 4 < measure.Qi + measure.EULq) & \
            (
                (self.data.ElectricTargetSector == measure.ElectricTargetSector) | \
                (self.data.ElectricTargetSector == 'ALL')
            )
        )
        return filtered_electric_rates
    RateScheduleElectric.filter_by_measure = \
        types.MethodType(filter_by_measure,RateScheduleElectric)

    return RateScheduleElectric

def setup_rate_schedule_gas(source, source_name, rate_schedule_version, InputMeasures, user={}):
    if source == 'csv':
        RateScheduleGas = Local_CSV(source_name, delimiter=',')

        mapping_source_name = source_name.split('.')[0] + 'Mapping.csv'
        RateScheduleGasMapping = Local_CSV(mapping_source_name, delimiter=',')
        RateScheduleGasMapping.rename_column('GasRateSchedule','Schedule')

        RateScheduleGas.data = \
            RateScheduleGas.data[[
                'PA',
                'Version',
                'Schedule',
                'Year',
                'RateG'
            ]].merge(
                RateScheduleGasMapping.data[[
                    'PA',
                    'Version',
                    'Schedule',
                    'GasSector'
                ]],
                on = ['PA','Version','Schedule']
            )
    else:
        if InputMeasures.source == 'database':
            if InputMeasures.table_name == 'InputMeasure':
                rate_schedule_metadata = '\n\tSELECT DISTINCT' \
                    '\n\t\tPA + \'|\' + ' \
                    'UPPER(COALESCE(GasSector,ElecTargetSector) AS LookupKey' \
                    '\n\tFROM {}'.format(InputMeasures.table_name)
            elif InputMeasures.table_name == 'InputMeasureCEDARS':
                rate_schedule_metadata = '\n\t\tSELECT DISTINCT' \
                    '\n\t\t\tPA + \'|\' + ' \
                    'UPPER(COALESCE(E3GasSector,E3TargetSector)) AS LookupKey' \
                    '\n\t\tFROM {}'.format(InputMeasures.table_name)
        else:
            def get_lookup_key(r):
                lookup_key = '\'{}|{}\''.format(r.ProgramAdministrator,r.GasTargetSector) 
                return lookup_key
            rate_schedule_metadata = ','.join(list(dict.fromkeys(
                InputMeasures.data.apply(get_lookup_key,axis='columns')
            )))
        sql_str = '\nSELECT' \
            '\n\tRates.PA,\n\tRates.Version,\n\tRates.Schedule,' \
            '\n\tMap.GasSector,\n\tRates.Year,\n\tRates.RateG' \
            '\nFROM {} AS Rates\nLEFT JOIN {} AS Map' \
            '\n\tON Rates.PA = Map.PA\n\tAND Rates.Version = Map.Version' \
            '\n\tAND Rates.Schedule = Map.GasRateSchedule' \
            '\nWHERE\n\tRates.PA + \'|\' + ' \
            'UPPER(REPLACE(Map.GasSector,CHAR(13)+CHAR(10),\'\')) IN ({}\n\t)' \
            '\nAND Rates.Version={}'.format(
                source_name,
                source_name + 'Mapping',
                rate_schedule_metadata,
                rate_schedule_version
            )
        RateScheduleGas = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    column_name_map= [
        ['PA','ProgramAdministrator'],
        ['Schedule','CustomerType'],
        ['GasSector','GasTargetSector'],
        ['Year','ApplicableYear'],
        ['RateG','GasRate'],
    ]
    for old_name,new_name in column_name_map:
            RateScheduleGas.rename_column(old_name,new_name)

    # fix column formatting:
    RateScheduleGas.column_map('ApplicableYear',int)
    RateScheduleGas.column_map('CustomerType',lambda s: s.upper())
    RateScheduleGas.column_map('GasTargetSector',lambda s: s.strip().upper())

    def filter_by_measure(self, measure):
        filtered_gas_rates = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ApplicableYear * 4 >= measure.Qi) & \
            (self.data.ApplicableYear * 4 < measure.Qi + measure.EULq) & \
            (self.data.GasTargetSector == measure.GasTargetSector)
        )
        return filtered_gas_rates
    RateScheduleGas.filter_by_measure = \
        types.MethodType(filter_by_measure,RateScheduleGas)

    return RateScheduleGas

def setup_output_measures():
    OutputMeasures = Local_CSV('OutputMeasures.csv',delimiter=',',fetch_init=False)
    OutputMeasures.set_table_cols([
       'CET_ID', 'ElectricBenefitsGross', 'ElectricBenefitsNet',
       'GasBenefitsGross', 'GasBenefitsNet', 'CO2GrossElectricFirstYear',
       'CO2GrossGasFirstYear', 'CO2GrossFirstYear',
       'CO2GrossElectricLifecycle', 'CO2GrossGasLifecycle',
       'CO2GrossLifecycle', 'CO2NetElectricFirstYear', 'CO2NetGasFirstYear',
       'CO2NetFirstYear', 'CO2NetElectricLifecycle', 'CO2NetGasLifecycle',
       'CO2NetLifecycle', 'NOxGrossElectricFirstYear', 'NOxGrossGasFirstYear',
       'NOxGrossFirstYear', 'NOxGrossElectricLifecycle',
       'NOxGrossGasLifecycle', 'NOxGrossLifecycle', 'NOxNetElectricFirstYear',
       'NOxNetGasFirstYear', 'NOxNetFirstYear', 'NOxNetElectricLifecycle',
       'NOxNetGasLifecycle', 'NOxNetLifecycle', 'PM10GrossFirstYear',
       'PM10GrossLifecycle', 'PM10NetFirstYear', 'PM10NetLifecycle',
       'TotalResourceCostGross', 'TotalResourceCostGrossNoAdmin',
       'TotalResourceCostNet', 'TotalResourceCostNetNoAdmin',
       'TotalResourceCostRatio', 'TotalResourceCostRatioNoAdmin',
       'ProgramAdministratorCost', 'ProgramAdministratorCostNoAdmin',
       'ProgramAdministratorCostRatio', 'ProgramAdministratorCostRatioNoAdmin',
       'BillReductionElectric', 'BillReductionGas',
       'RatepayerImpactMeasureCost'
    ])

    return OutputMeasures

def setup_output_programs():
    OutputPrograms = \
        Local_CSV('OutputProgram.csv',delimiter=',',fetch_init=False)
    OutputPrograms.set_table_cols([
       'ProgramID', 'ElectricBenefitsGross', 'ElectricBenefitsNet',
       'GasBenefitsGross', 'GasBenefitsNet', 'CO2GrossElectricFirstYear',
       'CO2GrossGasFirstYear', 'CO2GrossFirstYear',
       'CO2GrossElectricLifecycle', 'CO2GrossGasLifecycle',
       'CO2GrossLifecycle', 'CO2NetElectricFirstYear', 'CO2NetGasFirstYear',
       'CO2NetFirstYear', 'CO2NetElectricLifecycle', 'CO2NetGasLifecycle',
       'CO2NetLifecycle', 'NOxGrossElectricFirstYear', 'NOxGrossGasFirstYear',
       'NOxGrossFirstYear', 'NOxGrossElectricLifecycle',
       'NOxGrossGasLifecycle', 'NOxGrossLifecycle', 'NOxNetElectricFirstYear',
       'NOxNetGasFirstYear', 'NOxNetFirstYear', 'NOxNetElectricLifecycle',
       'NOxNetGasLifecycle', 'NOxNetLifecycle', 'PM10GrossFirstYear',
       'PM10GrossLifecycle', 'PM10NetFirstYear', 'PM10NetLifecycle',
       'TotalResourceCostGross', 'TotalResourceCostGrossNoAdmin',
       'TotalResourceCostNet', 'TotalResourceCostNetNoAdmin',
       'TotalResourceCostRatio', 'TotalResourceCostRatioNoAdmin',
       'ProgramAdministratorCost', 'ProgramAdministratorCostNoAdmin',
       'ProgramAdministratorCostRatio', 'ProgramAdministratorCostRatioNoAdmin',
       'BillReductionElectric', 'BillReductionGas',
       'RatepayerImpactMeasureCost'
    ])

    return OutputPrograms

def setup_output_portfolio():
    OutputPortfolio = \
        Local_CSV('OutputPortfolio.csv',delimiter=',',fetch_init=False)
    OutputPortfolio.set_table_cols([
       'ElectricBenefitsGross', 'ElectricBenefitsNet',
       'GasBenefitsGross', 'GasBenefitsNet', 'CO2GrossElectricFirstYear',
       'CO2GrossGasFirstYear', 'CO2GrossFirstYear',
       'CO2GrossElectricLifecycle', 'CO2GrossGasLifecycle',
       'CO2GrossLifecycle', 'CO2NetElectricFirstYear', 'CO2NetGasFirstYear',
       'CO2NetFirstYear', 'CO2NetElectricLifecycle', 'CO2NetGasLifecycle',
       'CO2NetLifecycle', 'NOxGrossElectricFirstYear', 'NOxGrossGasFirstYear',
       'NOxGrossFirstYear', 'NOxGrossElectricLifecycle',
       'NOxGrossGasLifecycle', 'NOxGrossLifecycle', 'NOxNetElectricFirstYear',
       'NOxNetGasFirstYear', 'NOxNetFirstYear', 'NOxNetElectricLifecycle',
       'NOxNetGasLifecycle', 'NOxNetLifecycle', 'PM10GrossFirstYear',
       'PM10GrossLifecycle', 'PM10NetFirstYear', 'PM10NetLifecycle',
       'TotalResourceCostGross', 'TotalResourceCostGrossNoAdmin',
       'TotalResourceCostNet', 'TotalResourceCostNetNoAdmin',
       'TotalResourceCostRatio', 'TotalResourceCostRatioNoAdmin',
       'ProgramAdministratorCost', 'ProgramAdministratorCostNoAdmin',
       'ProgramAdministratorCostRatio', 'ProgramAdministratorCostRatioNoAdmin',
       'BillReductionElectric', 'BillReductionGas',
       'RatepayerImpactMeasureCost'
    ])

    return OutputPortfolio
