import numpy as np
import types
from models import EDCS_Connection,EDCS_Table,EDCS_Query_Results

def setup_input_measures(first_year, user):
    InputMeasures = EDCS_Table('InputMeasureCEDARS',user['id'],user['passwd'])

    # indicate source of table from database:
    InputMeasures.source = 'database'

    # fix input measure column name and type issues:
    if InputMeasures.table_name == 'InputMeasure':
        rename_columns = [
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
        ]
        for old_name,new_name in rename_columns:
            InputMeasures.rename_column(old_name,new_name)
        new_columns = pd.DataFrame({
            'ImpactType'          : [''] * InputMeasures.data.CET_ID.count(),
            'AnnualInflationRate' : [0] * InputMeasures.data.CET_ID.count(),
        })
        InputMeasures.append_columns(new_columns)

    if InputMeasures.table_name == 'InputMeasureCEDARS':
        rename_columns = [
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
        for old_name,new_name in rename_columns:
            InputMeasures.rename_column(old_name,new_name)

    InputMeasures.column_map('CET_ID',lambda s: int(s))
    InputMeasures.column_map('ElectricTargetSector',lambda s: s.upper())
    InputMeasures.column_map('GasTargetSector',lambda s: s or '')
    InputMeasures.column_map('GasTargetSector',lambda s: s.upper())
    InputMeasures.column_map('GasSavingsProfile',lambda s: s or '')
    InputMeasures.column_map('GasSavingsProfile',lambda s: s.upper())
    InputMeasures.column_map('ElectricEndUse',lambda s: s.upper())
    InputMeasures.column_map('ClimateZone',lambda s: s.upper())

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

def setup_input_programs(user):
    InputPrograms = EDCS_Table('InputProgramCEDARS',user['id'],user['passwd'])

    rename_columns = [
        ['PrgID','ProgramID'],
        ['PA','ProgramAdministrator'],
        ['ClaimYearQuarter','InstallationQuarter'],
    ]
    for old_name,new_name in rename_columns:
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
    def filter_by_measure(self, input_measure):
        filtered_input_programs = self.data.get(
            (self.data.ProgramID == input_measure.ProgramID)
        )
        return filtered_input_programs
    InputPrograms.filter_by_measure = types.MethodType(filter_by_measure,InputPrograms)

    return InputPrograms

def setup_settings(acc_version, InputMeasures, user):
    sql_str = 'SELECT * FROM E3Settings WHERE Version={}'.format(acc_version)
    Settings = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns = [
        ['PA','ProgramAdministrator'],
    ]
    for old_name,new_name in rename_columns:
        Settings.rename_column(old_name,new_name)

    Settings.column_map('ProgramAdministrator',lambda s: s.strip())

    # add method to get settings data filtered by a single input measure:
    def filter_by_measure(self, input_measure):
        filtered_settings = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator)
        )
        return filtered_settings
    Settings.filter_by_measure = types.MethodType(filter_by_measure,Settings)

    return Settings


def setup_avoided_cost_electric(acc_electric_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+UPPER(TS)+UPPER(EU)+CZ\n\t' \
                'IN (\n\t\tSELECT PA+UPPER(ElecTargetSector)+UPPER(ElecEndUseShape)+ClimateZone\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_electric_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(TS)+\'|\'+UPPER(EU)+\'|\'+CZ\n\t' \
                'IN (\n\t\tSELECT PA + \'|\' + UPPER(E3TargetSector) + \'|\' + UPPER(E3MeaElecEndUseShape) + \'|\' + E3ClimateZone AS LookupKey\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_electric_table_name)
        else:
            sql_str = 'SELECT * FROM {}'.format(acc_electric_table_name)
    else:
        # use the following query string when input measures are from a file:
        input_metadata_electric = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['ProgramAdministrator','ElectricTargetSector','ElectricEndUse','ClimateZone']])+'\'' for r in InputMeasures.data.iterrows()])))
        sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ\n\tIN (\n\t\t{}\n\t)\n'.format(acc_electric_table_name,input_meta_data_electric)

    AvoidedCostElectric = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns = [
        ['PA','ProgramAdministrator'],
        ['TS','ElectricTargetSector'],
        ['EU','ElectricEndUse'],
        ['CZ','ClimateZone'],
        ['Qtr','ApplicableQuarter'],
    ]
    for old_name,new_name in rename_columns:
        AvoidedCostElectric.rename_column(old_name,new_name)

    # fix column formatting:
    AvoidedCostElectric.column_map('ElectricTargetSector',lambda s: s.upper())
    AvoidedCostElectric.column_map('ElectricEndUse',lambda s: s.upper())
    AvoidedCostElectric.column_map('ClimateZone',lambda s: s.upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.ApplicableQuarter.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostElectric.append_columns(AvoidedCostElectric.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost electric data filtered by a single input measure:
    def filter_by_measure(self, input_measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == input_measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == input_measure.ElectricEndUse) & \
            (self.data.ClimateZone == input_measure.ClimateZone) & \
            (self.data.Qi >= input_measure.Qi) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.filter_by_measure = types.MethodType(filter_by_measure,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_gas_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(GS)+\'|\'+UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA+\'|\'+UPPER(GasSector)+\'|\'+UPPER(GasSavingsProfile)\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_gas_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(GS)+\'|\'+UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA+\'|\'+UPPER(E3GasSector)+\'|\'+UPPER(E3GasSavProfile)\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_gas_table_name)
        else:
            sql_str = 'SELECT * FROM {}'.format(acc_gas_table_name)
    else:
        input_metadata_gas = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['ProgramAdministrator','GasTargetSector','GasSavingsProfile']])+'\'' for r in self.InputMeasures.data.iterrows()])))
        sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(GS)+\'|\'+UPPER(GP)\n\tIN (\n\t\t{}\n\t)\n'.format(self._acc_tables[acc_version]['gas'],input_metadata_gas)

    # use the following alternative query string when input measures are loaded into database:
    AvoidedCostGas = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns = [
        ['PA','ProgramAdministrator'],
        ['GS','GasTargetSector'],
        ['GP','GasSavingsProfile'],
        ['Qtr','ApplicableQuarter'],
        ['Total','Cost'],
    ]
    for old_name,new_name in rename_columns:
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
    def filter_by_measure(self, input_measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == input_measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == input_measure.GasSavingsProfile) & \
            (self.data.Qi >= input_measure.Qi) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.filter_by_measure = types.MethodType(filter_by_measure,AvoidedCostGas)

    return AvoidedCostGas

def setup_rate_schedule_electric(rate_schedule_version, InputMeasures, user):
    rate_schedule_electric_table_name = 'E3RateScheduleElec'
    if InputMeasures.source == 'database':
        if InputMeasures.table_name == 'InputMeasure':
            rate_schedule_metadata = '\n\tSELECT DISTINCT\n\t\tCASE' + \
                '\n\t\t\tWHEN PA = \'PGE\' OR PA = \'SDGE\'' + \
                '\n\t\t\tTHEN PA + \'|\' + UPPER(ElecTargetSector)' + \
                '\n\t\t\tELSE PA + \'|ALL\'' + \
                '\n\t\tEND AS LookupKey' + \
                '\n\tFROM {}'
            rate_schedule_metadata = rate_schedule_metadata.format(InputMeasures.table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            rate_schedule_metadata = '\n\tSELECT DISTINCT\n\t\tCASE' + \
                '\n\t\t\tWHEN PA = \'PGE\' OR PA = \'SDGE\'' + \
                '\n\t\t\tTHEN PA + \'|\' + UPPER(E3TargetSector)' + \
                '\n\t\t\tELSE PA + \'|ALL\'' + \
                '\n\t\tEND AS LookupKey' + \
                '\n\tFROM {}'
            rate_schedule_metadata = rate_schedule_metadata.format(InputMeasures.table_name)
    else:
        def get_lookup_key(r):
            if r.ProgramAdministrator == 'PGE' or r.ProgramAdministrator == 'SDGE':
                lookup_key = '\'{}|{}\''.format(r.ProgramAdministrator,r.ElectricTargetSector) 
            else:
                lookup_key = '\'{}|ALL\''.format(r.ProgramAdministrator)
            return lookup_key
        rate_schedule_metadata = ','.join(list(dict.fromkeys(InputMeasures.data.apply(get_lookup_key,axis='columns'))))
    sql_str = '\nSELECT' + \
        '\n\tRates.PA,\n\tRates.Version,\n\tRates.Schedule,' + \
        '\n\tMap.TargetSector,\n\tRates.Year,\n\tRates.RateE' + \
        '\nFROM {} AS Rates\nLEFT JOIN {} AS Map' + \
        '\n\tON Rates.PA = Map.PA\n\tAND Rates.Version = Map.Version' + \
        '\n\tAND Rates.Schedule = Map.Schedule' + \
        '\nWHERE\n\tRates.PA + \'|\' + UPPER(Map.TargetSector) IN ({}\n\t)\nAND Rates.Version={}'
    sql_str = sql_str.format(rate_schedule_electric_table_name,rate_schedule_electric_table_name + 'Mapping',rate_schedule_metadata,rate_schedule_version)

    RateScheduleElectric = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns= [
        ['PA','ProgramAdministrator'],
        ['Schedule','CustomerType'],
        ['TargetSector','ElectricTargetSector'],
        ['Year','ApplicableYear'],
        ['RateE','ElectricRate'],
    ]
    for old_name,new_name in rename_columns:
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
    RateScheduleElectric.filter_by_measure = types.MethodType(filter_by_measure,RateScheduleElectric)

    return RateScheduleElectric

def setup_rate_schedule_gas(rate_schedule_version, InputMeasures, user):
    rate_schedule_gas_table_name = 'E3RateScheduleGas'
    if InputMeasures.source == 'database':
        if InputMeasures.table_name == 'InputMeasure':
            rate_schedule_metadata = '\n\tSELECT DISTINCT' + \
                '\n\t\tPA + \'|\' + UPPER(COALESCE(GasSector,ElecTargetSector) AS LookupKey' + \
                '\n\tFROM {}'
            rate_schedule_metadata = rate_schedule_metadata.format(InputMeasures.table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            rate_schedule_metadata = '\n\t\tSELECT DISTINCT' + \
                '\n\t\t\tPA + \'|\' + UPPER(COALESCE(E3GasSector,E3TargetSector)) AS LookupKey' + \
                '\n\t\tFROM {}'
            rate_schedule_metadata = rate_schedule_metadata.format(InputMeasures.table_name)
    else:
        def get_lookup_key(r):
            lookup_key = '\'{}|{}\''.format(r.ProgramAdministrator,r.GasTargetSector) 
            return lookup_key
        rate_schedule_metadata = ','.join(list(dict.fromkeys(InputMeasures.data.apply(get_lookup_key,axis='columns'))))
    sql_str = '\nSELECT' + \
        '\n\tRates.PA,\n\tRates.Version,\n\tRates.Schedule,' + \
        '\n\tMap.GasSector,\n\tRates.Year,\n\tRates.RateG' + \
        '\nFROM {} AS Rates\nLEFT JOIN {} AS Map' + \
        '\n\tON Rates.PA = Map.PA\n\tAND Rates.Version = Map.Version' + \
        '\n\tAND Rates.Schedule = Map.GasRateSchedule' + \
        '\nWHERE\n\tRates.PA + \'|\' + UPPER(REPLACE(Map.GasSector,CHAR(13)+CHAR(10),\'\')) IN ({}\n\t)\nAND Rates.Version={}'
    sql_str = sql_str.format(rate_schedule_gas_table_name,rate_schedule_gas_table_name + 'Mapping',rate_schedule_metadata,rate_schedule_version)

    RateScheduleGas = EDCS_Query_Results(sql_str,user['id'],user['passwd'])

    rename_columns= [
        ['PA','ProgramAdministrator'],
        ['Schedule','CustomerType'],
        ['GasSector','GasTargetSector'],
        ['Year','ApplicableYear'],
        ['RateG','GasRate'],
    ]
    for old_name,new_name in rename_columns:
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
    RateScheduleGas.filter_by_measure = types.MethodType(filter_by_measure,RateScheduleGas)

    return RateScheduleGas

def setup_cost_effectiveness_outputs(user):
    OutputCE = EDCS_Table('OutputCE',user['id'],user['passwd'],fetch_init=False)
    OutputCE.set_table_cols([
        'ID','JobID','ProgramAdministrator','ProgramID','CET_ID',
        'ElectricBenefitsNet','GasBenefitsNet','ElectricBenefitsGross',
        'GasBenefitsGross','TRCCostNet','TRCCostGross','TRCCostNetNoAdmin',
        'TRCRatio','TRCRatioNoAdmin','PACCostNet','PACCostNetNoAdmin',
        'PACRatio','PACRatioNoAdmin','BillReducElec','BillReducGas','RIMCost',
        'WeightedBenefits','WeightedElecAlloc','WeightedProgramCost',
    ])

    return OutputCE

def setup_program_outputs(user):
    OutputPrograms = EDCS_Table('OutputProgram',user['id'],user['passwd'],fetch_init=False)
    OutputPrograms.set_table_cols([
        'ID','ProgramAdministrator','ProgramID'
    ])

    return OutputPrograms
