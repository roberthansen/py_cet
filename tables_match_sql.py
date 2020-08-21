import numpy as np
import types
from models import EDCS_Connection, EDCS_Table, EDCS_Query_Results

from tables import setup_input_programs, setup_settings, \
    setup_output_measures, setup_output_programs, \
    setup_rate_schedule_electric, setup_rate_schedule_gas

from login import user

def setup_input_measures(source, source_name, first_year, market_effects_benefits, market_effects_costs, user):
    if source == 'csv':
        InputMeasures = Local_CSV(source_name)
    elif source == 'database':
        InputMeasures = EDCS_Table(source_name,user['id'],user['passwd'])
    else:
        InputMeasures = EDCS_Table('InputMeasureCEDARS',user['id'],user['passwd'])

    # fix input measure column name and type issues:
    if InputMeasures.source == 'csv':
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

    # fix input measure column name and type issues:
    else:
        if InputMeasures.table_name == 'InputMeasure':
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

        elif InputMeasures.table_name == 'InputMeasureCEDARS':
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

    #INCORRECT OVERWRITE MEASURE-LEVEL MARKET EFFECTS TO MATCH SQL:
    InputMeasures.data.MarketEffectsBenefits = market_effects_benefits
    InputMeasures.data.MarketEffectsCosts = market_effects_costs

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

def setup_avoided_cost_electric(acc_electric_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)\n\t' \
                'IN (\n\t\tSELECT PA + \'|\' + UPPER(ElecTargetSector) + ' \
                '\'|\' + UPPER(ElecEndUseShape) + \'|\' + ' \
                'UPPER(ClimateZone)\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_electric_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + UPPER(CZ)\n\t' \
                'IN (\n\t\tSELECT' \
                '\n\t\t\tPA + \'|\' + UPPER(E3TargetSector) + \'|\' + ' \
                'UPPER(E3MeaElecEndUseShape) + \'|\' + ' \
                'UPPER(E3ClimateZone) AS LookupKey\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_electric_table_name)
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
            '\n\tIN (\n\t\t{}\n\t)\n'.format(acc_electric_table_name,lookup_keys)

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
    AvoidedCostElectric.column_map('ClimateZone',lambda s: s.upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.UsageQuarter.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostElectric.append_columns(AvoidedCostElectric.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost electric data filtered by a single input measure:
    def filter_by_measure(self, input_measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == input_measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == input_measure.ElectricEndUse) & \
            (self.data.ClimateZone == input_measure.ClimateZone) & \
            (self.data.Qi >= input_measure.Qi + 1) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq + 1)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_gas_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(GS) + \'|\' + UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA + UPPER(GasSector) + \'|\' + ' \
                'UPPER(GasSavingsProfile)\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_gas_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + ' \
                'UPPER(GS) + \'|\' + UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA + \'|\' + UPPER(E3GasSector) + ' \
                '\'|\' + UPPER(E3GasSavProfile)\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_gas_table_name)
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
            '\n\tIN (\n\t\t{}\n\t)\n'.format(acc_gas_table_name,lookup_keys)

    # use the following alternative query string when input measures are loaded into database:
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
    def filter_by_measure(self, input_measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == input_measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == input_measure.GasSavingsProfile) & \
            (self.data.Qi >= input_measure.Qi + 1) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq + 1)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.filter_by_measure = \
        types.MethodType(filter_by_measure,AvoidedCostGas)

    return AvoidedCostGas
