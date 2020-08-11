import numpy as np
import types
from models import EDCS_Connection,EDCS_Table,EDCS_Query_Results

from tables import setup_cost_effectiveness_outputs,setup_program_outputs

from login import user

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
            ['RealizationRatekW','RRkW'],
            ['InstallationRatekWh','IRkWh'],
            ['RealizationRatekWh','RRkWh'],
            ['RealizationRateTherm','RRTherm'],
            ['InstallationRateTherm','IRTherm'],
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
    InputMeasures.column_map('MarketEffectsCosts',np.nan_to_num)

    # helper function to calculate additional columns for input measure table:
    def input_measure_calculated_columns(data_frame_row):
        year,quarter = list(map(int,data_frame_row.InstallationQuarter.split('Q')))
        quarter_index = 4 * year + quarter - 1
        rul_quarters = 4 * data_frame_row.RUL
        eul_quarters = 4 * data_frame_row.EUL
        if rul_quarters == 0:
            EULq = [eul_quarters,0]
        else:
            EULq = [rul_quarters,eul_quarters]
        calculated_columns = {
            'Qi'    : quarter_index,
            'EULq1' : EULq[0],
            'EULq2' : EULq[1],
            'RULq'  : rul_quarters,
            'EULq'  : eul_quarters,
        }
        return calculated_columns

    # append input measures with calculated columns:
    InputMeasures.append_columns(InputMeasures.data.apply(input_measure_calculated_columns, axis='columns', result_type='expand'))

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
    def input_programs_metadata_filter(self, input_measure):
        filtered_input_programs = self.data.get(
            (self.data.ProgramID == input_measure.ProgramID)
        )
        return filtered_input_programs
    InputPrograms.metadata_filter = types.MethodType(input_programs_metadata_filter,InputPrograms)

    return InputPrograms

def setup_settings(acc_version, InputMeasures, user):
    sql_str = 'SELECT * FROM E3Settings WHERE Version={}'.format(acc_version)
    Settings = EDCS_Query_Results(sql_str,user['id'],user['passwd'])
    Settings.column_map('PA',lambda s: s.strip())

    rename_columns = [
        ['PA','ProgramAdministrator'],
    ]
    for old_name,new_name in rename_columns:
        Settings.rename_column(old_name,new_name)

    # add method to get settings data filtered by a single input measure:
    def settings_metadata_filter(self, input_measure):
        filtered_settings = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator)
        )
        return filtered_settings
    Settings.metadata_filter = types.MethodType(settings_metadata_filter,Settings)

    return Settings


def setup_avoided_cost_electric(acc_electric_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            acc_elec_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+UPPER(TS)+UPPER(EU)+CZ\n\t' \
                'IN (\n\t\tSELECT PA+UPPER(ElecTargetSector)+UPPER(ElecEndUseShape)+ClimateZone\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_electric_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            acc_elec_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+UPPER(TS)+UPPER(EU)+CZ\n\t' \
                'IN (\n\t\tSELECT PA+UPPER(E3TargetSector)+UPPER(E3MeaElecEndUseShape)+E3ClimateZone\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_electric_table_name)
    else:
        # use the following query string when input measures are from a file:
        input_meta_data_elec = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['ProgramAdministrator','ElectricTargetSector','ElectricEndUse','ClimateZone']])+'\'' for r in InputMeasures.data.iterrows()])))
        acc_elec_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(TS)+\'|\'+UPPER(EU)+\'|\'+CZ\n\tIN (\n\t\t{}\n\t)\n'.format(acc_electric_table_name,input_meta_data_elec)

    AvoidedCostElectric = EDCS_Query_Results(acc_elec_sql_str,user['id'],user['passwd'])

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
    def avoided_cost_electric_metadata_filter(self, input_measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == input_measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == input_measure.ElectricEndUse) & \
            (self.data.ClimateZone == input_measure.ClimateZone) & \
            (self.data.Qi >= input_measure.Qi + 1) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq + 1)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.metadata_filter = types.MethodType(avoided_cost_electric_metadata_filter,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_gas_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            acc_gas_sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA+UPPER(GS)+UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA+UPPER(GasSector)+UPPER(GasSavingsProfile)\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_gas_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            acc_gas_sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA+UPPER(GS)+UPPER(GP)\n\t' \
                'IN (\n\t\tSELECT PA+UPPER(E3GasSector)+UPPER(E3GasSavProfile)\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_gas_table_name)
    else:
        input_meta_data_gas = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['PA','GS','GP']])+'\'' for r in self.InputMeasures.data.iterrows()])))
        acc_gas_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(GS)+\'|\'+UPPER(GP)\n\tIN (\n\t\t{}\n\t)\n'.format(self._acc_tables[acc_version]['gas'],input_meta_data_gas)

    # use the following alternative query string when input measures are loaded into database:
    AvoidedCostGas = EDCS_Query_Results(acc_gas_sql_str,user['id'],user['passwd'])

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
    def avoided_cost_gas_metadata_filter(self, input_measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == input_measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == input_measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == input_measure.GasSavingsProfile) & \
            (self.data.Qi >= input_measure.Qi + 1) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq + 1)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.metadata_filter = types.MethodType(avoided_cost_gas_metadata_filter,AvoidedCostGas)

    return AvoidedCostGas
