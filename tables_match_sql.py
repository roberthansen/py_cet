import numpy as np
import types
from models import EDCS_Connection, EDCS_Table, EDCS_Query_Results

from tables import setup_input_measures, setup_input_programs, setup_settings, setup_cost_effectiveness_outputs, setup_program_outputs, setup_rate_schedule_electric, setup_rate_schedule_gas

from login import user

def setup_avoided_cost_electric(acc_electric_table_name, InputMeasures, user):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        if InputMeasures.table_name == 'InputMeasure':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ\n\t' \
                'IN (\n\t\tSELECT PA + \'|\' + UPPER(ElecTargetSector) + \'|\' + UPPER(ElecEndUseShape) + \'|\' + ClimateZone\n\t\t' \
                'FROM InputMeasure\n\t)\n'.format(acc_electric_table_name)
        elif InputMeasures.table_name == 'InputMeasureCEDARS':
            sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ AS LookupKey\n\t' \
                'IN (\n\t\tSELECT PA + \'|\' + UPPER(E3TargetSector) + \'|\' + UPPER(E3MeaElecEndUseShape) + \'|\' + E3ClimateZone\n\t\t' \
                'FROM InputMeasureCEDARS\n\t)\n'.format(acc_electric_table_name)
    else:
        # use the following query string when input measures are from a file:
        input_metadata_electric = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['ProgramAdministrator','ElectricTargetSector','ElectricEndUse','ClimateZone']])+'\'' for r in InputMeasures.data.iterrows()])))
        sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA + \'|\' + UPPER(TS) + \'|\' + UPPER(EU) + \'|\' + CZ\n\tIN (\n\t\t{}\n\t)\n'.format(acc_electric_table_name,input_metadata_electric)

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
    AvoidedCostElectric.filter_by_measure = types.MethodType(avoided_cost_electric_metadata_filter,AvoidedCostElectric)

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
    AvoidedCostGas.filter_by_measure = types.MethodType(avoided_cost_gas_metadata_filter,AvoidedCostGas)

    return AvoidedCostGas
