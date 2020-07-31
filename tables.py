import types
from models import EDCS_Connection,EDCS_Table,EDCS_Query_Results

from login import user

def setup_input_measures(first_year):
    InputMeasures = EDCS_Table('InputMeasure',user['id'],user['passwd'])

    # indicate source of table from database:
    InputMeasures.source = 'database'

    # fix input measure column name and type issues:
    InputMeasures.rename_column('ElecTargetSector','TS')
    InputMeasures.rename_column('GasSector','GS')
    InputMeasures.rename_column('GasSavingsProfile','GP')
    InputMeasures.rename_column('ElecEndUseShape','EU')
    InputMeasures.rename_column('ClimateZone','CZ')
    InputMeasures.column_map('CET_ID',lambda s: int(s))
    InputMeasures.column_map('TS',lambda s: s.upper())
    InputMeasures.column_map('GS',lambda s: s or '')
    InputMeasures.column_map('GS',lambda s: s.upper())
    InputMeasures.column_map('GP',lambda s: s or '')
    InputMeasures.column_map('GP',lambda s: s.upper())
    InputMeasures.column_map('EU',lambda s: s.upper())
    InputMeasures.column_map('CZ',lambda s: s.upper())

    # helper function to calculate additional columns for input measure table:
    def input_measure_calculated_columns(data_frame_row):
        year,quarter = list(map(int,data_frame_row.ClaimYearQuarter.split('Q')))
        quarter_index = 4 * year + quarter - 1
        rul_quarters = 4 * data_frame_row.RUL
        eul_quarters = 4 * data_frame_row.EUL
        kWh=data_frame_row[['UESkWh','UESkWh_ER']]
        kW=data_frame_row[['UESkW','UESkW_ER']]
        therm=data_frame_row[['UESThm','UESThm_ER']]
        if rul_quarters == 0:
            EULq = [eul_quarters,0]
        else:
            EULq = [rul_quarters,eul_quarters]
        calculated_columns = {
            'Qi' : quarter_index,
            'EULq1' : EULq[0],
            'EULq2' : EULq[1],
            'RULq' : rul_quarters,
            'EULq' : eul_quarters,
            'kWh1' : data_frame_row.UESkWh,
            'kWh2' : data_frame_row.UESkWh_ER,
            'kW1' : data_frame_row.UESkW,
            'kW2' : data_frame_row.UESkW_ER,
            'Thm1' : data_frame_row.UESThm,
            'Thm2' : data_frame_row.UESThm_ER,
        }
        return calculated_columns

    # append input measures with calculated columns:
    InputMeasures.append_columns(InputMeasures.data.apply(input_measure_calculated_columns, axis='columns', result_type='expand'))

    return InputMeasures

def setup_input_programs():
    InputPrograms = EDCS_Table('InputProgram',user['id'],user['passwd'])

    # add column with universal quarter index:
    def input_program_calculated_columns(data_frame_row):
        year,quarter = list(map(int,data_frame_row.ClaimYearQuarter.split('Q')))
        quarter_index = 4 * year + quarter - 1
        calculated_columns = {
            'Qi' : quarter_index,
        }
        return calculated_columns
    
    InputPrograms.append_columns(InputPrograms.data.apply(input_program_calculated_columns, axis='columns', result_type='expand'))

    # add method to get input program data filtered by a single input measure:
    def input_programs_metadata_filter(self, input_measure):
        filtered_input_programs = self.data.get(
            (self.data.PrgID == input_measure.PrgID)
        )
        return filtered_input_programs
    InputPrograms.metadata_filter = types.MethodType(input_programs_metadata_filter,InputPrograms)

    return InputPrograms

def setup_settings(acc_version,InputMeasures):
    sql_str = 'SELECT * FROM E3Settings WHERE Version={}'.format(acc_version)
    Settings = EDCS_Query_Results(sql_str,user['id'],user['passwd'])
    Settings.column_map('PA',lambda s: s.strip())

    # add method to get settings data filtered by a single input measure:
    def settings_metadata_filter(self, input_measure):
        filtered_settings = self.data.get(
            (self.data.PA == input_measure.PA)
        )
        return filtered_settings
    Settings.metadata_filter = types.MethodType(settings_metadata_filter,Settings)

    return Settings


def setup_avoided_cost_electric(acc_electric_table_name,InputMeasures):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        acc_elec_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+UPPER(TS)+UPPER(EU)+CZ\n\t' \
            'IN (\n\t\tSELECT PA+UPPER(ElecTargetSector)+UPPER(ElecEndUseShape)+ClimateZone\n\t\t' \
            'FROM InputMeasure\n\t)\n'.format(acc_electric_table_name)
    else:
        # use the following query string when input measures are from a file:
        input_meta_data_elec = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['PA','TS','EU','CZ']])+'\'' for r in InputMeasures.data.iterrows()])))
        acc_elec_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(TS)+\'|\'+UPPER(EU)+\'|\'+CZ\n\tIN (\n\t\t{}\n\t)\n'.format(acc_electric_table_name,input_meta_data_elec)

    AvoidedCostElectric = EDCS_Query_Results(acc_elec_sql_str,user['id'],user['passwd'])

    # fix column formatting:
    AvoidedCostElectric.column_map('TS',lambda s: s.upper())
    AvoidedCostElectric.column_map('EU',lambda s: s.upper())
    AvoidedCostElectric.column_map('CZ',lambda s: s.upper())

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.Qtr.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostElectric.append_columns(AvoidedCostElectric.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost electric data filtered by a single input measure:
    def avoided_cost_electric_metadata_filter(self, input_measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.PA == input_measure.PA) & \
            (self.data.TS == input_measure.TS) & \
            (self.data.EU == input_measure.EU) & \
            (self.data.CZ == input_measure.CZ) & \
            (self.data.Qi >= input_measure.Qi) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq)
        )
        return filtered_avoided_costs_electric
    AvoidedCostElectric.metadata_filter = types.MethodType(avoided_cost_electric_metadata_filter,AvoidedCostElectric)

    return AvoidedCostElectric

def setup_avoided_cost_gas(acc_gas_table_name,InputMeasures):
    if InputMeasures.source == 'database':
        # use the following query string when input measures are loaded into database:
        acc_gas_sql_str = 'SELECT *\n\tFROM {}\n\tWHERE PA+UPPER(GS)+UPPER(GP)\n\t' \
            'IN (\n\t\tSELECT PA+UPPER(GasSector)+UPPER(GasSavingsProfile)\n\t\t' \
            'FROM InputMeasure\n\t)\n'.format(acc_gas_table_name)
    else:
        input_meta_data_gas = ',\n\t\t'.join(list(dict.fromkeys(['\''+'|'.join(r[1][['PA','GS','GP']])+'\'' for r in self.InputMeasures.data.iterrows()])))
        acc_gas_sql_str = '\n\tSELECT *\n\tFROM {}\n\tWHERE PA+\'|\'+UPPER(GS)+\'|\'+UPPER(GP)\n\tIN (\n\t\t{}\n\t)\n'.format(self._acc_tables[acc_version]['gas'],input_meta_data_gas)

    # use the following alternative query string when input measures are loaded into database:
    AvoidedCostGas = EDCS_Query_Results(acc_gas_sql_str,user['id'],user['passwd'])

    # fix column formatting:
    AvoidedCostGas.column_map('GS',lambda s: s.upper())
    AvoidedCostGas.column_map('GP',lambda s: s.upper())
    AvoidedCostGas.rename_column('Total','Cost')

    # apply universal quarter indices:
    def quarter_index(r):
        year,quarter = list(map(int,r.Qtr.split('Q')))
        return {'Qi' : year * 4 + quarter - 1}
    AvoidedCostGas.append_columns(AvoidedCostGas.data.apply(quarter_index,axis='columns',result_type='expand'))

    # add method to get avoided cost gas data filtered by a single input measure:
    def avoided_cost_gas_metadata_filter(self, input_measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.PA == input_measure.PA) & \
            (self.data.GS == input_measure.GS) & \
            (self.data.GP == input_measure.GP) & \
            (self.data.Qi >= input_measure.Qi) & \
            (self.data.Qi < input_measure.Qi + input_measure.EULq)
        )
        return filtered_avoided_costs_gas
    AvoidedCostGas.metadata_filter = types.MethodType(avoided_cost_gas_metadata_filter,AvoidedCostGas)

    return AvoidedCostGas

def setup_outputs():
    OutputCE = EDCS_Table('OutputCE',user['id'],user['passwd'],fetch_init=False)
    OutputCE.set_table_cols([
        'ID','JobID','PA','PrgID','CET_ID','ElecBen','GasBen',
        'ElecBenGross','GasBenGross','TRCCost','PACCost','TRCCostGross',
        'TRCCostNoAdmin','PACCostNoAdmin','TRCRatio','PACRatio',
        'TRCRatioNoAdmin','PACRatioNoAdmin','BillReducElec',
        'BillReducGas','RIMCost','WeightedBenefits','WeightedElecAlloc',
        'WeightedProgramCost'
    ])

    return OutputCE
