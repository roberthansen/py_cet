import os, types, numpy as np, pandas as pd
from datetime import datetime as dt
import aggregation, aggregation_match_sql

import os
import multiprocessing as mp
import dill as pickle

# calculate measure-level benefits and costs:
def calculate_measure_cost_effectiveness(cet_scenario):
    ### parameters:
    ###     cet_scenario : an instance of the CETScenario class containing
    ###         the input data and parameters used in calculating cost
    ###         effectiveness outputs
    ###
    ### outputs:
    ###     pandas DataFrame with measure-level cost-effectiveness outputs

    if cet_scenario.match_sql():
        agg = aggregation_match_sql
    else:
        agg = aggregation

    if cet_scenario.parallelize:
        # create new classes for tables without pyodbc connections:
        InputMeasuresData = cet_scenario.InputMeasures.data
        Settings = SettingsTable(cet_scenario.Settings.data)

        AvoidedCostElectric = AvoidedCostElectricTable(cet_scenario.AvoidedCostElectric.data)

        AvoidedCostGas = AvoidedCostGasTable(cet_scenario.AvoidedCostGas.data)

        # run parallelized apply functions:
        t0 = dt.now()
        avoided_electric_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostElectric, Settings, agg.calculate_avoided_electric_costs, cet_scenario.first_year).calculate()
        avoided_gas_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostGas, Settings, agg.calculate_avoided_gas_costs, cet_scenario.first_year).calculate()
        print('< Benefits Calculation Time with Parallelization: {:.3f} seconds >'.format((dt.now()-t0).total_seconds()))
    else:
        t0 = dt.now()
        f = lambda r: agg.calculate_avoided_electric_costs(r, cet_scenario.AvoidedCostElectric, cet_scenario.Settings, cet_scenario.first_year)
        avoided_electric_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')

        f = lambda r: agg.calculate_avoided_gas_costs(r, cet_scenario.AvoidedCostGas, cet_scenario.Settings, cet_scenario.first_year)
        avoided_gas_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        print('< Benefits Calculation Time without Parallelization: {:.3f} seconds >'.format((dt.now()-t0).total_seconds()))

    measures = cet_scenario.InputMeasures.data.merge(
        avoided_electric_costs, on=['CET_ID','ProgramID','Qi']).merge(
        avoided_gas_costs, on=['CET_ID','ProgramID','Qi'])

    benefit_sums = pd.DataFrame({
        'ProgramID'             : avoided_electric_costs.ProgramID,
        'Qi'                    : avoided_electric_costs.Qi,
        'Count'                 : 1,
        'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross,
        'ElectricCostsGross'    : avoided_electric_costs.ElectricCostsGross,
        'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet,
        'ElectricCostsNet'      : avoided_electric_costs.ElectricCostsNet,
        'GasBenefitsGross'      : avoided_gas_costs.GasBenefitsGross,
        'GasCostsGross'         : avoided_gas_costs.GasCostsGross,
        'GasBenefitsNet'        : avoided_gas_costs.GasBenefitsNet,
        'GasCostsNet'           : avoided_gas_costs.GasBenefitsNet,
    }).groupby(['ProgramID','Qi']).aggregate(np.sum)

    programs = cet_scenario.InputPrograms.data.merge(benefit_sums, on=['ProgramID','Qi'])

    f = lambda r: agg.total_resource_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
    total_resource_cost_test_results = measures.apply(f, axis='columns')

    f = lambda r: agg.program_administrator_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
    program_administrator_cost_test_results = measures.apply(f, axis='columns')

    f = lambda r: agg.ratepayer_impact_measure_test(r,cet_scenario.RateScheduleElectric,cet_scenario.RateScheduleGas,cet_scenario.Settings,cet_scenario.first_year)
    ratepayer_impact_measure_test_results = measures.merge(program_administrator_cost_test_results,on='CET_ID').apply(f, axis='columns')

    weighted_benefits = 0
    weigted_electric_allocation = 0
    weighted_program_cost =0

    if cet_scenario.match_sql():
        outputs = pd.DataFrame({
            'CET_ID'                : avoided_electric_costs.CET_ID,
            'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross - avoided_electric_costs.ElectricCostsGross,
            'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet - avoided_electric_costs.ElectricCostsNet,
        }).merge( pd.DataFrame({
            'CET_ID'           : avoided_gas_costs.CET_ID,
            'GasBenefitsGross' : avoided_gas_costs.GasBenefitsGross - avoided_gas_costs.GasCostsGross,
            'GasBenefitsNet'   : avoided_gas_costs.GasBenefitsNet - avoided_gas_costs.GasCostsNet,
            }), on='CET_ID').merge(
            total_resource_cost_test_results, on='CET_ID').merge(
            program_administrator_cost_test_results, on='CET_ID').merge(
            ratepayer_impact_measure_test_results, on='CET_ID')
    else:
        outputs = pd.DataFrame({
            'CET_ID'                : avoided_electric_costs.CET_ID,
            'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross,
            'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet,
            'ElectricCostsGross'    : avoided_electric_costs.ElectricCostsGross,
            'ElectricCostsNet'      : avoided_electric_costs.ElectricCostsNet,
        }).merge( pd.DataFrame({
            'CET_ID'           : avoided_gas_costs.CET_ID,
            'GasBenefitsGross' : avoided_gas_costs.GasBenefitsGross,
            'GasBenefitsNet'   : avoided_gas_costs.GasBenefitsNet,
            'GasCostsGross'    : avoided_gas_costs.GasCostsGross,
            'GasCostsNet'      : avoided_gas_costs.GasCostsNet,
            }), on='CET_ID').merge(
            total_resource_cost_test_results, on='CET_ID').merge(
            program_administrator_cost_test_results, on='CET_ID').merge(
            ratepayer_impact_measure_test_results, on='CET_ID')

    return outputs

def calculate_program_cost_effectiveness(cet_scenario):
    ### parameters:
    ###     cet_scenario : an instance of the CETScenario class containing
    ###         the input data and parameters used in calculating cost
    ###         effectiveness outputs
    ###
    ### outputs:
    ###     pandas DataFrame with program-level cost-effectiveness outputs

    return pd.DataFrame()

# helper classes to parallelize iteration through measures:
class Table:
    data = pd.DataFrame()
    def filter_by_measure():
        pass
    def __init__(self):
        pass

class MultiprocessingAvoidedCosts:
    dataframe_chunks = None
    avoided_cost_function = None
    number_of_threads = 1
    first_year = None
    AvoidedCost = None
    Settings = None

    def __init__(self, InputMeasuresData, AvoidedCost, Settings, avoided_cost_function, first_year):
        self.number_of_threads = 2 * os.cpu_count()
        self.dataframe_chunks = np.array_split(InputMeasuresData,self.number_of_threads)
        self.AvoidedCost = AvoidedCost
        self.Settings = Settings
        self.avoided_cost_function = avoided_cost_function
        self.first_year = first_year

    def apply_function(self, dataframe_row):
        output = self.avoided_cost_function(dataframe_row, self.AvoidedCost, self.Settings, self.first_year)
        return output

    def map_function(self, dataframe_chunk):
        return dataframe_chunk.apply(self.apply_function, axis='columns')

    def calculate(self):
        with mp.Pool(self.number_of_threads) as mp_pool:
            output = pd.concat(mp_pool.map(self.map_function,self.dataframe_chunks))
        return output

class SettingsTable:
    data = pd.DataFrame()
    def __init__(self,dataframe):
        self.data = dataframe
    def filter_by_measure(self, measure):
        filtered_settings = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator)
        )
        return filtered_settings

class AvoidedCostElectricTable:
    data = pd.DataFrame()
    def __init__(self,dataframe):
        self.data = dataframe
    def filter_by_measure(self, measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == measure.ElectricEndUse) & \
            (self.data.ClimateZone == measure.ClimateZone) & \
            (self.data.Qi >= measure.Qi + 1) & \
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_electric

class AvoidedCostGasTable:
    data = pd.DataFrame()
    def __init__(self,dataframe):
        self.data = dataframe
    def filter_by_measure(self, measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == measure.GasSavingsProfile) & \
            (self.data.Qi >= measure.Qi + 1) & \
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_gas
