import numpy as np
import pandas as pd
import tables, aggregation, aggregation_match_sql

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

    if cet_scenario.match_sql:
        calculate_avoided_electric_costs = aggregation_match_sql.calculate_avoided_electric_costs
        calculate_avoided_gas_costs = aggregation_match_sql.calculate_avoided_gas_costs
    else:
        calculate_avoided_electric_costs = aggregation.calculate_avoided_electric_costs
        calculate_avoided_gas_costs = aggregation.calculate_avoided_gas_costs

    if cet_scenario.parallelize:
        # create new classes for tables without pyodbc connections:
        InputMeasuresData = cet_scenario.InputMeasures.data
        AvoidedCostElectric = Table()
        AvoidedCostElectric.data = cet_scenario.AvoidedCostElectric.data
        AvoidedCostElectric.metadata_filter = cet_scenario.AvoidedCostElectric.data
        AvoidedCostGas = Table()
        AvoidedCostGas.data = cet_scenario.AvoidedCostGas.data
        AvoidedCostGas.metadata_filter = cet_scenario.AvoidedCostGas.metadata_filter
        Settings = Table()
        Settings.data = cet_scenario.Settings.data
        Settings.metadata_filter = cet_scenario.Settings.metadata_filter

        # run parallelized apply functions:
        avoided_electric_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostElectric, Settings, calculate_avoided_electric_costs, cet_scenario.first_year, cet_scenario.market_effects).calculate()
        avoided_gas_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostGas, Settings, calculate_avoided_gas_costs, cet_scenario.first_year, cet_scenario.market_effects).calculate()
    else:
        f = lambda r: calculate_avoided_electric_costs(r,cet_scenario.AvoidedCostElectric, cet_scenario.Settings, cet_scenario.first_year, cet_scenario.market_effects)
        avoided_electric_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')

        f = lambda r: calculate_avoided_gas_costs(r, cet_scenario.AvoidedCostGas, cet_scenario.Settings, cet_scenario.first_year, cet_scenario.market_effects)
        avoided_gas_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')

    avoided_electric_costs.CET_ID = avoided_electric_costs.CET_ID.map(int)
    avoided_gas_costs.CET_ID = avoided_gas_costs.CET_ID.map(int)

    measures = cet_scenario.InputMeasures.data.merge(
        avoided_electric_costs, on='CET_ID').merge(
        avoided_gas_costs, on='CET_ID')

    if cet_scenario.match_sql:
        benefit_sums = pd.DataFrame({
            'PrgID'         : avoided_electric_costs.PrgID,
            'Qi'            : avoided_electric_costs.Qi,
            'Count'         : 1,
            'ElectricGross' : avoided_electric_costs.ElecBenGross,
            'ElectricNet'   : avoided_electric_costs.ElecBenNet,
            'GasGross'      : avoided_gas_costs.GasBenGross,
            'GasNet'        : avoided_gas_costs.GasBenNet,
        }).groupby('PrgID').aggregate(np.sum)
    else:
        nonneg = lambda benefit: max(benefit,0)
        benefit_sums = pd.DataFrame({
            'PrgID'         : avoided_electric_costs.PrgID,
            'Qi'            : avoided_electric_costs.Qi,
            'Count'         : 1,
            'ElectricGross' : avoided_electric_costs.ElecBenGross.map(nonneg),
            'ElectricNet'   : avoided_electric_costs.ElecBenNet.map(nonneg),
            'GasGross'      : avoided_gas_costs.GasBenGross.map(nonneg),
            'GasNet'        : avoided_gas_costs.GasBenNet.map(nonneg),
        }).groupby(by=['PrgID','Qi']).aggregate(np.sum)

    programs = cet_scenario.InputPrograms.data.merge(benefit_sums, on=['PrgID','Qi'])

    f = lambda r: aggregation.calculate_total_resource_costs(r, programs, cet_scenario.Settings, cet_scenario.first_year)
    total_resource_costs = measures.apply(f, axis='columns')

    program_administrator_cost_gross = 0
    program_administrator_cost_net = 0
    program_administrator_cost_no_admin = 0
    program_administrator_ratio = 0
    program_administrator_ratio_no_admin = 0

    bill_reduction_electric = 0
    bill_reduction_gas = 0

    ratepayer_cost = 0
    
    weighted_benefits = 0
    weigted_electric_allocation = 0
    weighted_program_cost =0

    outputs = avoided_electric_costs[['CET_ID','ElecBenGross','ElecBenNet']].merge(
        avoided_gas_costs[['CET_ID','GasBenGross','GasBenNet']], on='CET_ID').merge(
        total_resource_costs, on='CET_ID')

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
    def __init__(self):
        pass

class MultiprocessingAvoidedCosts:
    dataframe_chunks = None
    avoided_cost_function = None
    number_of_threads = 1
    first_year = None
    market_effects = 0
    AvoidedCost = None
    Settings = None

    def __init__(self, InputMeasuresData, AvoidedCostData, SettingsData, avoided_cost_function, first_year, market_effects):
        self.number_of_threads = os.cpu_count()
        self.dataframe_chunks = np.array_split(InputMeasuresData,self.number_of_threads)
        self.AvoidedCost = AvoidedCostData
        self.Settings = SettingsData
        self.avoided_cost_function = avoided_cost_function
        self.first_year = first_year
        self.market_effects = market_effects

    def apply_function(self, dataframe_row):
        output = self.avoided_cost_function(dataframe_row, self.AvoidedCost, self.Settings, self.first_year, self.market_effects)
        return output

    def map_function(self, dataframe_chunk):
        return dataframe_chunk.apply(self.apply_function, axis='columns')

    def calculate(self):
        with mp.Pool(self.number_of_threads) as mp_pool:
            output = pd.concat(mp_pool.map(self.map_function,self.dataframe_chunks))
        return output
