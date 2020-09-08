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
        t_acce = dt.now()
        avoided_electric_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostElectric, Settings, cet_scenario.first_year, agg.calculate_avoided_electric_costs).calculate()
        cet_scenario.calculation_times['avoided_electric_costs'] = (dt.now() - t_acce).total_seconds()

        t_accg = dt.now()
        avoided_gas_costs = MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostGas, Settings, cet_scenario.first_year, agg.calculate_avoided_gas_costs).calculate()
        cet_scenario.calculation_times['avoided_gas_costs'] = (dt.now() - t_accg).total_seconds()


        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['avoided_electric_costs','avoided_gas_costs']])
        print('< Benefits Calculation Time with Parallelization: {:.3f} seconds >'.format(calculation_time))

    else:
        # run standard serial apply functions:
        t_acce = dt.now()
        f = lambda r: agg.calculate_avoided_electric_costs(r, cet_scenario.AvoidedCostElectric, cet_scenario.Settings, cet_scenario.first_year)
        avoided_electric_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        cet_scenario.calculation_times['avoided_electric_costs'] = (dt.now() - t_acce).total_seconds()

        t_accg = dt.now()
        f = lambda r: agg.calculate_avoided_gas_costs(r, cet_scenario.AvoidedCostGas, cet_scenario.Settings, cet_scenario.first_year)
        avoided_gas_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        cet_scenario.calculation_times['avoided_gas_costs'] = (dt.now() - t_accg).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['avoided_electric_costs','avoided_gas_costs']])
        print('< Benefits Calculation Time without Parallelization: {:.3f} seconds >'.format(calculation_time))

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

    if cet_scenario.parallelize:
        t_trc = dt.now()
        total_resource_cost_test_results = MultiprocessingCostTest(measures, programs, Settings, cet_scenario.first_year, agg.total_resource_cost_test).calculate()
        cet_scenario.calculation_times['total_resource_cost_test'] = (dt.now() - t_trc).total_seconds()
        t_pac = dt.now()
        program_administrator_cost_test_results = MultiprocessingCostTest(measures, programs, Settings, cet_scenario.first_year, agg.program_administrator_cost_test).calculate()
        cet_scenario.calculation_times['program_administrator_cost_test'] = (dt.now() - t_pac).total_seconds()

        RateScheduleElectric = RateScheduleElectricTable(cet_scenario.RateScheduleElectric.data)
        RateScheduleGas = RateScheduleGasTable(cet_scenario.RateScheduleGas.data)

        measures = measures.merge(program_administrator_cost_test_results,on='CET_ID')

        t_rim = dt.now()
        ratepayer_impact_measure_results = MultiprocessingRatepayerImpactMeasure(measures, RateScheduleElectric, RateScheduleGas, Settings, cet_scenario.first_year, agg.ratepayer_impact_measure).calculate()
        cet_scenario.calculation_times['ratepayer_impact_measure'] = (dt.now() - t_pac).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['total_resource_cost_test','program_administrator_cost_test','ratepayer_impact_measure']])
        print('< Test Calculation Time with Parallelization: {:.3f} seconds >'.format(calculation_time))

    else:
        t_trc = dt.now()
        f = lambda r: agg.total_resource_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
        total_resource_cost_test_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['total_resource_cost_test'] = (dt.now() - t_trc).total_seconds()

        t_pac = dt.now()
        f = lambda r: agg.program_administrator_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
        program_administrator_cost_test_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['program_administrator_cost_test'] = (dt.now() - t_pac).total_seconds()

        measures = measures.merge(program_administrator_cost_test_results,on='CET_ID')

        t_rim = dt.now()
        f = lambda r: agg.ratepayer_impact_measure(r,cet_scenario.RateScheduleElectric,cet_scenario.RateScheduleGas,cet_scenario.Settings,cet_scenario.first_year)
        ratepayer_impact_measure_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['ratepayer_impact_measure'] = (dt.now() - t_pac).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['total_resource_cost_test','program_administrator_cost_test','ratepayer_impact_measure']])
        print('< Test Calculation Time without Parallelization: {:.3f} seconds >'.format(calculation_time))

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
            ratepayer_impact_measure_results, on='CET_ID')
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
            ratepayer_impact_measure_results, on='CET_ID')

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

class MultiprocessingApplier:
    dataframe_chunks = None
    aggregation_function = None
    number_of_threads = 1
    
    def __init__(self):
        pass

    def set_threads(self):
        self.number_of_threads = 2 * os.cpu_count()

    def apply_function(self, dataframe_row):
        pass

    def map_function(self, dataframe_chunk):
        return dataframe_chunk.apply(self.apply_function, axis='columns')

    def calculate(self):
        with mp.Pool(self.number_of_threads) as mp_pool:
            output = pd.concat(mp_pool.map(self.map_function, self.dataframe_chunks))
        return output

class MultiprocessingAvoidedCosts(MultiprocessingApplier):
    AvoidedCost = None
    Settings = None
    first_year = None

    def __init__(self, measures, AvoidedCost, Settings, first_year, avoided_cost_function):
        self.set_threads()
        self.dataframe_chunks = np.array_split(measures,self.number_of_threads)
        self.AvoidedCost = AvoidedCost
        self.Settings = Settings
        self.first_year = first_year
        self.aggregation_function = avoided_cost_function

    def apply_function(self, dataframe_row):
        output = self.aggregation_function(dataframe_row, self.AvoidedCost, self.Settings, self.first_year)
        return output

class MultiprocessingCostTest(MultiprocessingApplier):
    programs = pd.DataFrame
    Settings = None
    first_year = None

    def __init__(self, measures, programs, Settings, first_year, test_function):
        self.set_threads()
        self.dataframe_chunks = np.array_split(measures,self.number_of_threads)
        self.programs = programs
        self.Settings = Settings
        self.first_year = first_year
        self.aggregation_function = test_function

    def apply_function(self, dataframe_row):
        output = self.aggregation_function(dataframe_row, self.programs, self.Settings, self.first_year)
        return output

class MultiprocessingRatepayerImpactMeasure(MultiprocessingApplier):
    RateScheduleElectric = None
    RateScheduleGas = None
    Settings = None
    first_year = None

    def __init__(self, measures, RateScheduleElectric, RateScheduleGas, Settings, first_year, ratepayer_impact_measure_function):
        self.set_threads()
        self.dataframe_chunks = np.array_split(measures,self.number_of_threads)
        self.RateScheduleElectric = RateScheduleElectric
        self.RateScheduleGas = RateScheduleGas
        self.Settings = Settings
        self.first_year = first_year
        self.aggregation_function = ratepayer_impact_measure_function

    def apply_function(self, dataframe_row):
        output = self.aggregation_function(dataframe_row, self.RateScheduleElectric, self.RateScheduleGas, self.Settings, self.first_year)
        return output

class MultiprocessingTable:
    data = pd.DataFrame()
    def __init__(self, data):
        self.data = data

class SettingsTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_settings = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator)
        )
        return filtered_settings

class AvoidedCostElectricTable(MultiprocessingTable):
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

class AvoidedCostGasTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == measure.GasSavingsProfile) & \
            (self.data.Qi >= measure.Qi + 1) & \
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_gas

class RateScheduleElectricTable(MultiprocessingTable):
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

class RateScheduleGasTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_gas_rates = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ApplicableYear * 4 >= measure.Qi) & \
            (self.data.ApplicableYear * 4 < measure.Qi + measure.EULq) & \
            (self.data.GasTargetSector == measure.GasTargetSector)
        )
        return filtered_gas_rates
