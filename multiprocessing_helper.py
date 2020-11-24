import os, numpy as np, pandas as pd

import multiprocessing as mp
import dill as pickle


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
        self.dataframe_chunks = np.array_split(measures, self.number_of_threads)
        self.AvoidedCost = AvoidedCost
        self.Settings = Settings
        self.first_year = first_year
        self.aggregation_function = avoided_cost_function

    def apply_function(self, measure):
        output = self.aggregation_function(measure, self.AvoidedCost, self.Settings, self.first_year)
        return output

class MultiprocessingEmissionsReductions(MultiprocessingApplier):
    AvoidedCostElectric = None
    Emissions = None
    CombustionTypes = None
    Settings = None
    
    def __init__(self, measures, AvoidedCostElectric, Emissions, CombustionTypes, Settings, emissions_reductions_function):
        self.set_threads()
        self.dataframe_chunks = np.array_split(measures, self.number_of_threads)
        self.AvoidedCostElectric = AvoidedCostElectric
        self.Emissions = Emissions
        self.CombustionTypes = CombustionTypes
        self.Settings = Settings
        self.aggregation_function = emissions_reductions_function

    def apply_function(self, measure):
        output = self.aggregation_function(measure, self.AvoidedCostElectric, self.Emissions, self.CombustionTypes, self.Settings)
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

    def apply_function(self, measure):
        output = self.aggregation_function(measure, self.programs, self.Settings, self.first_year)
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

    def apply_function(self, measure):
        output = self.aggregation_function(measure, self.RateScheduleElectric, self.RateScheduleGas, self.Settings, self.first_year)
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

class EmissionsTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_emissions = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == measure.ElectricEndUse) & \
            (self.data.ClimateZone == measure.ClimateZone)
        )
        return filtered_emissions

class CombustionTypesTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_combustion_type = self.data.get(
            self.data.LookupCode == measure.CombustionType
        )
        return filtered_combustion_type

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
