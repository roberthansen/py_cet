import numpy as np, pandas as pd

import multiprocessing as mp
import dill as pickle

from multiprocessing_helper import Table, MultiprocessingApplier, \
    MultiprocessingAvoidedCosts, MultiprocessingEmissionsReductions, \
    MultiprocessingCostTest, MultiprocessingRatepayerImpactMeasure, \
    MultiprocessingTable, SettingsTable, EmissionsTable, CombustionTypesTable, \
    RateScheduleElectricTable, RateScheduleGasTable\

class AvoidedCostElectricTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_avoided_costs_electric = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.ElectricTargetSector == measure.ElectricTargetSector) & \
            (self.data.ElectricEndUse == measure.ElectricEndUse) & \
            (self.data.ClimateZone == measure.ClimateZone) & \
            #INCLUDE EXTRA QUARTER TO MATCH SQL:
            (self.data.Qi >= measure.Qi) & \
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_electric

class AvoidedCostGasTable(MultiprocessingTable):
    def filter_by_measure(self, measure):
        filtered_avoided_costs_gas = self.data.get(
            (self.data.ProgramAdministrator == measure.ProgramAdministrator) & \
            (self.data.GasTargetSector == measure.GasTargetSector) & \
            (self.data.GasSavingsProfile == measure.GasSavingsProfile) & \
            #INCLUDE EXTRA QUARTER TO MATCH SQL:
            (self.data.Qi >= measure.Qi) & \
            (self.data.Qi < measure.Qi + measure.EULq + 1)
        )
        return filtered_avoided_costs_gas
