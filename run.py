import numpy as np
import pandas as pd
import tables, aggregation, aggregation_match_sql

from calc import calculate_measure_cost_effectiveness, calculate_program_cost_effectiveness

import os
import multiprocessing as mp
import dill as pickle

class CET_Scenario:
    _acc_tables = {
        2013 : {'electric':'E3AvoidedCostElecSeq2013','gas':'E3AvoidedCostGasSeq2013'},
        2018 : {'electric':'E3AvoidedCostElecCO2Seq2018','gas':'E3AvoidedCostGasSeq2018'},
        2019 : {'electric':'E3AvoidedCostElecCO2Seq2019','gas':'E3AvoidedCostGasSeq2019'},
        2020 : {'electric':'E3AvoidedCostElecCO2Seq2020','gas':'E3AvoidedCostGasSeq2020'},
    }
    acc_version = None
    first_year = None
    market_effects = 0
    InputMeasures = None
    InputProgram = None
    Settings = None
    AvoidedCostElectric = None
    AvoidedCostGas = None
    OutputMeasures = None
    OutputPrograms = None
    parallelize = False
    match_sql = False
    
    def __init__(self,acc_version=2018,first_year=2018,market_effects=0.05,parallelize=False,match_sql=False):
        self.acc_version = acc_version
        self.first_year = first_year
        self.market_effects = market_effects
        self.parallelize = parallelize
        self.match_sql = match_sql

        self.setup_input_measures()
        self.setup_input_programs()
        self.setup_settings()
        self.setup_avoided_cost_electric()
        self.setup_avoided_cost_gas()
        self.setup_outputs()
        
    def setup_input_measures(self):
        self.InputMeasures = tables.setup_input_measures(self.first_year)

    def setup_input_programs(self):
        self.InputPrograms = tables.setup_input_programs()

    def setup_settings(self):
        self.Settings = tables.setup_settings(self.acc_version,self.InputMeasures)

    def setup_avoided_cost_electric(self):
        self.AvoidedCostElectric = tables.setup_avoided_cost_electric(self._acc_tables[self.acc_version]['electric'],self.InputMeasures)

    def setup_avoided_cost_gas(self):
        self.AvoidedCostGas = tables.setup_avoided_cost_gas(self._acc_tables[self.acc_version]['gas'],self.InputMeasures)

    def setup_outputs(self):
        self.OutputCE = tables.setup_outputs()

    # calculate measure-level benefits and costs:
    def run_cet(self):
        measure_cost_effectiveness = calculate_measure_cost_effectiveness(self)
        self.OutputCE = measure_cost_effectiveness

        program_cost_effectiveness = calculate_program_cost_effectiveness
        self.OutputProgram = program_cost_effectiveness

    def __str__(self):
        return 'CET_Scenario\nACC Version:\t{}\nFirst Year:\t{}\nMarket Effects:\t{}\nInput Measures:\t{}\nInput Programs:\t{}'.format(self.acc_version, self.first_year, self.market_effects, len(self.InputMeasures.data.index), len(self.InputPrograms.data.index))

