import numpy as np
import pandas as pd
import tables, tables_match_sql, aggregation, aggregation_match_sql

from calc import calculate_measure_cost_effectiveness, calculate_program_cost_effectiveness

import os
import multiprocessing as mp
import dill as pickle

class CET_Scenario:
    __acc_tbl_names__ = {
        2013 : {'electric':'E3AvoidedCostElecSeq2013','gas':'E3AvoidedCostGasSeq2013'},
        2018 : {'electric':'E3AvoidedCostElecCO2Seq2018','gas':'E3AvoidedCostGasSeq2018'},
        2019 : {'electric':'E3AvoidedCostElecCO2Seq2019','gas':'E3AvoidedCostGasSeq2019'},
        2020 : {'electric':'E3AvoidedCostElecCO2Seq2020','gas':'E3AvoidedCostGasSeq2020'},
    }
    user = None
    acc_version = None
    first_year = None
    market_effects = 0
    InputMeasures = None
    InputProgram = None
    Settings = None
    AvoidedCostElectric = None
    AvoidedCostGas = None
    RateScheduleElectric = None
    RateScheduleGas = None
    OutputMeasures = None
    OutputPrograms = None
    parallelize = False
    __match_sql__ = False
    __tbl__ = None
    
    def __init__(self,user,acc_version=2018,first_year=2018,market_effects=0.05,parallelize=False,match_sql=False):
        self.user = user
        self.acc_version = acc_version
        self.first_year = first_year
        self.market_effects = market_effects
        self.parallelize = parallelize
        self.set_match_sql(match_sql)

        self.setup_input_measures()
        self.setup_input_programs()
        self.setup_settings()
        self.setup_avoided_cost_electric()
        self.setup_avoided_cost_gas()
        self.setup_rate_schedule_electric()
        self.setup_rate_schedule_gas()
        self.setup_cost_effectiveness_outputs()
        self.setup_program_outputs()
        
    def setup_input_measures(self):
        self.InputMeasures = self.__tbl__.setup_input_measures(self.first_year, self.user)

    def setup_input_programs(self):
        self.InputPrograms = self.__tbl__.setup_input_programs(self.user)

    def setup_settings(self):
        self.Settings = self.__tbl__.setup_settings(self.acc_version, self.InputMeasures, self.user)

    def setup_avoided_cost_electric(self):
        self.AvoidedCostElectric = self.__tbl__.setup_avoided_cost_electric(self.__acc_tbl_names__[self.acc_version]['electric'], self.InputMeasures, self.user)

    def setup_avoided_cost_gas(self):
        self.AvoidedCostGas = self.__tbl__.setup_avoided_cost_gas(self.__acc_tbl_names__[self.acc_version]['gas'], self.InputMeasures, self.user)

    def setup_rate_schedule_electric(self):
        self.RateScheduleElectric = self.__tbl__.setup_rate_schedule_electric(self.acc_version, self.InputMeasures, self.user)

    def setup_rate_schedule_gas(self):
        self.RateScheduleGas = self.__tbl__.setup_rate_schedule_gas(self.acc_version, self.InputMeasures, self.user)

    def setup_cost_effectiveness_outputs(self):
        self.OutputMeasures = self.__tbl__.setup_cost_effectiveness_outputs(self.user)

    def setup_program_outputs(self):
        self.OutputPrograms = self.__tbl__.setup_program_outputs(self.user)

    # calculate measure-level benefits and costs:
    def run_cet(self):
        self.OutputMeasures.data = calculate_measure_cost_effectiveness(self)
        self.OutputPrograms.data = calculate_program_cost_effectiveness(self)

    def set_match_sql(self,b):
        self.__match_sql__ = b
        if self.__match_sql__:
            self.__tbl__ = tables_match_sql
        else:
            self.__tbl__ = tables

    def match_sql(self):
        return self.__match_sql__

    def __str__(self):
        return 'CET_Scenario\nACC Version:\t{}\nFirst Year:\t{}\nMarket Effects:\t{}\nInput Measures:\t{}\nInput Programs:\t{}'.format(self.acc_version, self.first_year, self.market_effects, len(self.InputMeasures.data.index), len(self.InputPrograms.data.index))
