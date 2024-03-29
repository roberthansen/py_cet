import numpy as np
import pandas as pd
import tables, tables_match_sql

from calc import calculate_measure_cost_effectiveness, \
    calculate_program_cost_effectiveness, \
    calculate_portfolio_cost_effectiveness

import os
import multiprocessing as mp
import dill as pickle

class CET_Scenario:
    __acc_tbl_names__ = {
        2013 : {
            'electric':'E3AvoidedCostElecSeq2013',
            'gas':'E3AvoidedCostGasSeq2013'
        },
        2018 : {
            'electric':'E3AvoidedCostElecCO2Seq2018',
            'gas':'E3AvoidedCostGasSeq2018'
        },
        2019 : {
            'electric':'E3AvoidedCostElecCO2Seq2019',
            'gas':'E3AvoidedCostGasSeq2019'
        },
        2020 : {
            'electric':'E3AvoidedCostElecCO2Seq2020',
            'gas':'E3AvoidedCostGasSeq2020'
        },
    }
    user = None
    acc_source = ''
    acc_version = None
    acc_source_directory = ''
    first_year = None
    market_effects_benefits = 0
    market_effects_costs = 0
    inputs_source = ''
    input_measures_source_name = ''
    input_programs_source_name = ''
    InputMeasures = None
    InputProgram = None
    Settings = None
    Emissions = None
    CombustionTypes = None
    AvoidedCostElectric = None
    AvoidedCostGas = None
    RateScheduleElectric = None
    RateScheduleGas = None
    OutputMeasures = None
    OutputPrograms = None
    OutputPortfolio = None
    calculation_times = {
        'avoided_electric_costs'          : None,
        'avoided_gas_costs'               : None,
        'emissions_reductions'            : None,
        'total_resource_cost_test'        : None,
        'program_administrator_cost_test' : None,
        'ratepayer_impact_measure'        : None,
    }
    __parallelize__ = False
    __match_sql__ = False
    __tbl__ = None

    def __init__(
            self,
            user = {'id':'','passwd':''},
            inputs_source = 'database',
            input_measures_source_name = 'InputMeasureCEDARS',
            input_programs_source_name = 'InputProgramCEDARS',
            acc_source = 'database',
            acc_source_directory = '',
            acc_version = 2018,
            first_year = 2018,
            market_effects_benefits = 0.05,
            market_effects_costs = 0.05,
            parallelize = False,
            match_sql = False
    ):
        self.user = user
        self.inputs_source = inputs_source
        self.input_measures_source_name = input_measures_source_name
        self.input_programs_source_name = input_programs_source_name
        self.acc_source = acc_source
        self.acc_source_directory = acc_source_directory
        self.acc_version = acc_version
        self.first_year = first_year
        self.market_effects_benefits = market_effects_benefits
        self.market_effects_costs = market_effects_costs
        self.set_parallelize(parallelize)
        self.set_match_sql(match_sql)

        self.retrieve_all_tables()
        self.setup_output_measures()
        self.setup_output_programs()
        self.setup_output_portfolio()

    def setup_input_measures(self):
        self.InputMeasures = \
            self.__tbl__.setup_input_measures(
                self.inputs_source,
                self.input_measures_source_name,
                self.first_year,
                self.market_effects_benefits,
                self.market_effects_costs,
                self.user
            )

    def setup_input_programs(self):
        self.InputPrograms = \
            self.__tbl__.setup_input_programs(
                self.inputs_source,
                self.input_programs_source_name,
                self.user
            )

    def setup_settings(self):
        if self.acc_source == 'csv':
            source_name = self.acc_source_directory + 'E3Settings.csv'
        else:
            source_name = 'E3Settings'
        self.Settings = \
            self.__tbl__.setup_settings(
                self.acc_source,
                source_name,
                self.acc_version,
                self.InputMeasures,
                self.user
            )

    def setup_emissions(self):
        if self.acc_source == 'csv':
            if self.acc_version == 2013:
                source_name = self.acc_source_directory + 'E3Emissions2013.csv'
            else:
                source_name = self.acc_source_directory + 'E3Emissions.csv'
        else:
            if self.acc_version == 2013:
                source_name = 'E3Emissions2013'
            else:
                source_name = 'E3Emissions'
        self.Emissions = \
            self.__tbl__.setup_emissions(
                self.acc_source,
                source_name,
                self.acc_version,
                self.InputMeasures,
                self.user
            )

    def setup_combustion_types(self):
        if self.acc_source=='csv':
            source_name = self.acc_source_directory + 'E3CombustionType.csv'
        else:
            source_name = 'E3CombustionType'
        self.CombustionTypes = \
            self.__tbl__.setup_combustion_types(
                self.acc_source,
                source_name,
                self.InputMeasures,
                self.user
            )

    def setup_avoided_cost_electric(self):
        if self.acc_source == 'csv':
            source_name = (
                self.acc_source_directory +
                self.__acc_tbl_names__[self.acc_version]['electric'] +
                '.csv'
            )
        else:
            source_name = self.__acc_tbl_names__[self.acc_version]['electric']
        self.AvoidedCostElectric = \
            self.__tbl__.setup_avoided_cost_electric(
                self.acc_source,
                source_name,
                self.InputMeasures,
                self.user
            )

    def setup_avoided_cost_gas(self):
        if self.acc_source == 'csv':
            source_name = (
                self.acc_source_directory +
                self.__acc_tbl_names__[self.acc_version]['gas'] +
                '.csv'
            )
        else:
            source_name = self.__acc_tbl_names__[self.acc_version]['gas']
        self.AvoidedCostGas = \
            self.__tbl__.setup_avoided_cost_gas(
                self.acc_source,
                source_name,
                self.InputMeasures,
                self.user
            )

    def setup_rate_schedule_electric(self):
        if self.acc_source == 'csv':
            source_name = (
                self.acc_source_directory +
                'E3RateScheduleElec.csv'
            )
        else:
            source_name = 'E3RateScheduleElec'
        self.RateScheduleElectric = \
            self.__tbl__.setup_rate_schedule_electric(
                self.acc_source,
                source_name,
                self.acc_version,
                self.InputMeasures,
                self.user
            )

    def setup_rate_schedule_gas(self):
        if self.acc_source == 'csv':
            source_name = (
                self.acc_source_directory +
                'E3RateScheduleGas.csv'
            )
        else:
            source_name = 'E3RateScheduleGas'
        self.RateScheduleGas = \
            self.__tbl__.setup_rate_schedule_gas(
                self.acc_source,
                source_name,
                self.acc_version,
                self.InputMeasures,
                self.user
            )

    def retrieve_all_tables(self):
        self.setup_input_measures()
        self.setup_input_programs()
        self.setup_settings()
        self.setup_emissions()
        self.setup_combustion_types()
        self.setup_avoided_cost_electric()
        self.setup_avoided_cost_gas()
        self.setup_rate_schedule_electric()
        self.setup_rate_schedule_gas()


    def setup_output_measures(self):
        self.OutputMeasures = \
            self.__tbl__.setup_output_measures()

    def setup_output_programs(self):
        self.OutputPrograms = \
            self.__tbl__.setup_output_programs()

    def setup_output_portfolio(self):
        self.OutputPortfolio = \
            self.__tbl__.setup_output_portfolio()

    # calculate measure-level benefits and costs:
    def run_cet(self):
        print('< Calculating Cost Effectiveness ... >')
        self.OutputMeasures.data = calculate_measure_cost_effectiveness(self)
        self.OutputPrograms.data = calculate_program_cost_effectiveness(self)
        self.OutputPortfolio.data = calculate_portfolio_cost_effectiveness(self)

    def set_parallelize(self,b):
        self.__parallelize__ = b

    @property
    def parallelize(self):
        return self.__parallelize__

    def set_match_sql(self,b):
        self.__match_sql__ = b
        if self.__match_sql__:
            self.__tbl__ = tables_match_sql
        else:
            self.__tbl__ = tables

    @property
    def match_sql(self):
        return self.__match_sql__

    def __str__(self):
        scenario_information = 'CET_Scenario\nACC Version:\t{}' \
            '\nFirst Year:\t{}\nMarket Effects Benefits:\t{}' \
            '\nMarket Effects Costs:\t{}\nInput Measures:\t{}' \
            '\nInput Programs:\t{}'.format(
                self.acc_version,
                self.first_year,
                self.market_effects_benefits,
                self.market_effects_costs,
                len(self.InputMeasures.data.index),
                len(self.InputPrograms.data.index)
            )
        return scenario_information
