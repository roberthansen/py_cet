import numpy as np
import pandas as pd

from equations_match_sql import present_value_generation_benefits
from equations_match_sql import present_value_transmission_and_distribution_benefits
from equations_match_sql import present_value_gas_benefits

def calculate_avoided_electric_costs(input_measure, AvoidedCostElectric, Settings, first_year, market_effects):
    ### parameters:
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     AvoidedCostElectric : an instance of an 'AvoidedCostElectric' object
    ###         of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###     Settings : an instance of a 'Settings' object of class 'EDCS_Table'
    ###         or 'EDCS_Query_Results'
    ###     market_effects : a float representing the allowed market effects for
    ###         the program
    ###     first_year : an int representing the first year for the program
    ###         through which the input measure is implemented
    ###
    ### returns:
    ###     pandas Series containing calculated measure benefits due to avoided
    ###     electric costs

    # filter avoided cost table for calculations based on sql version of cet:
    avoided_cost_electric = AvoidedCostElectric.metadata_filter(input_measure)

    if avoided_cost_electric.size > 0:
        # filter settings table:
        settings = Settings.metadata_filter(input_measure).iloc[0]

        f = lambda r: present_value_generation_benefits(r, input_measure, settings, first_year)
        pv_gen = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)

        f = lambda r: present_value_transmission_and_distribution_benefits(r, input_measure, settings, first_year)
        pv_td = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)
    else:
        pv_gen = 0
        pv_td = 0
    
    avoided_electric_costs = pd.Series([
        input_measure.CET_ID,
        input_measure.PrgID,
        input_measure.Qi,
        input_measure[['Qty','IRkWh','RRkWh']].product() * pv_gen,
        input_measure[['Qty','IRkW','RRkW']].product() * pv_td,
        input_measure[['Qty','IRkW','RRkW']].product() * ( pv_gen + pv_td ),
        input_measure[['Qty','IRkWh','RRkWh']].product() * (input_measure.NTGRkWh + market_effects) * pv_gen,
        input_measure[['Qty','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * pv_td,
        input_measure[['Qty','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * ( pv_gen + pv_td ),
    ], index=['CET_ID','PrgID','Qi','GenBenGross','TDBenGross','ElecBenGross','GenBenNet','TDBenNet','ElecBenNet'])

    return avoided_electric_costs

def calculate_avoided_gas_costs(input_measure, AvoidedCostGas, Settings, first_year, market_effects):
    ### parameters:
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     AvoidedCostGas : an instance of an 'AvoidedCostGas' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
    ###     Settings : an instance of a 'Settings' object of class 'EDCS_Table'
    ###         or 'EDCS_Query_Results'
    ###     market_effects : a float representing the allowed market effects for
    ###         the program
    ###
    ### returns:
    ###     pandas Series containing calculated measure benefits due to avoided
    ###     gas costs

    # filter avoided cost table for calculations based on sql version of cet:
    avoided_cost_gas = AvoidedCostGas.metadata_filter(input_measure)

    if avoided_cost_gas.size > 0:
        # filter settings table for calculations based on sql version of cet:
        settings = Settings.metadata_filter(input_measure).iloc[0]

        f = lambda r: present_value_gas_benefits(r, input_measure, settings, first_year)
        pv_gas = avoided_cost_gas.apply(f,axis='columns').aggregate(np.sum)
    else:
        pv_gas = 0

    avoided_gas_costs = pd.Series([
        input_measure.CET_ID,
        input_measure.PrgID,
        input_measure.Qi,
        input_measure[['Qty','IRThm','RRThm']].product() * pv_gas,
        input_measure[['Qty','IRThm','RRThm']].product() * (input_measure.NTGRkW + market_effects) * pv_gas,
    ], index=['CET_ID','PrgID','Qi','GasBenGross','GasBenNet'])

    return avoided_gas_costs

