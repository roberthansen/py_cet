import numpy as np
import pandas as pd

# calculate the quarterly generation benefits per unit--installation and realization rates, and net-to-gross is handled in the calculate_avoided_electric_costs functions in the aggregation file:
def present_value_generation_benefits(avoided_cost_electric, input_measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     settings : a single row from the 'data' variable of a 'Settings'
    ###         object of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###     first_year : an int representing the first year for the program
    ###         through which the input measure is implemented
    ###
    ### returns:
    ###     float with the calculated present value of generation benefits
    ###     attributed to the input measure for a single quarter of avoided
    ###     costs

    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    avoided_cost_quarter = avoided_cost_electric.Qi

    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_electric_savings_rate = input_measure.kWh1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = input_measure.EULq1 % 1
        annual_electric_savings_rate = input_measure.kWh1 * quarter_fraction + input_measure.kWh2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_electric_savings_rate = input_measure.kWh2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = input_measure.EULq2 % 1
        annual_electric_savings_rate = input_measure.kWh2 * quarter_fraction
    else:
        annual_electric_savings_rate = 0

    # correct generation_benefits calculation:
    generation_benefits = annual_electric_savings_rate * avoided_cost_electric.Gen / quarterly_discount_rate ** (avoided_cost_electric.Qi - first_year * 4)

    return generation_benefits

def present_value_transmission_and_distribution_benefits(avoided_cost_electric, input_measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     settings : a single row from the 'data' variable of a 'Settings'
    ###         object of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###     first_year : an int representing the first year for the program
    ###         through which the input measure is implemented
    ###
    ### returns:
    ###     float with the calculated present value of transmission and
    ###     distribution benefits attributed to the input measure for a single
    ###     quarter of avoided costs

    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    avoided_cost_quarter = avoided_cost_electric.Qi

    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if avoided_cost_electric.DSType == 'kWh':
        ds1 = input_measure.kWh1
        ds2 = input_measure.kWh2
    else:
        ds1 = input_measure.kW1
        ds2 = input_measure.kW2

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_demand_reduction = ds1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = input_measure.EULq1 % 1
        annual_demand_reduction = ds1 * quarter_fraction + ds2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_demand_reduction = ds2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = input_measure.EULq2 % 1
        annual_demand_reduction = ds2 * quarter_fraction
    else:
        annual_demand_reduction = 0

    transmission_and_distribution_benefits = annual_demand_reduction * avoided_cost_electric.TD / quarterly_discount_rate ** (avoided_cost_electric.Qi - first_year * 4)

    return transmission_and_distribution_benefits

def present_value_gas_benefits(avoided_cost_gas, input_measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_gas : a single row from the 'data' variable of an
    ###         'AvoidedCostGas' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     settings : a single row from the 'data' variable of a 'Settings'
    ###         object of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###     first_year : an int representing the first year for the program
    ###         through which the input measure is implemented
    ###
    ### returns:
    ###     float with the calculated present value of natural gas benefits 
    ###     attributed to the input measure for a single quarter of avoided
    ###     costs

    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    avoided_cost_quarter = avoided_cost_gas.Qi

    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_gas_savings_rate = input_measure.Thm1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = input_measure.EULq1 % 1
        annual_gas_savings_rate = input_measure.Thm1 * quarter_fraction + input_measure.Thm2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_gas_savings_rate = input_measure.Thm2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = input_measure.EULq2 % 1
        annual_gas_savings_rate = input_measure.Thm2 * quarter_fraction
    else:
        annual_gas_savings_rate = 0

    gas_benefits = annual_gas_savings_rate * avoided_cost_gas.Cost / quarterly_discount_rate ** (avoided_cost_gas.Qi - first_year * 4)

    return gas_benefits
