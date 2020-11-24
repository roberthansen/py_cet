import pandas as pd
import numpy as np

from quarter_calculations import present_value_bill_savings_electric, present_value_bill_savings_gas

# calculate the quarterly generation benefits per unit--installation and realization rates, and net-to-gross is handled in the calculate_avoided_electric_costs functions lower:
def present_value_generation_benefits(avoided_cost_electric, measure, settings, first_year):
    measure_install = measure.Qi
    measure_phase_1 = measure_install + measure.EULq1
    measure_phase_2 = measure_install + measure.EULq2

    #INCORRECT QUARTER INDEX TO MATCH SQL VERSION OF CET:
    avoided_cost_quarter = avoided_cost_electric.Qi - 1

    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_electric_savings_rate = measure.kWh1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = measure.EULq1 % 1
        annual_electric_savings_rate = \
            measure.kWh1 * quarter_fraction + \
            measure.kWh2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_electric_savings_rate = measure.kWh2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_electric_savings_rate = measure.kWh2 * quarter_fraction
    else:
        annual_electric_savings_rate = 0

    #INCORRECT PRESENT VALUE CALCULATION TO MATCH SQL VERSION OF CET:
    generation_benefits = annual_electric_savings_rate * avoided_cost_electric.Gen / quarterly_discount_rate ** ( avoided_cost_electric.Qac - 4 + 1 )

    return generation_benefits

def present_value_transmission_and_distribution_benefits(avoided_cost_electric, measure, settings, first_year):
    measure_install = measure.Qi
    measure_phase_1 = measure_install + measure.EULq1
    measure_phase_2 = measure_install + measure.EULq2

    #INCORRECT QUARTER INDEX TO MATCH SQL VERSION OF CET:
    avoided_cost_quarter = avoided_cost_electric.Qi - 1

    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if avoided_cost_electric.DSType == 'kWh':
        ds1 = measure.kWh1
        ds2 = measure.kWh2
    else:
        ds1 = measure.kW1
        ds2 = measure.kW2

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_demand_reduction = ds1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = measure.EULq1 % 1
        annual_demand_reduction = \
            ds1 * quarter_fraction + \
            ds2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_demand_reduction = ds2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_demand_reduction = ds2 * quarter_fraction
    else:
        annual_demand_reduction = 0

    #INCORRECT PRESENT VALUE CALCULATION TO MATCH SQL VERSION OF CET:
    transmission_and_distribution_benefits = (
        annual_demand_reduction *
        avoided_cost_electric.TD /
        quarterly_discount_rate ** ( avoided_cost_electric.Qac - 4 + 1 )
    )

    return transmission_and_distribution_benefits

def present_value_gas_benefits(avoided_cost_gas, measure, settings, first_year):
    measure_install = measure.Qi
    measure_phase_1 = measure_install + measure.EULq1
    measure_phase_2 = measure_install + measure.EULq2

    #INCORRECT QUARTER INDEX TO MATCH SQL VERSION OF CET:
    avoided_cost_quarter = avoided_cost_gas.Qi - 1

    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_gas_savings_rate = measure.Therm1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = measure.EULq1 % 1
        annual_gas_savings_rate = \
            measure.Therm1 * quarter_fraction + \
            measure.Therm2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_gas_savings_rate = measure.Therm2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_gas_savings_rate = measure.Therm2 * quarter_fraction
    else:
        annual_gas_savings_rate = 0

    #INCORRECT PRESENT VALUE CALCULATION TO MATCH SQL VERSION OF CET:
    gas_benefits = annual_gas_savings_rate * avoided_cost_gas.Cost / quarterly_discount_rate ** ( avoided_cost_gas.Qac - 4 + 1 )

    return gas_benefits

def emissions_reductions_electric(avoided_cost_electric, emissions, measure):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table',
    ###         'EDCS_Query_Results', or 'Local_CSV'
    ###     emissions : a single row from the 'data' variable of an
    ###         'Emissions' object of class 'EDCS_Table',
    ###         'EDCS_Query_Results', or 'Local_CSV'
    ###     measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table',
    ###         'EDCS_Query_Results', or 'Local_CSV'
    ###
    ### returns:
    ###     a dictionary containing the CO2 and NOx reductions due to
    ###     electric savings attributed to the measure

    emissions = emissions.iloc[0]

    measure_install = measure.Qi
    measure_phase_1 = measure.Qi + measure.EULq1
    measure_phase_2 = measure.Qi + measure.EULq2

    avoided_cost_quarter = avoided_cost_electric.Qi

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_electric_savings_rate = measure.kWh1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = measure.EULq1 % 1
        annual_electric_savings_rate = measure.kWh1 * quarter_fraction + measure.kWh2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_electric_savings_rate = measure.kWh2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_electric_savings_rate = measure.kWh2 * quarter_fraction
    else:
        annual_electric_savings_rate = 0

    emissions_reductions_electric = {
        'CO2'  : annual_electric_savings_rate * avoided_cost_electric.CO2,
        'NOx'  : annual_electric_savings_rate / 4 * emissions.NOx,
        'PM10' : annual_electric_savings_rate / 4 * emissions.PM10,
    }

    return emissions_reductions_electric

def emissions_reductions_gas(measure, combustion_type, settings):
    ###     measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     combustion_type : a single row from the 'data' variable of a
    ###         'CombustionTypes' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     settings : a single row from the 'data' variable of a 'Settings'
    ###         object of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###
    ### returns:
    ###     a dictionary containing the CO2, NOx, and PM10 reductions due to
    ###     natural gas savings attributed to the measure

    settings = settings.iloc[0]

    if combustion_type.size > 0:
        NOxGas = combustion_type.iloc[0].NOx
    else:
        NOxGas = 0

    if measure.RUL > 0:
        if measure.RUL >= 1:
            first_year_gas = measure.Therm1 * 1
        elif measure.EUL >=1:
            first_year_gas = measure.Therm1 * measure.RUL + measure.Therm2 * measure.EUL
        else:
            first_year_gas = measure.Therm1 * measure.RUL + measure.Therm2 * (measure.EUL - measure.RUL)
    elif measure.EUL > 0:
        if measure.EUL >= 1:
            first_year_gas = measure.Therm1 * 1
        else:
            first_year_gas = measure.Therm1 * measure.EUL
    else:
        first_year_gas = 0

    lifecycle_gas = measure.Therm1 * measure.EUL1 + measure.Therm2 * measure.EUL2

    emissions_reductions_gas = {
        'CO2FirstYear' : first_year_gas * settings.CO2Gas,
        'CO2Lifecycle' : lifecycle_gas * settings.CO2Gas,
        'NOxFirstYear' : first_year_gas * NOxGas,
        'NOxLifecycle' : lifecycle_gas * NOxGas,
    }

    return emissions_reductions_gas

def present_value_external_costs(measure, quarterly_discount_rate, first_year):
    present_value_external_costs = (
        measure.Quantity *
        measure[[
            'UnitIncentiveToOthers',
            'UnitLaborCost',
            'UnitMaterialsCost',
            'UnitEndUserRebate'
        ]].sum() /
        quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1 )
    )
    return present_value_external_costs

def present_value_gross_measure_cost(measure, quarterly_measure_inflation_rate, quarterly_discount_rate, first_year):
    if measure.RUL > 0:
        present_value_gross_measure_cost = (
            measure.Quantity *
            (
                measure.UnitGrossCost1 -
                (
                    measure.UnitGrossCost1 -
                    measure.UnitGrossCost2
                )*
                (
                    quarterly_measure_inflation_rate /
                    quarterly_discount_rate
                ) ** measure.RULq
            ) /
            quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1 )
        )
    else:
        present_value_gross_measure_cost = (
            measure.Quantity *
            measure.UnitGrossCost1 /
            quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1 )
        )
    return present_value_gross_measure_cost

def present_value_incentives_and_direct_installation(measure, quarterly_discount_rate, first_year):
    present_value_incentives_and_direct_installation = (
        measure.Quantity *
        measure[[
            'UnitIncentiveToOthers',
            'UnitLaborCost',
            'UnitMaterialsCost'
        ]].sum() /
        quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1 )
    )

    return present_value_incentives_and_direct_installation

def present_value_rebates(measure, quarterly_discount_rate, first_year):
    present_value_rebates = (
        measure.Quantity *
        measure.UnitEndUserRebate /
       quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1 )
    )
    return present_value_rebates
