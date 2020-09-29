import numpy as np
import pandas as pd

# calculate the quarterly generation benefits per unit--installation and realization rates, and net-to-gross is handled in the calculate_avoided_electric_costs functions in the aggregation file:
def present_value_generation_benefits(avoided_cost_electric, measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     measure : a single row from the 'data' variable of an
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

    measure_install = measure.Qi
    measure_phase_1 = measure.Qi + measure.EULq1
    measure_phase_2 = measure.Qi + measure.EULq2

    avoided_cost_quarter = avoided_cost_electric.Qi

    quarterly_discount_rate = 1 + settings.DiscountRateQtr

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

    # correct generation_benefits calculation:
    generation_benefits = annual_electric_savings_rate * avoided_cost_electric.Gen / quarterly_discount_rate ** (avoided_cost_electric.Qi - first_year * 4)

    return generation_benefits

def present_value_transmission_and_distribution_benefits(avoided_cost_electric, measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     measure : a single row from the 'data' variable of an
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

    measure_install = measure.Qi
    measure_phase_1 = measure.Qi + measure.EULq1
    measure_phase_2 = measure.Qi + measure.EULq2

    avoided_cost_quarter = avoided_cost_electric.Qi

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
        annual_demand_reduction = ds1 * quarter_fraction + ds2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_demand_reduction = ds2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_demand_reduction = ds2 * quarter_fraction
    else:
        annual_demand_reduction = 0

    transmission_and_distribution_benefits = annual_demand_reduction * avoided_cost_electric.TD / quarterly_discount_rate ** (avoided_cost_electric.Qi - first_year * 4)

    return transmission_and_distribution_benefits

def present_value_gas_benefits(avoided_cost_gas, measure, settings, first_year):
    ### parameters:
    ###     avoided_cost_gas : a single row from the 'data' variable of an
    ###         'AvoidedCostGas' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     measure : a single row from the 'data' variable of an
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

    measure_install = measure.Qi
    measure_phase_1 = measure.Qi + measure.EULq1
    measure_phase_2 = measure.Qi + measure.EULq2

    avoided_cost_quarter = avoided_cost_gas.Qi

    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    if measure_install <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_1 - 1:
        annual_gas_savings_rate = measure.Therm1
    elif measure_phase_1 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_1:
        quarter_fraction = measure.EULq1 % 1
        annual_gas_savings_rate = measure.Therm1 * quarter_fraction + measure.Therm2 * (1 - quarter_fraction)
    elif measure_phase_1 <= avoided_cost_quarter and avoided_cost_quarter <= measure_phase_2 - 1:
        annual_gas_savings_rate = measure.Therm2
    elif measure_phase_2 - 1 < avoided_cost_quarter and avoided_cost_quarter < measure_phase_2:
        quarter_fraction = measure.EULq2 % 1
        annual_gas_savings_rate = measure.Therm2 * quarter_fraction
    else:
        annual_gas_savings_rate = 0

    gas_benefits = annual_gas_savings_rate * avoided_cost_gas.Cost / quarterly_discount_rate ** (avoided_cost_gas.Qi - first_year * 4)

    return gas_benefits

def emissions_reductions_electric(avoided_cost_electric, emissions, measure):
    ### parameters:
    ###     avoided_cost_electric : a single row from the 'data' variable of an
    ###         'AvoidedCostElectric' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     emissions : a single row from the 'data' variable of an
    ###         'Emissions' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
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
        'NOx'  : annual_electric_savings_rate * emissions.NOx,
        'PM10' : annual_electric_savings_rate * emissions.PM10,
    }

    return emissions_reductions_electric

def emissions_reductions_gas(measure, settings):
    ###     measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     settings : a single row from the 'data' variable of a 'Settings'
    ###         object of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###
    ### returns:
    ###     a dictionary containing the CO2, NOx, and PM10 reductions due to
    ###     natural gas savings attributed to the measure

    settings = settings.iloc[0]

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
        'NOxFirstYear' : first_year_gas * settings.NOxGas,
        'NOxLifecycle' : lifecycle_gas * settings.NOxGas,
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
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
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
                ) *
                (
                    quarterly_measure_inflation_rate /
                    quarterly_discount_rate
                ) ** measure.RULq
            ) /
            quarterly_discount_rate ** ( measure.Qi - first_year * 4 )
        )
    else:
        present_value_gross_measure_cost = (
            measure.Quantity * 
            quarterly_discount_rate ** ( measure.Qi - 4 * first_year + 1)
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
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )

    return present_value_incentives_and_direct_installation

def present_value_rebates(measure, quarterly_discount_rate, first_year):
    present_value_rebates = (
        measure.Quantity *
        measure.UnitEndUserRebate /
       quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )
    return present_value_rebates

def present_value_excess_incentives(measure, quarterly_discount_rate, first_year):
    present_value_excess_incentives = (
        measure.Quantity *
        (
            measure.UnitIncentiveToOthers +
            measure.UnitLaborCost +
            measure.UnitMaterialsCost -
            measure.UnitGrossCost1
        ) /
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )
    return max(present_value_excess_incentives, 0)

def present_value_bill_savings_electric(rate_schedule_electric, measure, settings, first_year):
    measure_install_year = np.floor(measure.Qi / 4)
    measure_phase_1 = measure_install_year + measure.EUL1
    measure_phase_2 = measure_install_year + measure.EUL2

    rate_schedule_year = rate_schedule_electric.ApplicableYear

    annual_discount_rate = 1 + settings.DiscountRateAnnual

    if measure_install_year <= rate_schedule_year and rate_schedule_year <= measure_phase_1 - 1:
        annual_electric_savings_rate = measure.kWh1
    elif measure_phase_1 - 1 < rate_schedule_year and rate_schedule_year < measure_phase_1:
        year_fraction = measure.EUL1 % 1
        annual_electric_savings_rate = measure.kWh1 * year_fraction + measure.kWh2 * (1 - year_fraction)
    elif measure_phase_1 <= rate_schedule_year and rate_schedule_year <= measure_phase_2 - 1:
        annual_electric_savings_rate = measure.kWh2
    elif measure_phase_2 - 1 < rate_schedule_year and rate_schedule_year < measure_phase_2:
        year_fraction = measure.EUL2 % 1
        annual_electric_savings_rate = measure.kWh2 * year_fraction
    else:
        annual_electric_savings_rate = 0

    present_value_bill_savings_electric = (
        annual_electric_savings_rate *
        rate_schedule_electric.ElectricRate /
        annual_discount_rate ** (rate_schedule_year - first_year)
    )

    return present_value_bill_savings_electric

def present_value_bill_savings_gas(rate_schedule_gas, measure, settings, first_year):
    measure_install_year = np.floor(measure.Qi / 4)
    measure_phase_1 = measure_install_year + measure.EUL1
    measure_phase_2 = measure_install_year + measure.EUL2

    rate_schedule_year = rate_schedule_gas.ApplicableYear

    annual_discount_rate = 1 + settings.DiscountRateAnnual

    if measure_install_year <= rate_schedule_year and rate_schedule_year <= measure_phase_1 - 1:
        annual_gas_savings_rate = measure.Therm1
    elif measure_phase_1 - 1 < rate_schedule_year and rate_schedule_year < measure_phase_1:
        year_fraction = measure.EUL1 % 1
        annual_gas_savings_rate = measure.Therm1 * year_fraction + measure.Therm2 * (1 - year_fraction)
    elif measure_phase_1 <= rate_schedule_year and rate_schedule_year <= measure_phase_2 - 1:
        annual_gas_savings_rate = measure.Therm2
    elif measure_phase_2 - 1 < rate_schedule_year and rate_schedule_year < measure_phase_2:
        year_fraction = measure.EUL2 % 1
        annual_gas_savings_rate = measure.Therm2 * year_fraction
    else:
        annual_gas_savings_rate = 0

    present_value_bill_savings_gas = (
        annual_gas_savings_rate *
        rate_schedule_gas.GasRate /
        annual_discount_rate ** (rate_schedule_year - first_year)
    )

    return present_value_bill_savings_gas
