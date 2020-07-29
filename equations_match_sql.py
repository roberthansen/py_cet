import pandas as pd
import numpy as np

# calculate the quarterly generation benefits per unit--installation and realization rates, and net-to-gross is handled in the calculate_avoided_electric_costs functions lower:
def present_value_generation_benefits(avoided_cost_electric, input_measure, settings, first_year):
    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    #incorrect quarter index to match sql version of cet:
    avoided_cost_quarter = avoided_cost_electric.Qi - 1

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

    # incorrect generation_benefits to match sql version of cet:
    generation_benefits = annual_electric_savings_rate * avoided_cost_electric.Gen / quarterly_discount_rate ** ( avoided_cost_electric.Qac - 4 + 1 )

    return generation_benefits

def present_value_transmission_and_distribution_benefits(avoided_cost_electric, input_measure, settings, first_year):
    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    #incorrect quarter index to match sql version of cet:
    avoided_cost_quarter = avoided_cost_electric.Qi - 1

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

    # incorrect generation_benefits to match sql version of cet:
    transmission_and_distribution_benefits = annual_demand_reduction * avoided_cost_electric.TD / quarterly_discount_rate ** ( avoided_cost_electric.Qac - 4 + 1 )

    return transmission_and_distribution_benefits

def present_value_gas_benefits(avoided_cost_gas, input_measure, settings, first_year):
    measure_install = input_measure.Qi
    measure_phase_1 = input_measure.Qi + input_measure.EULq1
    measure_phase_2 = input_measure.Qi + input_measure.EULq2

    # correct quarter index:
    #avoided_cost_quarter = avoided_cost_gas.Qi

    #incorrect quarter index to match sql version of cet:
    avoided_cost_quarter = avoided_cost_gas.Qi - 1

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

    # incorrect generation_benefits to match sql version of cet:
    gas_benefits = annual_gas_savings_rate * avoided_cost_gas.Cost / quarterly_discount_rate ** ( avoided_cost_gas.Qac - 4 + 1 )

    return gas_benefits

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
    ###
    ### returns:
    ###     pandas Series containing calculated measure benefits due to avoided
    ###     electric costs

    # filter avoided cost table:
    avoided_cost_electric = AvoidedCostElectric.metadata_filter(input_measure)
    avoided_cost_electric_quarters_filter = (
        (avoided_cost_electric.Qi >= input_measure.Qi) & \
        (avoided_cost_electric.Qi <= input_measure.Qi + input_measure.EULq)
    )
    avoided_cost_electric = avoided_cost_electric.get(avoided_cost_electric_quarters_filter)

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
        input_measure[['Qty','IRkWh','RRkWh']].product() * pv_gen,
        input_measure[['Qty','IRkW','RRkW']].product() * pv_td,
        input_measure[['Qty','IRkWh','RRkWh']].product() * (input_measure.NTGRkWh + market_effects) * pv_gen,
        input_measure[['Qty','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * pv_td,
    ], index=['CET_ID','GenBenGross','TDBenGross','GenBenNet','TDBenNet'])

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

    # filter avoided cost table:
    avoided_cost_gas = AvoidedCostGas.metadata_filter(input_measure)
    avoided_cost_gas_quarters_filter = (
        (avoided_cost_gas.Qi >= input_measure.Qi) &
        (avoided_cost_gas.Qi <= input_measure.Qi + input_measure.EULq)
    )
    avoided_cost_gas = avoided_cost_gas.get(avoided_cost_gas_quarters_filter)

    if avoided_cost_gas.size > 0:
        # filter settings table:
        settings = Settings.metadata_filter(input_measure).iloc[0]

        f = lambda r: present_value_gas_benefits(r, input_measure, settings, first_year)
        pv_gas = avoided_cost_gas.apply(f,axis='columns').aggregate(np.sum)
    else:
        pv_gas = 0

    avoided_gas_costs = pd.Series([
        input_measure.CET_ID,
        input_measure[['Qty','IRThm','RRThm']].product() * pv_gas,
        input_measure[['Qty','IRThm','RRThm']].product() * (input_measure.NTGRkW + market_effects) * pv_gas,
    ], index=['CET_ID','GasBenGross','GasBenNet'])

    return avoided_gas_costs

def calculate_program_costs(input_program, Settings, first_year):
    # extract cost columns from input program:
    total_program_costs = input_program[[
        'AdminCostsOverheadAndGA',
        'MarketingOutreach',
        'DIActivity',
        'DIInstallation',
        'DIHardwareAndMaterials',
        'DIRebateAndInspection',
        'EMV',
        'UserInputIncentive',
        'CostsRecoveredFromOtherSources',
    ]].aggregate(np.sum)
    settings = Settings.metadata_filter(input_program).iloc[0]

    # get annual discount rate:
    annual_discount_rate = 1 + settings.DiscountRateAnnual

    # get program year from claim year string:
    program_year = int(input_program.ClaimYearQuarter.split('Q')[0])

    # calculate net present value of program costs:
    program_costs_npv = total_program_costs / annual_discount_rate ** (program_year - first_year)

    program_costs = pd.Series([
        input_program.PrgID,
        total_program_costs,
        program_costs_npv,
    ], index=['PrgID','SumCosts','SumCostsNPV'])

    return program_costs

def calculate_rebates_and_incentives(input_measure, Settings, first_year, annual_measure_inflation_rate=0):
    ### parameters:
    ###     input_measure : a single row from the 'data' variable of an
    ###         'InputMeasures' object of class 'EDCS_Table' or
    ###         'EDCS_Query_Results'
    ###     Settings : an instance of a 'Settings' object of class 'EDCS_Table'
    ###         or 'EDCS_Query_Results'
    ###     first_year : an int representing the first year of programs in a cet
    ###         run
    ###     annual_measure_inflation_rate : a float representing the annual
    ###         inflaction rate by which the cost of a measure is expected to
    ###         rise or fall, i.e., cost*(1+r)^y
    ###
    ### returns:
    ###     pandas Series containing calculated measure costs

    # get filtered settings table:
    settings = Settings.metadata_filter(input_measure).iloc[0]

    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    quarterly_measure_inflation_rate = 1 + annual_measure_inflation_rate / 4

    # incentive cost in excess of gross cost:
    excess_incentive_present_value = input_measure.Qty * (input_measure.IncentiveToOthers + input_measure.DILaborCost - input_measure.UnitMeasureGrossCost)

    # present value of rebates to end-user:
    rebates_present_value = input_measure.Qty * input_measure.EndUserRebate / ( quarterly_discount_rate ** (input_measure.Qi - first_year * 4))

    # present value of up- and mid-stream incentives and direct installation costs:
    incentives_and_direct_installation_present_value = input_measure.Qty * input_measure[['IncentiveToOthers','DILaborCost','DIMaterialCost']].aggregate(np.sum) / ( quarterly_discount_rate ** (input_measure.Qi - first_year * 4))

    # present value of measure costs -- based on gross measure cost at time of installation less standard replacement cost at expected time-of-burnout (rul after install date):
    measure_cost_gross_present_value = input_measure.Qty * ( input_measure.UnitMeasureGrossCost - (input_measure.UnitMeasureGrossCost - input_measure.UnitMeasureGrossCost_ER) * (quarterly_measure_inflation_rate / quarterly_discount_rate) ** input_measure.RULq) / (quarterly_discount_rate ** (input_measure.Qi - first_year * 4))

    # incremental cost of all measures installed:
    measure_incremental_cost = input_measure.Qty * input_measure.UnitMeasureGrossCost_ER

    rebates_and_incentives = pd.Series([
        input_measure.CET_ID,
        input_measure.PrgID,
        input_measure.NTGRCost,
        excess_incentive_present_value,
        rebates_present_value,
        incentives_and_direct_installation_present_value,
        measure_cost_gross_present_value,
        measure_incremental_cost,
    ], index=['CET_ID','PrgID','NTGRCost','ExcessIncPV','RebatesPV','IncentsAndDIPV','GrossMeasCostPV','MeasureIncCost'])
    return rebates_and_incentives 
