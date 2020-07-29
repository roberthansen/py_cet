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
    avoided_cost_electric = AvoidedCostElectric.data.get(
        (AvoidedCostElectric.data.PA == input_measure.PA) & \
        (AvoidedCostElectric.data.TS == input_measure.TS) & \
        (AvoidedCostElectric.data.EU == input_measure.EU) & \
        (AvoidedCostElectric.data.CZ == input_measure.CZ) & \
        (AvoidedCostElectric.data.Qi - 1 >= input_measure.Qi) & \
        (AvoidedCostElectric.data.Qi - 1 < input_measure.Qi + input_measure.EULq)
    )

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
        input_measure[['Qty','IRkW','RRkW']].product() * ( pv_gen + pv_td ),
        input_measure[['Qty','IRkWh','RRkWh']].product() * (input_measure.NTGRkWh + market_effects) * pv_gen,
        input_measure[['Qty','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * pv_td,
        input_measure[['Qty','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * ( pv_gen + pv_td ),
    ], index=['CET_ID','GenBenGross','TDBenGross','ElecBenGross','GenBenNet','TDBenNet','ElecBenNet'])

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
    avoided_cost_gas = AvoidedCostGas.data.get(
        (AvoidedCostGas.data.PA == input_measure.PA) & \
        (AvoidedCostGas.data.GS == input_measure.GS) & \
        (AvoidedCostGas.data.GP == input_measure.GP) & \
        (AvoidedCostGas.data.Qi - 1 >= input_measure.Qi) & \
        (AvoidedCostGas.data.Qi - 1 < input_measure.Qi + input_measure.EULq)
    )

    if avoided_cost_gas.size > 0:
        # filter settings table for calculations based on sql version of cet:
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
