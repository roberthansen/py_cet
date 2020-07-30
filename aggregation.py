import numpy as np
import pandas as pd

from equations import present_value_generation_benefits
from equations import present_value_transmission_and_distribution_benefits
from equations import present_value_gas_benefits

def calculate_avoided_electric_costs(input_measure, AvoidedCostElectric, Settings, first_year, market_effects):
    ### parameters:
    ###     input_measure : a pandas Series containing a single row from the
    ###         'data' pandas DataFrame in an 'InputMeasures' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
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

    # filter avoided cost table:
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
    ###     input_measure : a pandas Series containing a single row from the
    ###         'data' pandas DataFrame in an 'InputMeasures' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
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

    if avoided_cost_gas.size > 0:
        # filter settings table:
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
    measure_gross_incremental_cost = input_measure.Qty * input_measure.UnitMeasureGrossCost_ER

    rebates_and_incentives = pd.Series([
        input_measure.CET_ID,
        input_measure.PrgID,
        input_measure.NTGRCost,
        excess_incentive_present_value,
        rebates_present_value,
        incentives_and_direct_installation_present_value,
        measure_cost_gross_present_value,
        measure_gross_incremental_cost,
    ], index=['CET_ID','PrgID','NTGRCost','ExcessIncPV','RebatesPV','IncentsAndDIPV','GrossMeasCostPV','MeasureIncCost'])
    return rebates_and_incentives 

def calculate_total_resource_costs(measure, programs, Settings, first_year):
    ### parameters:
    ###     measure: a pandas Series containing a single row from a pandas
    ###         DataFrame representing a single input measure and corresponding
    ###         calculated avoided costs
    ###     programs : a pandas Series containing summed measure benefits
    ###         rolled up at the program level along with program costs
    ###     Settings : an instance of a 'Settings' object of class 'EDCS_Table'
    ###         or 'EDCS_Query_Results'
    ###     first_year : an int representing the first year of programs in a cet
    ###         run
    ###
    ### outputs:
    ###     float value of the total resource cost for the given measure
    
    # filter programs based on measure, both subtotals for measure's installation quarter and totals for all quarters:
    sum_columns = ['PrgID','Count','ElectricGross','ElectricNet','GasGross','GasNet']
    program = programs.get(programs.PrgID == measure.PrgID)
    program_quarter = programs.get((programs.PrgID == measure.PrgID) & (programs.Qi == measure.Qi)).iloc[0]
    program_total = programs.get(programs.PrgID == measure.PrgID)[sum_columns].groupby('PrgID').aggregate(np.sum).iloc[0]

    # filter settings based on measure:
    settings = Settings.metadata_filter(measure).iloc[0]

    # get quarterly discount rate for exponentiation:
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    # get measure inflation rate if present:
    try:
        quarterly_measure_inflation_rate = 1 + measure.MeasInflation / 4
    except:
        quarterly_measure_inflation_rate = 1.0

    # calculate future and present values of program-level costs:
    program_cost_columns = [
        'AdminCostsOverheadAndGA',
        'AdminCostsOther',
        'MarketingOutreach',
        'DIActivity',
        'DIHardwareAndMaterials',
        'DIRebateAndInspection',
        'EMV',
        'UserInputIncentive',
        'CostsRecoveredFromOtherSources',
    ]
    f = lambda r: r[program_cost_columns].sum() / quarterly_discount_rate ** (r.Qi - first_year * 4)
    present_value_program_costs = program.apply(f, axis='columns').aggregate(np.sum)

    present_value_measure_external_costs = (
        measure.Qty *
        measure[[
            'IncentiveToOthers',
            'DILaborCost',
            'DIMaterialCost',
            'EndUserRebate'
        ]].sum() /
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )

    # calculate the present value of the incremental cost of the measure:
    if measure.RUL > 0:
        present_value_measure_gross_incremental_cost = (
            measure.Qty *
            (
                measure.UnitMeasureGrossCost -
                (
                    measure.UnitMeasureGrossCost -
                    measure.UnitMeasureGrossCost_ER
                ) *
                (
                    quarterly_measure_inflation_rate /
                    quarterly_discount_rate
                ) ** measure.RULq
            ) /
            quarterly_discount_rate ** ( measure.Qi - first_year * 4 )
        )
    else:
        present_value_measure_gross_incremental_cost = 0


    # calculate present value of up- and mid-stream incentives and direct installation costs:
    present_value_incentives_and_direct_installation = (
        measure.Qty *
        measure[[
            'IncentiveToOthers',
            'DILaborCost',
            'DIMaterialCost'
        ]].sum() /
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )

    # calculate present value of rebates to end-user:
    present_value_rebates = (
        measure.Qty *
        measure.EndUserRebate /
       quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )

    # calculate incentives in excess of measure cost:
    present_value_excess_incentives = max(0,
        measure.Qty *
        (
            measure.IncentiveToOthers +
            measure.DILaborCost +
            measure.DIMaterialCost -
            measure.UnitMeasureGrossCost
        ) /
        quarterly_discount_rate ** (measure.Qi - first_year * 4)
    )

    # weigh program costs based on measure gross savings, if possible, otherwise by install count:
    if program_total[['ElectricGross','GasGross']].sum() != 0:
        program_weighting_gross = (
            max(measure.ElecBenGross, 0) +
            max(measure.GasBenGross, 0)
        ) / program_total[['ElectricGross','GasGross']].sum()
    else:
        program_weighting_gross = 1 / program_total.Count

    present_value_gross_participant_costs = (
        present_value_measure_gross_incremental_cost -
        (
            present_value_incentives_and_direct_installation +
            present_value_rebates
        ) +
        present_value_excess_incentives
    )

    total_resource_cost_gross = (
        program_weighting_gross * present_value_program_costs +
        present_value_measure_external_costs +
        present_value_gross_participant_costs
    )

    total_resource_cost_gross_no_admin = (
        present_value_measure_external_costs +
        present_value_gross_participant_costs
    )

    # weigh program costs based on measure net savings, if possible, otherwise by install count:
    if program_total[['ElectricNet','GasNet']].sum() != 0:
        program_weighting_net = (
            max(measure.ElecBenNet, 0) +
            max(measure.GasBenNet, 0)
        ) / program_total[['ElectricNet','GasNet']].sum()
    else:
        program_weighting_net = 1 / program_total.Count

    present_value_net_participant_costs = measure.NTGRCost * (
        present_value_measure_gross_incremental_cost -
        (
            present_value_incentives_and_direct_installation +
            present_value_rebates
        ) +
        present_value_excess_incentives
    )

    total_resource_cost_net = (
        program_weighting_net * present_value_program_costs +
        present_value_measure_external_costs +
        present_value_net_participant_costs
    )

    total_resource_cost_net_no_admin = (
        present_value_measure_external_costs +
        present_value_net_participant_costs
    )

    if total_resource_cost_net != 0:
        total_resource_cost_ratio = measure[['ElecBenNet','GasBenNet']].sum() / total_resource_cost_net
    else:
        total_resource_cost_ratio = 0

    if total_resource_cost_net_no_admin != 0:
        total_resource_cost_ratio_no_admin = measure[['ElecBenNet','GasBenNet']].sum() / total_resource_cost_net_no_admin
    else:
        total_resource_cost_ratio_no_admin = 0

    return pd.Series({
        'CET_ID'                        : measure.CET_ID,
        'TotalResourceCostGross'        : total_resource_cost_gross,
        'TotalResourceCostGrossNoAdmin' : total_resource_cost_gross_no_admin,
        'TotalResourceCostNet'          : total_resource_cost_net,
        'TotalResourceCostNetNoAdmin'   : total_resource_cost_net_no_admin,
        'TotalResourceCostRatio'        : total_resource_cost_ratio,
        'TotalResourceCostRatioNoAdmin' : total_resource_cost_ratio_no_admin,
    })

def calculate_program_administrator_costs():
    pass

def calculate_bill_reduction_electric():
    pass

def calculate_bill_reduction_gas():
    pass

def calculate_ratepayer_cost():
    pass

def calculate_weighted_benefits():
    pass

def calculate_weigted_electric_allocation():
    pass

def calculate_weighted_program_cost():
    pass
