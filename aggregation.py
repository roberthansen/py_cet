import numpy as np
import pandas as pd

import equations as eq

def calculate_avoided_electric_costs(measure, AvoidedCostElectric, Settings, first_year, market_effects):
    ### parameters:
    ###     measure : a pandas Series containing a single row from the
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
    avoided_cost_electric = AvoidedCostElectric.filter_by_measure(measure)

    if avoided_cost_electric.size > 0:
        # filter settings table:
        settings = Settings.filter_by_measure(measure).iloc[0]

        f = lambda r: eq.present_value_generation_benefits(r, measure, settings, first_year)
        present_value_generation_benefits = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)

        f = lambda r: eq.present_value_transmission_and_distribution_benefits(r, measure, settings, first_year)
        present_value_transmission_and_distribution_benefits = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)
    else:
        present_value_generation_benefits = 0
        present_value_transmission_and_distribution_benefits = 0
    
    avoided_electric_costs = pd.Series({
        'CET_ID'                                   : measure.CET_ID,
        'ProgramID'                                : measure.ProgramID,
        'Qi'                                       : measure.Qi,
        'GenerationBenefitsGross'                  : max(
            measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionBenefitsGross' : max(
            measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricBenefitsGross'                    : max(
            -measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits -
            measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationCostsGross'                     : max(
            -measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionCostsGross'    : max(
            -measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricCostsGross'                       : max(
            -measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits -
            measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationBenefitsNet'                    : max(
            measure[['Quantity','IRkWh','RRkWh']].product() *
            (measure.NTGRkWh + market_effects) *
            present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionBenefitsNet'   : max(
            measure[['Quantity','IRkW','RRkW']].product() *
            (measure.NTGRkW + market_effects) *
            present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricBenefitsNet'                      : max(
            measure[['Quantity','IRkWh','RRkWh']].product() * (measure.NTGRkWh + market_effects) * present_value_generation_benefits +
            measure[['Quantity','IRkW','RRkW']].product() * (measure.NTGRkW + market_effects) * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationCostsNet'                       : max(
            measure[['Quantity','IRkWh','RRkWh']].product() *
            (measure.NTGRkWh + market_effects) *
            present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionCostsNet'      : max(
            measure[['Quantity','IRkW','RRkW']].product() *
            (measure.NTGRkW + market_effects) *
            present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricCostsNet'                         : max(
            measure[['Quantity','IRkWh','RRkWh']].product() * (measure.NTGRkWh + market_effects) * present_value_generation_benefits +
            measure[['Quantity','IRkW','RRkW']].product() * (measure.NTGRkW + market_effects) * present_value_transmission_and_distribution_benefits,
            0
        ),
    })

    return avoided_electric_costs

def calculate_avoided_gas_costs(measure, AvoidedCostGas, Settings, first_year, market_effects):
    ### parameters:
    ###     measure : a pandas Series containing a single row from the
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
    avoided_cost_gas = AvoidedCostGas.filter_by_measure(measure)

    if avoided_cost_gas.size > 0:
        # filter settings table:
        settings = Settings.filter_by_measure(measure).iloc[0]

        f = lambda r: eq.present_value_gas_benefits(r, measure, settings, first_year)
        pv_gas = avoided_cost_gas.apply(f,axis='columns').aggregate(np.sum)
    else:
        pv_gas = 0

    avoided_gas_costs = pd.Series({
        'CET_ID'           : measure.CET_ID,
        'ProgramID'        : measure.ProgramID,
        'Qi'               : measure.Qi,
        'GasBenefitsGross' : max(
            measure[['Quantity','IRTherm','RRTherm']].product() * pv_gas,
            0
        ),
        'GasCostsGross'    : max(
            -measure[['Quantity','IRTherm','RRTherm']].product() * pv_gas,
            0
        ),
        'GasBenefitsNet'   : max(
            measure[['Quantity','IRTherm','RRTherm']].product() *
            (measure.NTGRkW + market_effects) * pv_gas,
            0
        ),
        'GasCostsNet'      : max(
            -measure[['Quantity','IRTherm','RRTherm']].product() *
            (measure.NTGRkW + market_effects) * pv_gas,
            0
        ),
    })

    return avoided_gas_costs

def total_resource_cost_test(measure, programs, Settings, first_year):
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
    ###     float value of the total resource cost test for the given measure
    
    # filter programs based on measure, both subtotals for measure's installation quarter and totals for all quarters:
    sum_columns = ['ProgramID','Count','ElectricBenefitsGross','ElectricBenefitsNet','GasBenefitsGross','GasBenefitsNet']
    program = programs.get(programs.ProgramID == measure.ProgramID)
    program_total = programs.get(programs.ProgramID == measure.ProgramID)[sum_columns].groupby('ProgramID').aggregate(np.sum).iloc[0]

    # filter settings based on measure:
    settings = Settings.filter_by_measure(measure).iloc[0]

    # get quarterly discount rate for exponentiation:
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    # get measure inflation rate:
    quarterly_measure_inflation_rate = 1 + measure.AnnualInflationRate / 4

    # calculate the present value of cost to external parties:
    present_value_external_costs = eq.present_value_external_costs(measure, quarterly_discount_rate, first_year)

    # calculate the present value of the incremental cost of the measure:
    present_value_gross_incremental_cost = eq.present_value_gross_incremental_cost(measure, quarterly_measure_inflation_rate, quarterly_discount_rate, first_year)

    # calculate present value of up- and mid-stream incentives and direct installation costs:
    present_value_incentives_and_direct_installation =  eq.present_value_incentives_and_direct_installation(measure, quarterly_discount_rate, first_year)

    # calculate present value of rebates to end-user:
    present_value_rebates = eq.present_value_rebates(measure, quarterly_discount_rate, first_year)

    # calculate incentives in excess of measure cost:
    present_value_excess_incentives = eq.present_value_excess_incentives(measure, quarterly_discount_rate, first_year)

    present_value_gross_participant_costs = (
        present_value_gross_incremental_cost -
        (
            present_value_incentives_and_direct_installation +
            present_value_rebates
        ) +
        present_value_excess_incentives
    )

    present_value_net_participant_costs = (
        ( measure.NTGRCost + measure.MarketEffectsCosts ) * 
        (
            present_value_gross_incremental_cost -
            (
                present_value_incentives_and_direct_installation +
                present_value_rebates
            ) +
            present_value_excess_incentives
        )
    )

    # calculate future and present values of program-level costs:
    program_cost_columns = [
        'AdminCostsOverheadAndGA',
        'AdminCostsOther',
        'MarketingOutreach',
        'DIActivity',
        'DIInstallation',
        'DIHardwareAndMaterials',
        'DIRebateAndInspection',
        'EMV',
        'UserInputIncentive',
        'CostsRecoveredFromOtherSources',
    ]
    f = lambda r: r[program_cost_columns].sum() / quarterly_discount_rate ** (r.Qi - first_year * 4)
    present_value_program_costs = program.apply(f, axis='columns').aggregate(np.sum)

    # weigh program costs based on measure gross savings, if possible, otherwise by install count:
    if program_total[['ElectricBenefitsGross','GasBenefitsGross']].sum() > 0:
        program_weighting_gross = (
            measure[['ElectricBenefitsGross','GasBenefitsGross']].sum() /
            program_total[['ElectricBenefitsGross','GasBenefitsGross']].sum()
        )
    else:
        program_weighting_gross = 1 / program_total.Count

    total_resource_cost_gross = (
        program_weighting_gross * present_value_program_costs +
        present_value_external_costs +
        present_value_gross_participant_costs +
        measure.ElectricCostsGross +
        measure.GasCostsGross
    )

    total_resource_cost_gross_no_admin = (
        present_value_external_costs +
        present_value_gross_participant_costs +
        measure.ElectricCostsGross +
        measure.GasCostsGross
    )

    # weigh program costs based on measure net savings, if possible, otherwise by install count:
    if program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum() > 0:
        program_weighting_net = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum()
        )
    else:
        program_weighting_net = 1 / program_total.Count

    total_resource_cost_net = (
        program_weighting_net * present_value_program_costs +
        present_value_external_costs +
        present_value_net_participant_costs +
        measure.ElectricCostsNet +
        measure.GasCostsNet
    )

    total_resource_cost_net_no_admin = (
        present_value_external_costs +
        present_value_net_participant_costs +
        measure.ElectricCostsNet +
        measure.GasCostsNet
    )

    if total_resource_cost_net != 0:
        total_resource_cost_ratio = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            total_resource_cost_net
        )
    else:
        total_resource_cost_ratio = 0

    if total_resource_cost_net_no_admin != 0:
        total_resource_cost_ratio_no_admin = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            total_resource_cost_net_no_admin
        )
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

def program_administrator_cost_test(measure, programs, Settings, first_year):
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
    ###     float value of the program administrator cost test for the given measure

    # filter programs based on measure, both subtotals for measure's installation quarter and totals for all quarters:
    sum_columns = ['ProgramID','Count','ElectricBenefitsGross','ElectricBenefitsNet','GasBenefitsGross','GasBenefitsNet']
    program = programs.get(programs.ProgramID == measure.ProgramID)
    program_total = programs.get(programs.ProgramID == measure.ProgramID)[sum_columns].groupby('ProgramID').aggregate(np.sum).iloc[0]

    # filter settings based on measure:
    settings = Settings.filter_by_measure(measure).iloc[0]

    # get quarterly discount rate for exponentiation:
    quarterly_discount_rate = 1 + settings.DiscountRateQtr

    # get measure inflation rate if present:
    try:
        quarterly_measure_inflation_rate = 1 + measure.AnnualInflationRate / 4
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

    # weigh program costs based on measure gross savings, if possible, otherwise by install count:
    if program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum() > 0:
        program_weighting = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum()
        )
    else:
        program_weighting = 1 / program_total.Count

    # calculate the present value of cost to external parties:
    present_value_external_costs = eq.present_value_external_costs(measure, quarterly_discount_rate, first_year)

    program_administrator_cost = (
        program_weighting *
        present_value_program_costs +
        present_value_external_costs
    )

    program_administrator_cost_no_admin = (
        present_value_external_costs
    )

    if program_administrator_cost != 0:
        program_administrator_cost_ratio = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            program_administrator_cost
        )
    else:
        program_administrator_cost_ratio = 0

    if program_administrator_cost_no_admin != 0:
        program_administrator_cost_ratio_no_admin = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            program_administrator_cost_no_admin
        )
    else:
        program_administrator_cost_ratio_no_admin = 0

    return pd.Series({
        'CET_ID'                               : measure.CET_ID,
        'ProgramAdministratorCost'             : program_administrator_cost,
        'ProgramAdministratorCostNoAdmin'      : program_administrator_cost_no_admin,
        'ProgramAdministratorCostRatio'        : program_administrator_cost_ratio,
        'ProgramAdministratorCostRatioNoAdmin' : program_administrator_cost_ratio_no_admin,
    })

def ratepayer_impact_measure_test(measure, RateScheduleElectric, RateScheduleGas, Settings, market_effects, first_year):
    ### parameters:
    ###     measure : a pandas Series containing a single row from the
    ###         'data' pandas DataFrame in an 'InputMeasures' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
    ###     RateScheduleElectric : an instance of a 'RateScheduleElectric' object
    ###         of class 'EDCS_Table' or 'EDCS_Query_Results'
    ###     RateScheduleGas : an instance of a 'RateScheduleGas' object of class
    ###         'EDCS_Table' or 'EDCS_Query_Results'
    ###     Settings : an instance of a 'Settings' object of class 'EDCS_Table'
    ###         or 'EDCS_Query_Results'
    ###     market_effects : a float representing the allowed market effects for
    ###         the program
    ###     first_year : an int representing the first year for the program
    ###         through which the input measure is implemented
    ###
    ### returns:
    ###     pandas Series containing the results of the Ratepayer Impact Measure
    ###     test

    # filter electric and gas rate schedules according to measure:
    annual_electric_rates = RateScheduleElectric.filter_by_measure(measure)
    annual_gas_rates = RateScheduleGas.filter_by_measure(measure)

    # filter settings table according to measure:
    settings = Settings.filter_by_measure(measure).iloc[0]

    # calculate electric ratepayer impact:
    f = lambda r: eq.present_value_bill_savings_electric(r, measure, settings, first_year)

    ratepayer_impact_electric = (
        measure[['Quantity','IRkWh','RRkWh']].product() *
        (measure.NTGRkWh + market_effects) *
        annual_electric_rates.apply(f,axis='columns').aggregate(np.sum)
    )

    # calculate natural gas ratepayer impact:
    f = lambda r: eq.present_value_bill_savings_gas(r, measure, settings, first_year)

    ratepayer_impact_gas = (
        measure[['Quantity','IRTherm','RRTherm']].product() *
        (measure.NTGRTherm + market_effects) *
        annual_gas_rates.apply(f,axis='columns').aggregate(np.sum)
    )

    # combine results for output:
    return pd.Series({
        'CET_ID' : measure.CET_ID,
        'RatepayerImpactElectric' : ratepayer_impact_electric,
        'RatepayerImpactGas'      : ratepayer_impact_gas,
        'RatepayerImpactTotal'    : ratepayer_impact_electric + ratepayer_impact_gas,

    })

def calculate_weighted_benefits():
    pass

def calculate_weigted_electric_allocation():
    pass

def calculate_weighted_program_cost():
    pass
