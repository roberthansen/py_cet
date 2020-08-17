import numpy as np
import pandas as pd

from aggregation import ratepayer_impact_measure_test
import equations_match_sql as eq

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
    avoided_cost_electric = AvoidedCostElectric.filter_by_measure(input_measure)

    if avoided_cost_electric.size > 0:
        # filter settings table:
        settings = Settings.filter_by_measure(input_measure).iloc[0]

        f = lambda r: eq.present_value_generation_benefits(r, input_measure, settings, first_year)
        present_value_generation_benefits = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)

        f = lambda r: eq.present_value_transmission_and_distribution_benefits(r, input_measure, settings, first_year)
        present_value_transmission_and_distribution_benefits = avoided_cost_electric.apply(f, axis='columns').aggregate(np.sum)
    else:
        present_value_generation_benefits = 0
        present_value_transmission_and_distribution_benefits = 0
    
    avoided_electric_costs = pd.Series({
        'CET_ID'                                   : input_measure.CET_ID,
        'ProgramID'                                : input_measure.ProgramID,
        'Qi'                                       : input_measure.Qi,
        'GenerationBenefitsGross'                  : max(
            input_measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionBenefitsGross' : max(
            input_measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricBenefitsGross'                    : max(
            input_measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits +
            input_measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationCostsGross'                     : max(
            -input_measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionCostsGross'    : max(
            -input_measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricCostsGross'                       : max(
            -input_measure[['Quantity','IRkWh','RRkWh']].product() * present_value_generation_benefits -
            input_measure[['Quantity','IRkW','RRkW']].product() * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationBenefitsNet'                    : max(
            input_measure[['Quantity','IRkWh','RRkWh']].product() *
            (input_measure.NTGRkWh + market_effects) *
            present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionBenefitsNet'   : max(
            input_measure[['Quantity','IRkW','RRkW']].product() *
            (input_measure.NTGRkW + market_effects) *
            present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricBenefitsNet'                      : max(
            input_measure[['Quantity','IRkWh','RRkWh']].product() * (input_measure.NTGRkWh + market_effects) * present_value_generation_benefits +
            input_measure[['Quantity','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * present_value_transmission_and_distribution_benefits,
            0
        ),
        'GenerationCostsNet'                       : max(
            -input_measure[['Quantity','IRkWh','RRkWh']].product() *
            (input_measure.NTGRkWh + market_effects) *
            present_value_generation_benefits,
            0
        ),
        'TransmissionAndDistributionCostsNet'      : max(
            -input_measure[['Quantity','IRkW','RRkW']].product() *
            (input_measure.NTGRkW + market_effects) *
            present_value_transmission_and_distribution_benefits,
            0
        ),
        'ElectricCostsNet'                         : max(
            -input_measure[['Quantity','IRkWh','RRkWh']].product() * (input_measure.NTGRkWh + market_effects) * present_value_generation_benefits -
            input_measure[['Quantity','IRkW','RRkW']].product() * (input_measure.NTGRkW + market_effects) * present_value_transmission_and_distribution_benefits,
            0
        ),
    })

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
    avoided_cost_gas = AvoidedCostGas.filter_by_measure(input_measure)

    if avoided_cost_gas.size > 0:
        # filter settings table for calculations based on sql version of cet:
        settings = Settings.filter_by_measure(input_measure).iloc[0]

        f = lambda r: eq.present_value_gas_benefits(r, input_measure, settings, first_year)
        pv_gas = avoided_cost_gas.apply(f,axis='columns').aggregate(np.sum)
    else:
        pv_gas = 0

    avoided_gas_costs = pd.Series({
        'CET_ID'           : input_measure.CET_ID,
        'ProgramID'        : input_measure.ProgramID,
        'Qi'               : input_measure.Qi,
        'GasBenefitsGross' : max(
            input_measure[['Quantity','IRTherm','RRTherm']].product() * pv_gas,
            0
        ),
        'GasCostsGross'    : max(
            -input_measure[['Quantity','IRTherm','RRTherm']].product() * pv_gas,
            0
        ),
        'GasBenefitsNet'   : max(
            input_measure[['Quantity','IRTherm','RRTherm']].product() *
            (input_measure.NTGRkW + market_effects) * pv_gas,
            0
        ),
        'GasCostsNet'      : max(
            -input_measure[['Quantity','IRTherm','RRTherm']].product() *
            (input_measure.NTGRkW + market_effects) * pv_gas,
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

    # get quarterly and annual discount rates for exponentiation:
    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    annual_discount_rate = 1 + settings.DiscountRateAnnual

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

    # calculate incentives in excess of measure cost (ONLY IN INCORRECT CALCULATIONS):
    present_value_excess_incentives = eq.present_value_excess_incentives(measure, quarterly_discount_rate, first_year)

    present_value_gross_participant_costs = (
        present_value_gross_incremental_cost +
        present_value_excess_incentives -
        (
            present_value_incentives_and_direct_installation +
            present_value_rebates
        )
    )

    #INCORRECT CALCULATION WITH MISAPPLIED MARKET EFFECTS:
    present_value_net_participant_costs = (
        measure.NTGRCost * (
            present_value_gross_incremental_cost +
            present_value_excess_incentives -
            (
                present_value_incentives_and_direct_installation +
                present_value_rebates
            )
        ) +
        measure.MarketEffectsCosts *
        (
            present_value_gross_incremental_cost +
            present_value_excess_incentives
        )
    )

    # calculate present value of program-level costs:
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
    # INCORRECT CALCULATIONS USING INSTALLATION YEAR TO MATCH SQL CODE:
    f = lambda r: r[program_cost_columns].sum() / annual_discount_rate ** (int(r.InstallationQuarter.split('Q')[0]) - first_year)
    present_value_program_costs = program.apply(f, axis='columns').aggregate(np.sum)

    # weigh program costs based on measure gross savings, if possible, otherwise by install count:
    if program_total[['ElectricBenefitsGross','GasBenefitsGross']].sum() > 0:
        program_weighting_gross = (
            measure[['ElectricBenefitsGross','GasBenefitsGross']].sum() /
            program_total[['ElectricBenefitsGross','GasBenefitsGross']].sum()
        )
    else:
        program_weighting_gross = 1 / program_total.Count

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    total_resource_cost_gross = (
        program_weighting_gross * present_value_program_costs +
        present_value_external_costs +
        present_value_gross_participant_costs
    )

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    total_resource_cost_gross_no_admin = (
        program_weighting_gross * present_value_external_costs +
        present_value_gross_participant_costs
    )

    # weigh program costs based on measure net savings, if possible, otherwise by install count:
    if program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum() > 0:
        program_weighting_net = (
            measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
            program_total[['ElectricBenefitsNet','GasBenefitsNet']].sum()
        )
    else:
        program_weighting_net = 1 / program_total.Count

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    total_resource_cost_net = (
        program_weighting_net * present_value_program_costs +
        present_value_external_costs +
        present_value_net_participant_costs
    )

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    total_resource_cost_net_no_admin = (
        program_weighting_net * present_value_external_costs +
        present_value_net_participant_costs
    )

    # INCORRECT APPLICATION OF NEGATIVE BENEFITS TO NUMERATOR OF RATIO:
    if total_resource_cost_net != 0:
        total_resource_cost_ratio = (
            (
                measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() -
                measure[['ElectricCostsNet','GasCostsNet']].sum()
            ) /
            total_resource_cost_net
        )
    else:
        total_resource_cost_ratio = 0

    # INCORRECT APPLICATION OF NEGATIVE BENEFITS TO NUMERATOR OF RATIO:
    if total_resource_cost_net_no_admin != 0:
        total_resource_cost_ratio_no_admin = (
            (
                measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() -
                measure[['ElectricCostsNet','GasCostsNet']].sum()
            ) /
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

    # get quarterly and annual discount rates for exponentiation:
    quarterly_discount_rate = 1 + settings.DiscountRateQtr
    annual_discount_rate = 1 + settings.DiscountRateAnnual

    # calculate incentives in excess of measure cost (ONLY IN INCORRECT CALCULATIONS):
    present_value_excess_incentives = eq.present_value_excess_incentives(measure, quarterly_discount_rate, first_year)

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
    # INCORRECT CALCULATION USING INSTALLATION YEAR INSTEAD OF QUARTER TO MATCH SQL CODE:
    f = lambda r: r[program_cost_columns].sum() / annual_discount_rate ** (int(r.InstallationQuarter.split('Q')[0]) - first_year)
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

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    program_administrator_cost = (
        program_weighting * present_value_program_costs +
        present_value_external_costs
    )

    # INCORRECT EXCLUSION OF NEGATIVE AVOIDED COSTS:
    program_administrator_cost_no_admin = (
        present_value_external_costs
    )

    # INCORRECT APPLICATION OF NEGATIVE BENEFITS TO NUMERATOR OF RATIO:
    if program_administrator_cost != 0:
        program_administrator_cost_ratio = (
            (
                measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() -
                measure[['ElectricCostsNet','GasCostsNet']].sum()
            ) /
            program_administrator_cost
        )
    else:
        program_administrator_cost_ratio = 0

    # INCORRECT APPLICATION OF NEGATIVE BENEFITS TO NUMERATOR OF RATIO:
    if program_administrator_cost_no_admin != 0:
        program_administrator_cost_ratio_no_admin = (
            (
                measure[['ElectricBenefitsNet','GasBenefitsNet']].sum() -
                measure[['ElectricCostsNet','GasCostsNet']].sum()
            ) /
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
