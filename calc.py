import os, types, numpy as np, pandas as pd
from datetime import datetime as dt
import measure_calculations, measure_calculations_match_sql
import multiprocessing_helper, multiprocessing_helper_match_sql

import dill as pickle

# calculate measure-level benefits and costs:
def calculate_measure_cost_effectiveness(cet_scenario):
    ### parameters:
    ###     cet_scenario : an instance of the CETScenario class containing
    ###         the input data and parameters used in calculating cost
    ###         effectiveness outputs
    ###
    ### outputs:
    ###     pandas DataFrame with measure-level cost-effectiveness outputs

    if cet_scenario.match_sql:
        mc = measure_calculations_match_sql
        mp = multiprocessing_helper_match_sql
    else:
        mc = measure_calculations
        mp = multiprocessing_helper

    if cet_scenario.parallelize:
        # create objects without pyodbc connections:
        InputMeasuresData = cet_scenario.InputMeasures.data
        Emissions = mp.EmissionsTable(cet_scenario.Emissions.data)
        CombustionTypes = mp.CombustionTypesTable(cet_scenario.CombustionTypes.data)
        Settings = mp.SettingsTable(cet_scenario.Settings.data)
        AvoidedCostElectric = mp.AvoidedCostElectricTable(cet_scenario.AvoidedCostElectric.data)
        AvoidedCostGas = mp.AvoidedCostGasTable(cet_scenario.AvoidedCostGas.data)

        # run parallelized apply functions:
        ## avoided costs:
        t_acce = dt.now()
        avoided_electric_costs = mp.MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostElectric, Settings, cet_scenario.first_year, mc.calculate_avoided_electric_costs).calculate()
        cet_scenario.calculation_times['avoided_electric_costs'] = (dt.now() - t_acce).total_seconds()

        t_accg = dt.now()
        avoided_gas_costs = mp.MultiprocessingAvoidedCosts(InputMeasuresData, AvoidedCostGas, Settings, cet_scenario.first_year, mc.calculate_avoided_gas_costs).calculate()
        cet_scenario.calculation_times['avoided_gas_costs'] = (dt.now() - t_accg).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['avoided_electric_costs','avoided_gas_costs']])
        print('< Benefits Calculation Time with Parallelization: {:.3f} seconds >'.format(calculation_time))

        ## emissions reductions:
        t_emiss = dt.now()
        emissions_reductions = mp.MultiprocessingEmissionsReductions(InputMeasuresData, AvoidedCostElectric, Emissions, CombustionTypes, Settings, mc.calculate_emissions_reductions).calculate()
        calculation_time = (dt.now() - t_emiss).total_seconds()
        cet_scenario.calculation_times['emissions_reductions'] = calculation_time
        print('< Emissions Reductions Calculation Time with Parallelization: {:.3f} seconds >'.format(calculation_time))

    else:
        # run standard serial apply functions:
        ## avoided costs:
        t_acce = dt.now()
        f = lambda r: mc.calculate_avoided_electric_costs(r, cet_scenario.AvoidedCostElectric, cet_scenario.Settings, cet_scenario.first_year)
        avoided_electric_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        cet_scenario.calculation_times['avoided_electric_costs'] = (dt.now() - t_acce).total_seconds()

        t_accg = dt.now()
        f = lambda r: mc.calculate_avoided_gas_costs(r, cet_scenario.AvoidedCostGas, cet_scenario.Settings, cet_scenario.first_year)
        avoided_gas_costs = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        cet_scenario.calculation_times['avoided_gas_costs'] = (dt.now() - t_accg).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['avoided_electric_costs','avoided_gas_costs']])
        print('< Benefits Calculation Time without Parallelization: {:.3f} seconds >'.format(calculation_time))

        ## emissions reductions:
        t_emiss = dt.now()
        f = lambda r: mc.calculate_emissions_reductions(r, cet_scenario.AvoidedCostElectric, cet_scenario.Emissions, cet_scenario.CombustionTypes, cet_scenario.Settings)
        emissions_reductions = cet_scenario.InputMeasures.data.apply(f, axis='columns')
        calculation_time  = (dt.now() - t_emiss).total_seconds()
        print('< Emissions Reductions Calculation Time without Parallelization: {:.3f} seconds >'.format(calculation_time))

    measures = cet_scenario.InputMeasures.data.merge(
        avoided_electric_costs, on=['CET_ID','ProgramID','Qi']
    ).merge(
        avoided_gas_costs, on=['CET_ID','ProgramID','Qi']
    )

    benefit_sums = pd.DataFrame({
        'ProgramID'             : avoided_electric_costs.ProgramID,
        'Qi'                    : avoided_electric_costs.Qi,
        'Count'                 : 1,
        'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross,
        'ElectricCostsGross'    : avoided_electric_costs.ElectricCostsGross,
        'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet,
        'ElectricCostsNet'      : avoided_electric_costs.ElectricCostsNet,
        'GasBenefitsGross'      : avoided_gas_costs.GasBenefitsGross,
        'GasCostsGross'         : avoided_gas_costs.GasCostsGross,
        'GasBenefitsNet'        : avoided_gas_costs.GasBenefitsNet,
        'GasCostsNet'           : avoided_gas_costs.GasBenefitsNet,
    }).groupby(['ProgramID','Qi']).aggregate(np.sum)

    programs = cet_scenario.InputPrograms.data.merge(benefit_sums, on=['ProgramID','Qi'])

    if cet_scenario.parallelize:
        t_trc = dt.now()
        total_resource_cost_test_results = mp.MultiprocessingCostTest(measures, programs, Settings, cet_scenario.first_year, mc.total_resource_cost_test).calculate()
        cet_scenario.calculation_times['total_resource_cost_test'] = (dt.now() - t_trc).total_seconds()
        t_pac = dt.now()
        program_administrator_cost_test_results = mp.MultiprocessingCostTest(measures, programs, Settings, cet_scenario.first_year, mc.program_administrator_cost_test).calculate()
        cet_scenario.calculation_times['program_administrator_cost_test'] = (dt.now() - t_pac).total_seconds()

        RateScheduleElectric = mp.RateScheduleElectricTable(cet_scenario.RateScheduleElectric.data)
        RateScheduleGas = mp.RateScheduleGasTable(cet_scenario.RateScheduleGas.data)

        measures = measures.merge(program_administrator_cost_test_results,on='CET_ID')

        t_rim = dt.now()
        ratepayer_impact_measure_results = mp.MultiprocessingRatepayerImpactMeasure(
            measures,
            RateScheduleElectric,
            RateScheduleGas,
            Settings,
            cet_scenario.first_year,
            mc.ratepayer_impact_measure
        ).calculate()
        cet_scenario.calculation_times['ratepayer_impact_measure'] = (dt.now() - t_pac).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['total_resource_cost_test','program_administrator_cost_test','ratepayer_impact_measure']])
        cet_scenario.calculation_times['emissions_reductions'] = calculation_time
        print('< Test Calculation Time with Parallelization: {:.3f} seconds >'.format(calculation_time))

    else:
        t_trc = dt.now()
        f = lambda r: mc.total_resource_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
        total_resource_cost_test_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['total_resource_cost_test'] = (dt.now() - t_trc).total_seconds()

        t_pac = dt.now()
        f = lambda r: mc.program_administrator_cost_test(r, programs, cet_scenario.Settings, cet_scenario.first_year)
        program_administrator_cost_test_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['program_administrator_cost_test'] = (dt.now() - t_pac).total_seconds()

        measures = measures.merge(program_administrator_cost_test_results,on='CET_ID')

        t_rim = dt.now()
        f = lambda r: mc.ratepayer_impact_measure(r,cet_scenario.RateScheduleElectric,cet_scenario.RateScheduleGas,cet_scenario.Settings,cet_scenario.first_year)
        ratepayer_impact_measure_results = measures.apply(f, axis='columns')
        cet_scenario.calculation_times['ratepayer_impact_measure'] = (dt.now() - t_pac).total_seconds()

        calculation_time = sum([cet_scenario.calculation_times[s] for s in ['total_resource_cost_test','program_administrator_cost_test','ratepayer_impact_measure']])
        print('< Test Calculation Time without Parallelization: {:.3f} seconds >'.format(calculation_time))

    weighted_benefits = 0
    weigted_electric_allocation = 0
    weighted_program_cost =0

    if cet_scenario.match_sql:
        outputs = pd.DataFrame({
            'CET_ID'                : avoided_electric_costs.CET_ID,
            'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross - avoided_electric_costs.ElectricCostsGross,
            'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet - avoided_electric_costs.ElectricCostsNet,
        }).merge(
            pd.DataFrame({
                'CET_ID'           : avoided_gas_costs.CET_ID,
                'GasBenefitsGross' : avoided_gas_costs.GasBenefitsGross - avoided_gas_costs.GasCostsGross,
                'GasBenefitsNet'   : avoided_gas_costs.GasBenefitsNet - avoided_gas_costs.GasCostsNet,
                }), on='CET_ID'
            ).merge(
                emissions_reductions, on='CET_ID'
            ).merge(
                total_resource_cost_test_results, on='CET_ID'
            ).merge(
                program_administrator_cost_test_results, on='CET_ID'
            ).merge(
                ratepayer_impact_measure_results, on='CET_ID'
            )
    else:
        outputs = pd.DataFrame({
            'CET_ID'                : avoided_electric_costs.CET_ID,
            'ElectricBenefitsGross' : avoided_electric_costs.ElectricBenefitsGross,
            'ElectricBenefitsNet'   : avoided_electric_costs.ElectricBenefitsNet,
            'ElectricCostsGross'    : avoided_electric_costs.ElectricCostsGross,
            'ElectricCostsNet'      : avoided_electric_costs.ElectricCostsNet,
        }).merge(
            pd.DataFrame({
                'CET_ID'           : avoided_gas_costs.CET_ID,
                'GasBenefitsGross' : avoided_gas_costs.GasBenefitsGross,
                'GasBenefitsNet'   : avoided_gas_costs.GasBenefitsNet,
                'GasCostsGross'    : avoided_gas_costs.GasCostsGross,
                'GasCostsNet'      : avoided_gas_costs.GasCostsNet,
            }), on='CET_ID'
        ).merge(
            emissions_reductions, on='CET_ID'
        ).merge(
            total_resource_cost_test_results, on='CET_ID'
        ).merge(
            program_administrator_cost_test_results, on='CET_ID'
        ).merge(
            ratepayer_impact_measure_results, on='CET_ID'
        )

    return outputs

def calculate_program_cost_effectiveness(cet_scenario):
    ### parameters:
    ###     cet_scenario : an instance of the CETScenario class containing
    ###         the input data and parameters used in calculating cost
    ###         effectiveness outputs
    ###
    ### outputs:
    ###     pandas DataFrame with program-level cost-effectiveness outputs

    # Retrieve output measures with program identifiers from input measures table:
    OutputMeasures = cet_scenario.InputMeasures.data[['CET_ID','ProgramID']].merge(cet_scenario.OutputMeasures.data, on='CET_ID')

    # Sum all columns grouped by program:
    OutputPrograms = OutputMeasures.groupby(by='ProgramID').aggregate(np.sum)

    # Replace columns that are not simple sums:
    OutputPrograms.TotalResourceCostRatio = (
        OutputPrograms[['ElectricBenefitsNet','GasBenefitsNet']].sum(axis='columns') /
        OutputPrograms.TotalResourceCostNet
    )
    OutputPrograms.TotalResourceCostRatioNoAdmin = (
        OutputPrograms[['ElectricBenefitsNet','GasBenefitsNet']].sum(axis='columns') /
        OutputPrograms.TotalResourceCostNetNoAdmin
    )
    OutputPrograms.ProgramAdministratorCostRatio = (
        OutputPrograms[['ElectricBenefitsNet','GasBenefitsNet']].sum(axis='columns') /
        OutputPrograms.ProgramAdministratorCost
    )
    OutputPrograms.ProgramAdministratorCostNoAdmin = (
        OutputPrograms[['ElectricBenefitsNet','GasBenefitsNet']].sum(axis='columns') /
        OutputPrograms.ProgramAdministratorCostNoAdmin
    )
    return OutputPrograms

def calculate_portfolio_cost_effectiveness(cet_scenario):
    ### parameters:
    ###     cet_scenario : an instance of the CETScenario class containing
    ###         the input data and parameters used in calculating cost
    ###         effectiveness outputs
    ###
    ### outputs:
    ###     pandas DataFrame with portfolio-level cost-effectiveness outputs

    # Sum all columns:
    OutputPortfolio = cet_scenario.OutputMeasures.data.aggregate(np.sum, axis='index')

    # Replace columns that are not simple sums:
    OutputPortfolio.TotalResourceCostRatio = (
        OutputPortfolio[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
        OutputPortfolio.TotalResourceCostNet
    )
    OutputPortfolio.TotalResourceCostRatioNoAdmin = (
        OutputPortfolio[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
        OutputPortfolio.TotalResourceCostNetNoAdmin
    )
    OutputPortfolio.ProgramAdministratorCostRatio = (
        OutputPortfolio[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
        OutputPortfolio.ProgramAdministratorCost
    )
    OutputPortfolio.ProgramAdministratorCostNoAdmin = (
        OutputPortfolio[['ElectricBenefitsNet','GasBenefitsNet']].sum() /
        OutputPortfolio.ProgramAdministratorCostNoAdmin
    )

    return OutputPortfolio
