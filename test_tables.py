from login import user
from tables import setup_input_measures,setup_input_programs,setup_settings, \
    setup_emissions,setup_combustion_types,setup_avoided_cost_electric, \
    setup_avoided_cost_gas,setup_rate_schedule_electric,setup_rate_schedule_gas

def main():

    # path to input tables:
    in_path = '/home/rhansen/src/python/sqlserver/PythonCET/UserInputTables/'

    # path to E3 tables:
    e3_path = '/home/rhansen/src/python/sqlserver/PythonCET/E3Tables/'

    # input files:
    user_inputs = {
        'measures' : [
            'InputMeasureCEDARS.csv',
            'Measure.csv',
        ],
        'programs' : [
            'InputProgramCEDARS.csv',
            'ProgramCost.csv',
        ],
    }

    # avoided cost versions:
    avoided_cost = {
        'electric' : [
            #'E3AvoidedCostElecSeq2013',
            'E3AvoidedCostElecCO2Seq2018',
            'E3AvoidedCostElecCO2Seq2019',
            'E3AvoidedCostElecCO2Seq2020',

        ],
        'gas' : [
            #'E3AvoidedCostGasSeq2013',
            'E3AvoidedCostGasSeq2018',
            'E3AvoidedCostGasSeq2019',
            'E3AvoidedCostGasSeq2020',
        ],
    }

    # database only:
    im = setup_input_measures('database','InputMeasureCEDARS',2018,0.05,0.00,user)
    ip = setup_input_programs('database','InputProgramCEDARS',user)
    s = setup_settings('database','E3Settings',2018,im,user)
    em = setup_emissions('database','E3Emissions',2018,im,user)
    ct = setup_combustion_types('database','E3CombustionType',im,user)
    for ace_table in avoided_cost['electric']:
        ace = setup_avoided_cost_electric('database',ace_table,im,user)
    for acg_table in avoided_cost['gas']:
        acg = setup_avoided_cost_gas('database',acg_table,im,user)
    rse = setup_rate_schedule_electric('database','E3RateScheduleElec',2018,im,user)
    rsg = setup_rate_schedule_gas('database','E3RateScheduleGas',2018,im,user)

    # local input files (CEDARS CET format), database e3 tables:
    for fn in user_inputs['measures']:
        im = setup_input_measures('csv',in_path + fn,2018,0.05,0.00,user)

        for fn in user_inputs['programs']:
            ip = setup_input_programs('csv',in_path + fn,user)

        s = setup_settings('database','E3Settings',2018,im,user)
        em = setup_emissions('database','E3Emissions',2018,im,user)
        ct = setup_combustion_types('database','E3CombustionType',im,user)
        for ace_table in avoided_cost['electric']:
            ace = setup_avoided_cost_electric('database',ace_table,im,user)
        for acg_table in avoided_cost['gas']:
            acg = setup_avoided_cost_gas('database',acg_table,im,user)
        rse = setup_rate_schedule_electric('database','E3RateScheduleElec',2018,im,user)
        rsg = setup_rate_schedule_gas('database','E3RateScheduleGas',2018,im,user)

    # local files only:
    for fn in user_inputs['measures']:
        im = setup_input_measures('csv',in_path + fn,2018,0.05,0.00,user)

        for fn in user_inputs['programs']:
            ip = setup_input_programs('csv',in_path + fn,user)

        s = setup_settings('csv',e3_path + 'E3Settings.csv',2018,im,user)
        em = setup_emissions('csv',e3_path + 'E3Emissions.csv',2018,im,user)
        ct = setup_combustion_types('csv',e3_path + 'E3CombustionType.csv',im,user)
        for ace_table in avoided_cost['electric']:
            ace = setup_avoided_cost_electric('csv',e3_path + ace_table + '.csv',im,user)
        for acg_table in avoided_cost['gas']:
            acg = setup_avoided_cost_gas('csv',e3_path + acg_table + '.csv',im,user)
        rse = setup_rate_schedule_electric('csv',e3_path + 'E3RateScheduleElec.csv',2018,im,user)
        rsg = setup_rate_schedule_gas('csv',e3_path + 'E3RateScheduleGas.csv',2018,im,user)
