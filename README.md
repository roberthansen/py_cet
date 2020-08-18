# py_cet
## Python-based Cost Effectiveness Tool

py_cet is an unofficial project to implement the California Public Utilities Commission's (CPUC) Cost Effectiveness Tool (CET) in Python.

This effort has a X primary goals:
+ To analyze the existing Cost Effectiveness Tool, written in Transact-SQL, and improve Energy Division Staff's familiarity with the CET calculations
+ To identify any errors or misalignments with Commission's policy in the existing SQL-based Cost Effectiveness Tool
+ To develop a more accessible Cost Effectiveness platform that is easier for the Commission to maintain and is more transparent to California's energy efficiency stakeholders
+ To allow for more data storage and processing optimization, improving calculation speed
+ To build a platform on which to develop a more interactive CET

## Background
The Cost Effectiveness Tool relies on an extensive database of avoided costs and fuel rates, managed by the California Public Utilities Commission. This database is derived from E3's Avoided Cost Calculations, and is updated roughly once a year. The stored data tables include:
+ Avoided Electric Costs - forecasted utility-side electric energy rates in $/kWh for each year from present through 20+years in the future
+ Avoided Gas Costs - forecasted utility-side natural gas energy costs in $/therm for each quarter-year from present through 20+ years in the future
+ Electric Rate Schedule - forecasted customer-side electric energy rates in $/kWh for each year from present through 20+ years in the future
+ Gas Rate Schedule - forecasted customer-side natural gas energy rates in $/Therms for each year from present through 20+ years in the future

Users must submit input data in the form of two tables describing measure-level and program-level data.

## Calculation
The CET iterates through two levels of loops. The outer "aggregation" loop progresses through each measure included in the user's inputs. For each measure, the Cost Effectiveness tool calculates inner-loop "equations" for each row of the avoided cost and rate schedule tables matching the given measure. The avoided cost and rate schedule tables include periodic values which are compared against the measure's installation date and expected useful life, thus only rows with data applicable to the measure are used in the "equations." The results of the inner "equations" loop are rolled-up to the measure-level. The results of the outer "aggregation" loop are available as outputs of the CET, and are further rolled-up to the program-level results.

## Cost Effectiveness Tests
The Cost Effectiveness Tool produces the outputs of three tests, according to :
+ Total Resource Cost (TRC) Test
+ Program Administrator Cost (PAC) Test
+ Ratepayer Impact Measure (RIM) Test
