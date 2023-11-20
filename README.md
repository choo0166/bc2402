# BC2402 Designing and Developing Databases
Final project submission for BC2402 Designing and Developing Databases AY2020/2021 @ Nanyang Technological University

## Data Sources
- [Our World in Data](https://github.com/owid/covid-19-data/tree/master/public/data)
- [ECDC Response Measures](https://www.ecdc.europa.eu/en/publications-data/download-data-response-measures-covid-19)
- [Google Community Mobility Reports](https://www.google.com/covid19/mobility/index.html?hl=en)

## Appendix of queries
- Generate a list of unique locations (countries) in Asia
- Generate a list of unique locations (countries) in Asia and Europe, with more than 10 total cases on 2020-04-01
- Generate a list of unique locations (countries) in Africa, with less than 10,000 total cases between 2020-04-01 and 2020-04-20 (inclusive of the start date and end date)
- Generate a list of unique locations (countries) without any data on total tests
- Conduct trend analysis, i.e., for each month, compute the total number of new cases globally. 
- Conduct trend analysis, i.e., for each month, compute the total number of new cases in each continent
- Generate a list of EU countries that have implemented mask related responses (i.e., response measure contains the word “mask”).
- Compute the period (i.e., start date and end date) in which most EU countries has implemented MasksMandatory as the response measure. For NA values, use 1 August 2020.
- Based on the period above, conduct trend analysis for Europe and North America, i.e., for each day during the period, compute the total number of new cases.
- Generate a list of unique locations (countries) that have successfully flattened the curve (i.e., achieved more than 14 days of 0 new cases, after recording more than 50 cases)
- Second wave detection – generate a list of unique locations (countries) that have flattened the curve (as defined above) but suffered upticks in new cases (i.e., after >= 14 days, registered more than 50 cases in a subsequent 7-day window)
- Display the top 3 countries in terms of changes from baseline in each of the place categories (i.e., grocery and pharmacy, parks, transit stations, retail and recreation, residential, and workplaces)
- Conduct mobility trend analysis, i.e., in Indonesia, identify the date where more than 20,000 cases were recorded (D-day). Based on D-day, show the daily changes in mobility trends for the 3 place categories (i.e., retail and recreation, workplaces, and grocery and pharmacy).

