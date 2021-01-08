use covid_world

// Q1
db.owid_covid_data.aggregate([
    {$match: {"continent": "Asia"}},
    {$group: {_id: "$location"}},
    {$project: {"location": 1}}
    ])
 
    
// Q2 
db.owid_covid_data.aggregate([
    {$match: {"continent": {$in: ['Asia', 'Europe']}}},
    {$match: {"total_cases": {$gt: 10}}},
    {$match: {"date": ISODate('2020-04-01T00:00:00.000+08:00')
    {$group: {_id: "$location"}}
    ])


// Q3    
db.owid_covid_data.aggregate([
    {$match: {"continent": 'Africa'}},
    {$match: {"total_cases": {$lt: 10000}}},
    {$match: {"date": {$gte: ISODate('2020-04-01T00:00:00.000+08:00'), $lte: ISODate('2020-04-20T00:00:00.000+08:00')}}},
    {$group: {_id: "$location"}}
    ])
 
    
// Q4
// Summing a field with NaN in any document returns NaN

// Perform update to set NaN as null
db.owid_covid_data.updateMany({"total_tests": NaN}, {$set: {"total_tests": null}})

db.owid_covid_data.aggregate([
    {$group: {_id: "$location", sumTotalTests: {$sum: "$total_tests"}}},
    {$match: {"sumTotalTests": 0}}
    ])


// Q5
// Result differs from MySQL due to date operators
db.owid_covid_data.updateMany({"new_cases": NaN}, {$set: {"new_cases": null}})

db.owid_covid_data.aggregate([
    {$match: {"new_cases": {$ne: null}}}
    {$project: {"year": {$year: "$date"}, "month": {$month: "$date"}, "location": 1, "new_cases": 1}},
    {$group: {_id: {location: "$location", year: "$year", month: "$month"}, sumTotalNew: {$sum: "$new_cases"}}},
    {$sort: {'_id.location': 1, '_id.year': 1, '_id.month': 1, sumTotalNew: 1}}
    ])
 
 
// Q6
// Result differs from MySQL due to date operators
db.owid_covid_data.aggregate([
    {$match: {"continent": {$ne: ""}}},
    {$project: {"year": {$year: "$date"}, "month": {$month: "$date"}, "continent": 1, "new_cases": 1}},
    {$group: {_id: {continent: "$continent", year: "$year", month: "$month"}, sumTotalNew: {$sum: "$new_cases"}}},
    {$sort: {'_id.continent': 1, '_id.year': 1, '_id.month': 1}}
    ])
 
    
// Q7 
db.response_graphs.aggregate([
    {$match: {"Response_measure": {$regex: /mask/i}}},
    {$match: {"Country": {$in: ['Austria' , 'Belgium',
        'Bulgaria',
        'Croatia',
        'Cyprus',
        'Czechia',
        'Denmark',
        'Estonia',
        'Finland',
        'France',
        'Germany',
        'Greece',
        'Hungary',
        'Ireland',
        'Italy',
        'Latvia',
        'Lithuania',
        'Luxembourg',
        'Malta',
        'Netherlands',
        'Poland',
        'Portugal',
        'Romania',
        'Slovakia',
        'Slovenia',
        'Spain',
        'Sweden']}}},
    {$group: {_id: "$Country"}}])

    
// Q8
db.response_graphs.updateMany({"date_end": ISODate("1970-01-01")}, {$set: {"date_end": ISODate('2020-08-01')}})

db.response_graphs.aggregate([
    {$match: {"Response_measure": "MasksMandatory"}},
    {$match: {"Country": {$in: ['Austria' , 'Belgium',
        'Bulgaria',
        'Croatia',
        'Cyprus',
        'Czechia',
        'Denmark',
        'Estonia',
        'Finland',
        'France',
        'Germany',
        'Greece',
        'Hungary',
        'Ireland',
        'Italy',
        'Latvia',
        'Lithuania',
        'Luxembourg',
        'Malta',
        'Netherlands',
        'Poland',
        'Portugal',
        'Romania',
        'Slovakia',
        'Slovenia',
        'Spain',
        'Sweden']}}},
    {$group: {_id: {Country: "$Country", date_start: "$date_start", date_end: "$date_end"}])


// Q9
db.owid_covid_data.aggregate([
    {$match: {continent: {$in: ['Europe', 'North America']}}}, 
    {$match: {date: {$gte: ISODate('2020-03-30T00:00:00.000+08:00'), $lte: ISODate('2020-08-01T00:00:00.000+08:00')}}},
    {$group: {_id: {continent: "$continent", date: "$date"}, sumTotalNew: {$sum: "$new_cases"}}}
    {$sort: {'_id.date': 1}}
    ])
 
    
// Q10
db.owid_covid_data.aggregate([
    {$match: {continent: {$ne: ""}}},
    {$match: {total_cases: {$gt: 50}}},
    {$match: {new_cases: 0}},
    {$project: {new_cases: 1, location: 1, date: 1, "intervalDate": {$add: ["$date", 14*24*60*60000]}}},
    {$out: "covidStat"}
    ])

// $lookup is slow without indexes
// Idea here is to push documents into an array based on 
// the 14 day interval and if days are consecutive, there
// would be exactly 15 docs (14 + starting day)

db.covidStat.createIndex({"location": 1, "date": 1, "intervalDate": 1})
db.covidStat.aggregate([
    {$lookup: {
        from: "covidStat", 
        let: {loc: "$location", dt: "$date", idt: "$intervalDate"},
        pipeline: [
            {$match: {$expr: {$and: [
                {$eq: ["$location", "$$loc"]},
                {$gte: ["$date", "$$dt"]},
                {$lte: ["$date", "$$idt"]}
                ]
            }}},
            {$project: {_id: 0, continent: 0, total_tests: 0}}
            ],
            as: "intervaldata"
    }},
    {$match: {intervaldata: {$size: 15}}},
    {$group: {_id: "$location"}},
    ])


// Q11
db.owid_covid_data.createIndex({"location": 1, "date": 1})
db.covidStat.aggregate([
    {$lookup: {
        from: "covidStat", 
        let: {loc: "$location", dt: "$date", idt: "$intervalDate"},
        pipeline: [
            {$match: {$expr: {$and: [
                {$eq: ["$location", "$$loc"]},
                {$gte: ["$date", "$$dt"]},
                {$lte: ["$date", "$$idt"]}
                ]
            }}},
            {$project: {_id: 0, continent: 0, total_tests: 0}}
            ],
            as: "intervaldata"
    }
    {$match: {intervaldata: {$size: 15}}},
    {$project: {location: 1, eoi_doc: {$arrayElemAt: ["$intervaldata", -1]}}},
    {$project: {location: 1, "eoi_doc.date": 1, "eow_day": {$add: ["$eoi_doc.date", 8*24*60*60000]}}},
    {$lookup: {
        from: "owid_covid_data",
        let: {loc: "$location", eoi_dt: "$eoi_doc.date", eow_dt: "$eow_day"},
        pipeline: [
            {$match: {$expr: {$and: [
                {$eq: ["$location", "$$loc"]},
                {$gte: ["$date", "$$eoi_dt"]},
                {$lte: ["$date", "$$eow_dt"]}
                ]
            }}},
            {$project: {_id: 0, continent: 0, total_tests: 0, location: 0}}
            ],
            as: "week_newcases"
    }},
    {$unwind: "$week_newcases"},
    {$group: {_id: {location: "$location", eoi_dt: "$eoi_doc.date"}, sum_week_new: {$sum: "$week_newcases.new_cases"}}}
    {$match: {sum_week_new: {$gt: 50}}},
    {$group: {_id: "$_id.location"}}
    ])


// Q12
db.global_mobility_report.updateMany({"grocery_and_pharmacy_percent_change_from_baseline": NaN}, 
                                    {$set: {"grocery_and_pharmacy_percent_change_from_baseline": null}})
                                    
db.global_mobility_report.updateMany({"retail_and_recreation_percent_change_from_baseline": NaN}, 
                                    {$set: {"retail_and_recreation_percent_change_from_baseline": null}})
                                    
db.global_mobility_report.updateMany({"residential_percent_change_from_baseline": NaN},
                                    {$set: {"residential_percent_change_from_baseline": null}})

db.global_mobility_report.updateMany({"parks_percent_change_from_baseline": NaN}, 
                                    {$set: {"parks_percent_change_from_baseline": null}})
                                    
db.global_mobility_report.updateMany({"transit_stations_percent_change_from_baseline": NaN}, 
                                    {$set: {"transit_stations_percent_change_from_baseline": null}})
                                    
db.global_mobility_report.updateMany({"workplaces_percent_change_from_baseline": NaN}, 
                                    {$set: {"workplaces_percent_change_from_baseline": null}})
                                    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_grocery_pharm: {$max: {$abs: "$grocery_and_pharmacy_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_grocery_pharm: -1}},
    {$limit: 3}
    ])
    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_retail_recreat: {$max: {$abs: "$retail_and_recreation_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_retail_recreat: -1}},
    {$limit: 3}
    ])
    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_residential: {$max: {$abs: "$residential_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_residential: -1}},
    {$limit: 3}
    ])
    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_parks: {$max: {$abs: "$parks_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_parks: -1}},
    {$limit: 3}
    ])
    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_transit: {$max: {$abs: "$transit_stations_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_transit: -1}},
    {$limit: 3}
    ])
    
db.global_mobility_report.aggregate([
    {$group: {_id: {country: "$country_region"}, max_cfb_workplaces: {$max: {$abs: "$workplaces_percent_change_from_baseline"}}}},
    {$sort: {max_cfb_workplaces: -1}},
    {$limit: 3}
    ])
    
// Q13
db.global_mobility_report.aggregate([
    {$match: {country_region: "Indonesia"}},
    {$project: {country_region: 1, sub_region_1: 1, sub_region_2: 1, year: {$year: "$date"}, month: {$month: "$date"}, day: {$dayOfMonth: "$date"}, retail_and_recreation_percent_change_from_baseline: 1, workplaces_percent_change_from_baseline: 1, grocery_and_pharmacy_percent_change_from_baseline: 1}},
    {$lookup: {
        from: "owid_covid_data",
        let: {loc: "$country_region"},
        pipeline: [
            {$match: {$expr: {$and: [
                {$eq: ["$location", "$$loc"]},
                {$gt: ["$total_cases", 20000]}
                ]
            }}},
            {$limit: 1},
            {$project: {date: 1, total_cases: 1}}
            ],
            as: "D_dates"
    }},
    {$unwind: "$D_dates"},
    {$project: {country_region: 1, sub_region_1: 1, sub_region_2: 1, year: 1, month: 1, day: 1, retail_and_recreation_percent_change_from_baseline: 1, workplaces_percent_change_from_baseline: 1, grocery_and_pharmacy_percent_change_from_baseline: 1, Dday_year: {$year: "$D_dates.date"}, Dday_month: {$month: "$D_dates.date"}, Dday_day: {$dayOfMonth: "$D_dates.date"}}},
    {$match: {$expr: {$and: [
        {$eq: ["$year", "$Dday_year"]}, 
        {$eq: ["$month", "$Dday_month"]}, 
        {$eq: ["$day", "$Dday_day"]}
        ]}
        }}
    ])