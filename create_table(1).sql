drop table if exists Covid19_Stat;
drop table if exists Response;
drop table if exists Mobility_Change;
drop table if exists Location;

create table `Location` (
`location_name` varchar(128) not null,
`continent` varchar(64) default "",
primary key (`location_name`)
);

create table `Covid19_Stat` (
`location_name` varchar(128) not null,
`date` date not null,
`total_tests` int default null,
`total_cases` int default null,
`new_cases` int default null,
primary key (`location_name`, `date`),
constraint `location_name_fk`
foreign key (`location_name`) references `Location` (`location_name`)
);
 
create table `Response` (
`country` varchar(128) not null,
`response_measure` varchar(255) not null,
`start_date` date not null,
`end_date` date not null,
primary key (`country`, `response_measure`, `start_date`, `end_date`),
constraint `country_fk`
foreign key (`country`) references `Location` (`location_name`)
);

create table `Mobility_Change` (
`country_region` varchar(64) not null,
`sub_region_1` varchar(128) default "",
`sub_region_2` varchar(128) default "",
`metro_area` varchar(128) default "",
`date` date not null,
`cfb_retail_recreation` int default null,
`cfb_grocery_pharm` int default null,
`cfb_parks` int default null,
`cfb_transit_stations` int default null,
`cfb_workplaces` int default null,
`cfb_residential` int default null,
primary key (`country_region`, `sub_region_1`, `sub_region_2`, `metro_area`, `date`),
constraint `country_region_fk`
foreign key (`country_region`) references `Location` (`location_name`)
);