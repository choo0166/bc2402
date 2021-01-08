-- Queries 

-- Q1
select distinct
    location_name
from
    Location
where
    continent = 'Asia';

-- Q2
select distinct
    location_name
from
    Location
        natural join
    Covid19_Stat
where
    continent in ('Asia' , 'Europe')
        and `date` = '2020-04-01'
        and total_cases > 10;

-- Q3
select distinct
    location_name
from
    Location
        natural join
    Covid19_Stat
where
    continent = 'Africa'
        and total_cases < 10000
        and `date` between '2020-04-01' and '2020-04-20';
        
-- Q4
select 
    location_name
from
    Location
        natural join
    Covid19_Stat
group by location_name
having sum(total_tests) is null;

-- Q5
select 
    location_name,
    year(`date`) as `Year`,
    month(`date`) as `Month`,
    sum(new_cases) as total_new
from
    Covid19_Stat
group by location_name , year(`date`) , month(`date`);

-- Q6
select 
    continent,
    year(`date`) as `Year`,
    month(`date`) as `Month`,
    sum(new_cases) as total_new
from
    Location
        natural join
    Covid19_Stat
where continent != ''
group by continent , year(`date`) , month(`date`)
order by continent , year(`date`) , month(`date`);

-- Q7
select distinct
    country
from
    Response
where
    country in ('Austria' , 'Belgium',
        'Bulgaria',
        'Croatia',
        'Cyprus',
        'Czech Republic',
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
        'Sweden')
        and response_measure like '%mask%';

-- Q8
select 
    start_date, end_date
from
    Response
where
    response_measure = 'MasksMandatory' and 
    country in ('Austria' , 'Belgium',
        'Bulgaria',
        'Croatia',
        'Cyprus',
        'Czech Republic',
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
        'Sweden')
group by start_date, end_date
order by count(*) desc
limit 1;

-- Q9
-- Date interval from Q8
select 
    continent, `date`, sum(new_cases) as total_new
from
    Location
        natural join
    Covid19_Stat
where
    `date` between '2020-05-04' and '2020-08-01'
        and continent in ('Europe' , 'North America')
group by continent , `date`
order by `date`;

-- Q10
drop procedure if exists createcountryList;
delimiter $$
create procedure createcountryList()
begin 
	declare finished integer default 0;
    declare prevDate date default null;
    declare currDate date default null;
    declare prevloc text default "";
    declare currloc text default "";
    declare counter integer default 1;
    
    declare curDaysCount cursor for select 
    location_name, `date`
	from
		Location natural join Covid19_Stat
	where
		new_cases = 0 and total_cases > 50
	order by location_name , `date`;
    
    -- declare NOT FOUND handler
    declare continue handler for not found set finished = 1;
    
    open curDaysCount;
    fetch curDaysCount into prevloc, prevDate;
    
    getcountry: loop
		fetch curDaysCount into currloc, currDate;
        
        if currloc in (select * from result_TBL) then iterate getcountry;
        end if;
        
        if finished = 1 then leave getcountry; 
        end if;
        
        if datediff(currDate, prevDate) = 1 and prevloc = currloc then set counter = counter + 1;
        else set counter = 1;
        end if;
        
        if counter > 14 then insert into result_TBL (select currloc);
        end if;
        
        set prevloc = currloc;
        set prevDate = currDate;
	end loop getcountry;
    
    select * from result_TBL;
    
    close curDaysCount;
end $$
delimiter ;

drop temporary table if exists result_TBL;
create temporary table result_TBL (
`location_name` varchar(128)
);
call createcountryList();

-- Q11
drop procedure if exists createcountryList2;
delimiter $$
create procedure createcountryList2()
begin 
	declare finished integer default 0;
    declare prevDate date default null;
    declare currDate date default null;
    declare date1 date default null;
    declare date2 date default null;
    declare prevloc text default "";
    declare currloc text default "";
    declare counter integer default 1;
    declare week_new integer default 0;
    
    declare curDaysCount cursor for select 
    location_name, `date`
	from
		Covid19_Stat
	where
		new_cases = 0.0 and total_cases > 50
	order by location_name , `date`;
    
    -- declare NOT FOUND handler
    declare continue handler for not found set finished = 1;
    
    open curDaysCount;
    fetch curDaysCount into prevloc, prevDate;
    
    getcountry: loop
		fetch curDaysCount into currloc, currDate;
        
        if currloc in (select * from result_TBL_2) then iterate getcountry;
        end if;
        
        if finished = 1 then leave getcountry; 
        end if;
        
        if datediff(currDate, prevDate) = 1 and prevloc = currloc then set counter = counter + 1;
        else set counter = 1;
        end if;
                
        if counter >= 14 then 
			set date1 = date_add(currDate, interval 1 day);
            set date2 = date_add(date1, interval 6 day);
            
            -- insert into log_TBL (select concat('location is: ', currloc, ' date1 is: ', date1, ' date2 is:', date2));
            
            select sum(new_cases) into week_new from `Covid19_Stat` where location_name = currloc and `date` between date1 and date2;
            -- insert into log_TBL (select concat('week_new is: ', week_new));
            
            if week_new > 50 then insert into result_TBL_2 (select currloc);
            end if;
        end if;
        
        set prevloc = currloc;
        set prevDate = currDate;
	end loop getcountry;
    
    select * from result_TBL_2;
    
    close curDaysCount;
end $$
delimiter ;

drop temporary table if exists result_TBL_2;
create temporary table result_TBL_2 (
`location_name` varchar(128)
);
-- For debugging
-- create temporary table if not exists log_TBL (
-- `log_msg` varchar(255) 
-- );
call createcountryList2();
-- select * from log_TBL;

-- Q12
select 
    country_region as retail_recreat
from
    Mobility_Change
group by country_region
order by max(abs(cfb_retail_recreation)) desc
limit 3;

select 
    country_region as grocery_pharm
from
    Mobility_Change
group by country_region
order by max(abs(cfb_grocery_pharm)) desc
limit 3;

select 
    country_region as parks
from
    Mobility_Change
group by country_region
order by max(abs(cfb_parks)) desc
limit 3;

select 
    country_region as transit_station
from
    Mobility_Change
group by country_region
order by max(abs(cfb_transit_stations)) desc
limit 3;

select 
    country_region as workplaces
from
    Mobility_Change
group by country_region
order by max(abs(cfb_workplaces)) desc
limit 3;

select 
    country_region as residential
from
    Mobility_Change
group by country_region
order by max(abs(cfb_residential)) desc
limit 3;

-- Q13
select 
    mc.country_region,
    mc.`date` as D_day,
    mc.sub_region_1,
    mc.sub_region_2,
    mc.metro_area,
    mc.cfb_retail_recreation,
    mc.cfb_grocery_pharm,
    mc.cfb_workplaces
from
    Mobility_Change as mc
        inner join
    (select 
        location_name, min(`date`) as D_day
    from
        Covid19_Stat
    where
        total_cases > 20000
    group by location_name) as df ON mc.country_region = df.location_name
        and mc.`date` = df.D_day
        and mc.country_region = 'Indonesia';