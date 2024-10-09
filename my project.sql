-- data cleaning 
use layoffs;

select * from layoffs ;

CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT * FROM layoffs;

insert into layoffs2
select * ,
ROW_NUMBER() OVER(PARTITION BY company, industry, 
total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs;


SELECT COMPANY , TRIM(COMPANY)FROM LAYOFFS2;

UPDATE LAYOFFS2 SET COMPANY=TRIM(COMPANY);


use layoffs;

SELECT COMPANY FROM LAYOFFS2;
SELECT INDUSTRY FROM LAYOFFS2
GROUP BY INDUSTRY;

UPDATE LAYOFFS2 SET INDUSTRY ="CRYOTO%" WHERE INDUSTRY LIKE "CRYPTO%";
 
SELECT DISTINCT COUNTRY ,TRIM( TRAILING'.' FROM COUNTRY)
FROM  LAYOFFS2 ORDER BY 1; 

UPDATE layoffs2 SET COUNTRY = TRIM(TRAILING'.' FROM COUNTRY)
WHERE COUNTRY LIKE 'UNITED STATES&';

SELECT*FROM LAYOFFS2;
INSERT INTO LAYOFFS2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY COMPANY,INDUSTRY, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF,'DATE') AS ROW_NUM FROM LAYOFFS;




SELECT * FROM LAYOFFS2 WHERE ROW_NUM>1;
DELETE FROM LAYOFFS2 WHERE ROW_NUM>1;




select distinct location
from layoffs2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs2
order by 1;

update layoffs2 
set country=trim(trailing '.' from country)
where country like 'united states%';

select distinct country
from layoffs2
order by 1;

select date
from layoffs2;

update layoffs2
set date = str_to_date(date, '%m/%d/%Y');

alter table layoffs2
modify column `date` date;

select *
from layoffs2 
where industry is null 
or industry = '';

update layoffs2
set industry = null 
where industry='';

select * from layoffs2
where industry is null;

SELECT `date` FROM layoffs2;

select * from  layoffs2;


select t1. industry, t2. industry
from layoffs2 t1
join layoffs2 t2
on t1. company=t2.company
and t1.location=t2.location
where (t1.industry is null or t1.industry=' ')
and t2.industry is not null;



update layoffs2 t1
join layoffs2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

select industry from layoffs2;

select *
from layoffs2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from layoffs2
where total_laid_off is null 
and percentage_laid_off is null; 

select *
from layoffs2;
alter table layoffs2
drop column row_num;

-- 1. How many layoffs happened per country?
select country , sum(total_laid_off)
as total_layoffs
from layoffs2
group by country 
order by total_layoffs desc
limit 10;


-- 2. which industry faced the highest number 
-- of layoffs ?

select industry , sum(total_laid_off)
as total_layoffs 
from layoffs2
group by industry 
order by total_layoffs desc;

-- 3. which companies laid off more than a 
-- certain threshold of emplpoyees (e.g., 1000)?
select company , total_laid_off
from layoffs2
where total_laid_off>1000
order by total_laid_off desc;

-- 3. which companies laid off more than a 
-- certain threshold of emplpoyees (e.g., 1000)?
select company , total_laid_off
from layoffs2
where total_laid_off>5000
order by total_laid_off desc;


-- 4. what is the average percentage of workforce
select industry, AVG(percentage_laid_off) AS avg_percentage_laid_off
from layoffs2
group by industry 
order by avg_percentage_laid_off desc;


-- 5. what are the top 10 companies with 
-- the most layoffs?
select company , total_laid_off
from layoffs2
order by total_laid_off desc
limit 10;

-- 6. what is the total number of layoffs
-- across all companies?
select SUM(total_laid_off) AS total_offs
from layoffs2;

-- 7. how many layoffs occurred in each funding 
-- stage(e.g., post-IPO, series B)?
SELECT stage, COUNT(*) AS num_layoffs
from layoffs2
group by stage 
order by num_layoffs desc;

-- 8. which country raised the highest amount of 
select country, SUM(funds_raised_millions) 
as total_fund_raised
from layoffs2
group by country 
order by total_fund_raised desc
limit 10;

-- 9. which companies had layoffs with 
-- missing data for percentage laid off?
select company total_laid_off
from layoffs2
where percentage_laid_off is null 
order by total_laid_off desc;

-- 10. how many layoffs occurred in a 
-- specific time period (e.g., 2023)?
select count(*) as layoffs_in_2023
from layoffs2
where YEAR(date) = 2023;


-- 11. which countries have the higest percentage of layoffs relative to their total workforce ? 

select country, round(avg(percentage_laid_off),2) as avg_percentage_laid_off 
from layoffs2
group by country
order by avg_percentage_laid_off desc
limit 10; 


-- 12. what is the distribution of layoffs accross different locations (cities)? 

select location,count(*) as num_offs 
from layoffs2 
group by location 
order by num_offs desc; 

-- 13. what are the top 5 industries that raised the most funds and had layoffs ? 

select industry,sum(funds_raised_millions) 
as total_funds_raised 
from layoffs2
where total_laid_off>0 
group by industry 
order by total_funds_raised desc 
limit 5; 


-- 14. maximum of total_laid_off and percentage_laid_off ? 

select max(total_laid_off), 
max(percentage_laid_off)
FROM layoffs2; 


-- 15. layoffs from start date to end date ? 

select min(date),max(date)
from layoffs2;

-- 16.  Sum of total_laid_off by year? 

select year(date),sum(total_laid_off) 
from layoffs2 
group by year(date) 
order by 1 desc;


select industry,stage, sum(total_laid_off)
as total_layoffs
from layoffs2
group by industry , stage
order by industry;

