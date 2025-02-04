-- DATA CLEANING --

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize The Data 
-- 3. Null Values or Blank Values
-- 4. Remove Useless Columns

-- 1. Remove Duplicates

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;


select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

-- CTE
with duplicate_cte as 
(select *,
row_number() over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off, 'date', stage, 
 country, funds_raised_millions) as row_num
from layoffs_staging)
select * 
from duplicate_cte
where row_num > 1 ;

-- SUB
select * from (
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage, 
country, funds_raised_millions) as row_num
from layoffs_staging) as duplicate_table  
 where row_num > 1;


select *
from layoffs_staging
where company = 'Better.com';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage, 
country, funds_raised_millions) as row_num
from layoffs_staging ;

select * 
from layoffs_staging2
where row_num > 1;


-- SET SQL_SAFE_UPDATES = 0;


delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2
where row_num > 1;


select *
from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- 2. Standardize The Data
 
SET SQL_SAFE_UPDATES = 0;

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = 'United States'
where country like 'United States%';
 
 -- OTHER WAY --
 
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

--------------------------------------------------

select *
from layoffs_staging2
where country like 'United States.';

select * from layoffs_staging2;

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2 
modify column `date` date;

select *
from layoffs_staging2;

-- 3. Null Values or Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
and funds_raised_millions is null;

select * 
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = ''; 	# can replace this by -- and (t2.industry is not null or t2.industry != '') --

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select * 
from layoffs_staging2
where industry like 'entertainment%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select count(company) 
from layoffs_staging2;

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.total_laid_off is not null 
    and t2.total_laid_off is null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.total_laid_off is null 
    and t2.total_laid_off is not null
set t1.total_laid_off = t1.percentage_laid_off *
 ((t2.total_laid_off / t2.percentage_laid_off) - 
 t2.total_laid_off)
where t1.percentage_laid_off is not null
and t1.`date` > t2.`date`;

select * 
from layoffs_staging2 
where total_laid_off is null;