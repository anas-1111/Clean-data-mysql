-- EDA

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select *
from layoffs_staging2
where company = 'Amazon';

select min(`date`), max(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select substring(`date`, 1, 7) as `Monthly`, sum(total_laid_off)
from layoffs_staging2
group by 1 
order by 1 asc;


with lay_cte as (
select substring(`date`, 1, 7) as `Monthly`, sum(total_laid_off) total_offs
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by 1 
order by 1 asc
)
select `Monthly`,
total_offs,
sum(total_offs) 
over(
order by `Monthly`) as rolling
from lay_cte;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by 1, 2
order by 3 desc;


with
company_year(company, years, total_laid_off) as
(select company,
year(`date`),
sum(total_laid_off)
from layoffs_staging2
group by 1, 2),
company_rank as
(select *,
dense_rank() over(partition by years order by total_laid_off desc) rank_num
from company_year
where years is not null)
select *
from company_rank
where rank_num <= 5;