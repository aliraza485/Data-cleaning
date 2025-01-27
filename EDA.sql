-- DATA EXPLORATORY PROJECT --
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_stagging2;
layoffs
SELECT*
FROM layoffs_stagging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)layoffs_stagging
FROM layoffs_stagging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_stagging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT SUBSTRING(`date`,6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_stagging2
WHERE SUBSTRING(`date`,6,2) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,6,2) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_stagging2
WHERE SUBSTRING(`date`,6,2) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM ROlling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

WITH Company_Year(company,Years,total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT*, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL)
SELECT*
FROM Company_Year_Rank
WHERE Ranking <= 5;