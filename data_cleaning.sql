-- Data Cleaning

CREATE DATABASE layoffs;


-- 1. Removing Duplicates


CREATE TABLE layoffs_staging -- Create staging table to keep raw data safe
LIKE layoffs;

INSERT layoffs_staging -- Copy raw data into staging table
SELECT *
FROM layoffs;

WITH duplicate_cte AS (  -- Finding duplicate rows by partitioning over each column
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location,
    industry, total_laid_off, percentage_laid_off, `date`,
    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE layoffs_staging2  -- Creating another staging table to add row_num from the partitioning
LIKE layoffs_staging;

ALTER TABLE layoffs_staging2  -- Adding row_num column
ADD COLUMN row_num int AFTER funds_raised_millions;

INSERT INTO layoffs_staging2  -- Inserting data from partitioning into new staging table
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location,
    industry, total_laid_off, percentage_laid_off, `date`,
    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE  -- Deleting duplicate rows based on the partitioning results
FROM layoffs_staging2
WHERE row_num > 1;


-- 2. Standardizing data


UPDATE layoffs_staging2  -- Removing whitespace
SET company = TRIM(company);

UPDATE layoffs_staging2  -- Standardizing industry names
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2  -- Standardizing countries
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2  -- Changing date from string to date format
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` NOT LIKE 'NULL';

ALTER TABLE layoffs_staging2  -- Changing data type of date from text to date
MODIFY COLUMN `date` DATE;


-- 3. Dealing with blank and null values


UPDATE layoffs_staging2 t1  -- Updating missing industry information based on existing information from other rows
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry != '');


-- 4. Removing Columns and Rows


DELETE  -- Delete empty data which won't be helpful for/would ruin the analysis
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2  -- Deleting row_colum from the partition
DROP COLUMN row_num;













