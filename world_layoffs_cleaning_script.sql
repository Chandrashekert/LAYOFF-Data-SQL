-- Data Cleaning
-- select * from layoffs;

-- CREATING DUPLICATE TABLE
-- create table layoffs_staging like layoffs;
-- insert layoffs_staging select * from layoffs

-- 1. remove duplicates
WITH duplicate_cte AS
(SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;
SELECT * FROM layoffs_staging
WHERE company = "Casper";
DELETE FROM duplicate_cte WHERE row_num > 1; -- CANNOT do this in a CTE, DELETE IS AN UPDATE.
-- Starting point:
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

select * FROM layoffs_staging2 WHERE row_num > 1; -- can see duplicates are gone.

-- 2. standardize data
-- Removing Spaces in company names
SELECT DISTINCT(company), TRIM(company)
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Changing all CryptoCurrency type names to just Crypto
SELECT DISTINCT industry FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';
UPDATE layoffs_staging2 SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardizing US. to US : TRIM DOESNT FIX THE PERIOD BUT CAN USE TRIM(TRAILING '.' FROM country)
-- SELECT DISTINCT country FROM layoffs_staging2;
UPDATE layoffs_staging2 SET country = 'United States'
WHERE country LIKE 'United States%';

-- Changing format of DATE from text to date
SELECT `date` 
-- STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;
UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); -- Modifying the DATE FORMAT STILL TEXT HERE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NUll or Blank values
SELECT * from layoffs_staging2 WHERE industry IS NULL OR industry = '';
SELECT * FROM layoffs_staging2 WHERE company = 'Airbnb';
-- Missing + NULL Values for Industry can technically be populated with available DATA
UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL) AND t2.industry IS NOT NULL;

SELECT * from layoffs_staging2 WHERE industry IS NULL OR industry = ''; -- Check WORK
-- Bally's Interactive does not have Industry Data so will stay NULL

-- 4. remove unnecessary columns or rows
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; -- Can get rid of these because total/percentage layoff information is unavailable & undecided
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; -- Deleted

ALTER TABLE layoffs_staging2 DROP COLUMN row_num; -- Drop table

select * from layoffs_staging2; -- Final Dataset (Removed Duplicates, Standardized Data, Null Values edited, Removed Rows/Columns)
