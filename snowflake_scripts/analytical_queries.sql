use role sysadmin;
use warehouse transform_wh;

-- Top 10 Businesses by Average Rating
SELECT 
    db.BUSINESS_NAME,
    db.CITY,
    db.STATE,
    db.PRIMARY_CATEGORY,
    db.CURRENT_STARS,
    db.CURRENT_REVIEW_COUNT,
    db.STAR_RATING_CATEGORY
FROM YELP_DB.GOLD.DIM_BUSINESS db
WHERE db.IS_OPEN = TRUE
  AND db.CURRENT_REVIEW_COUNT >= 100
ORDER BY db.CURRENT_STARS DESC, db.CURRENT_REVIEW_COUNT DESC
LIMIT 10;

-- User Engagement Analysis
SELECT 
    du.USER_ENGAGEMENT_TIER,
    du.TENURE_BUCKET,
    COUNT(*) AS user_count,
    AVG(du.TOTAL_REVIEWS) AS avg_reviews,
    AVG(du.USER_INFLUENCE_SCORE) AS avg_influence_score
FROM YELP_DB.GOLD.DIM_USER du
GROUP BY du.USER_ENGAGEMENT_TIER, du.TENURE_BUCKET
ORDER BY du.USER_ENGAGEMENT_TIER, du.TENURE_BUCKET;

-- Review Trends Over Time
SELECT 
    dd.YEAR,
    dd.QUARTER,
    COUNT(fr.REVIEW_ID) AS total_reviews,
    AVG(fr.STARS) AS avg_rating,
    SUM(CASE WHEN fr.IS_POSITIVE THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN fr.IS_NEGATIVE THEN 1 ELSE 0 END) AS negative_reviews,
    AVG(fr.ENGAGEMENT_SCORE) AS avg_engagement
FROM YELP_DB.GOLD.FACT_REVIEW fr
JOIN YELP_DB.GOLD.DIM_DATE dd ON fr.REVIEW_DATE_KEY = dd.DATE_KEY
GROUP BY dd.YEAR, dd.QUARTER
ORDER BY dd.YEAR, dd.QUARTER;

-- Business Performance by Category
SELECT 
    db.PRIMARY_CATEGORY,
    COUNT(DISTINCT db.BUSINESS_ID) AS business_count,
    AVG(db.CURRENT_STARS) AS avg_rating,
    SUM(db.CURRENT_REVIEW_COUNT) AS total_reviews,
    COUNT(CASE WHEN db.IS_OPEN THEN 1 END) AS open_count,
    ROUND(COUNT(CASE WHEN db.IS_OPEN THEN 1 END)::DECIMAL / COUNT(*) * 100, 2) AS open_pct
FROM YELP_DB.GOLD.DIM_BUSINESS db
WHERE db.PRIMARY_CATEGORY IS NOT NULL
GROUP BY db.PRIMARY_CATEGORY
HAVING COUNT(DISTINCT db.BUSINESS_ID) >= 10
ORDER BY business_count DESC
LIMIT 20;

-- Elite vs Non-Elite User Comparison
SELECT 
    du.IS_ELITE,
    COUNT(DISTINCT du.USER_ID) AS user_count,
    AVG(du.TOTAL_REVIEWS) AS avg_reviews,
    AVG(du.AVERAGE_STARS_GIVEN) AS avg_stars_given,
    AVG(du.FAN_COUNT) AS avg_fans,
    AVG(du.USER_INFLUENCE_SCORE) AS avg_influence_score
FROM YELP_DB.GOLD.DIM_USER du
GROUP BY du.IS_ELITE;

-- Weekend vs Weekday Review Patterns
SELECT 
    dd.IS_WEEKEND,
    COUNT(fr.REVIEW_ID) AS review_count,
    AVG(fr.STARS) AS avg_rating,
    AVG(fr.REVIEW_WORD_COUNT) AS avg_word_count,
    AVG(fr.TOTAL_VOTES) AS avg_votes
FROM YELP_DB.GOLD.FACT_REVIEW fr
JOIN YELP_DB.GOLD.DIM_DATE dd ON fr.REVIEW_DATE_KEY = dd.DATE_KEY
GROUP BY dd.IS_WEEKEND;