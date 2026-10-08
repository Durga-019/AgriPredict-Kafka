-- name: yield_by_farm_season
SELECT farm_id, season, COUNT(*) AS events,
       ROUND(AVG(predicted_yield_t_ha), 3) AS avg_yield,
       ROUND(MIN(predicted_yield_t_ha), 3) AS min_yield,
       ROUND(MAX(predicted_yield_t_ha), 3) AS max_yield
FROM agripredict.yield_predictions
WHERE status = 'OK'
GROUP BY farm_id, season
ORDER BY farm_id, season;

-- name: season_summary
SELECT season, COUNT(*) AS events, COUNT(DISTINCT farm_id) AS farms,
       ROUND(AVG(predicted_yield_t_ha), 3) AS avg_yield,
       ROUND(AVG(rainfall_mm), 1) AS avg_rainfall,
       ROUND(AVG(nitrogen_kg_ha), 1) AS avg_nitrogen
FROM agripredict.yield_predictions
WHERE status = 'OK'
GROUP BY season;

-- name: top_farms
SELECT farm_id, ROUND(AVG(predicted_yield_t_ha), 3) AS avg_yield,
       ROUND(SUM(predicted_yield_t_ha * farm_size_ha), 2) AS est_total_tonnes
FROM agripredict.yield_predictions
WHERE status = 'OK'
GROUP BY farm_id
ORDER BY avg_yield DESC
LIMIT 10;

-- name: irrigation_impact
SELECT season, irrigation, COUNT(*) AS events,
       ROUND(AVG(predicted_yield_t_ha), 3) AS avg_yield
FROM agripredict.yield_predictions
WHERE status = 'OK'
GROUP BY season, irrigation
ORDER BY season, irrigation;

-- name: data_quality
SELECT farm_id, SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END) AS invalid_events,
       COUNT(*) AS total_events
FROM agripredict.yield_predictions
GROUP BY farm_id
ORDER BY invalid_events DESC;
