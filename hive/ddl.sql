-- AgriPredict Hive warehouse (batch layer)
CREATE DATABASE IF NOT EXISTS agripredict;

CREATE TABLE IF NOT EXISTS agripredict.yield_predictions (
  event_id STRING,
  farm_id STRING,
  event_ts STRING,
  rainfall_mm DOUBLE,
  avg_temp_c DOUBLE,
  sunlight_hours DOUBLE,
  soil_ph DOUBLE,
  organic_carbon_pct DOUBLE,
  nitrogen_kg_ha DOUBLE,
  irrigation INT,
  farm_size_ha DOUBLE,
  predicted_yield_t_ha DOUBLE,
  status STRING
)
PARTITIONED BY (season STRING, yr INT)
STORED AS ORC;
