SELECT
  snapshot_time,
  COUNT(*) AS total_canceled_trains
FROM trains
WHERE snapshot_time = '2025-09-02 16:00:00'
  AND (arrival_canceled = TRUE OR departure_canceled = TRUE)
GROUP BY snapshot_time;
