SELECT
  station_name,
  AVG(arrival_delay) AS avg_arrival_delay,
  AVG(departure_delay) AS avg_departure_delay
FROM
  trains
WHERE
  station_name = 'Ahrensfelde'
GROUP BY
  station_name;
