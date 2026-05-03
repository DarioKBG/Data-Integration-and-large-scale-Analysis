-- we can use postgit to be even more precise

SELECT 
	station_name,
    station_number,
    latitude,
    longitude,
    sqrt(power(latitude - 52.531976, 2) + power(longitude - 13.209125, 2)) AS distance
	
FROM stations
ORDER BY distance
LIMIT 1;


-- test coordinates
--52.418841  13.314288 osdorferstr
-- 52.531976  13.209125 stresow 