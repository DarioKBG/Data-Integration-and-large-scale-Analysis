-- given station_name it returns its coordinates and identifier(ifopt)

SELECT 
	station_name, 
	station_number, 
	ifopt, 
	longitude, 
	latitude
	
FROM stations
WHERE station_name ILIKE 'Altglienicke';