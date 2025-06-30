SHOW databases;


-- CREATE DATABASE CAR_RENTAL_DB;

USE CAR_RENTAL_DB;

SHOW TABLES;

----------------------------------------------------------------------------   FIRTS ANALYSIS --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
    -- count(*) -- 5.350
    *
    -- DISTINCT
    --     country
FROM
    CAR_RENTAL_DB.temp_car_rental_data;

DROP TABLE CAR_RENTAL_DB.temp_car_rental_data;

-- TRUNCATE TABLE FLIGHTS_DB.aeropuerto_detalles_tabla;


-- DROP TABLE FLIGHTS_DB.aeropuerto_detalle;

SELECT 
    *
    -- count(*) -- 483.122
FROM
    CAR_RENTAL_DB.temp_geo_car_rental_data;
    
WITH CTEA AS (  
    SELECT
        /*
        owner_id 
        ,city 
        ,COUNTRY
        ,
        */
        *
        -- ,row_number() OVER(PARTITION BY LATITUDE_INT, LATITUDE_INT ORDER BY LATITUDE_INT, LATITUDE_INT) AS RN
        ,row_number() OVER(PARTITION BY LATITUDE_INT, longitude_INT, make, model, vehicle_year ORDER BY LATITUDE_INT, Longitude_INT) AS RN 
    FROM 
        CAR_RENTAL_DB.temp_car_rental_data
)
SELECT 
    * 
FROM 
    CTEA
WHERE 
    RN > 1
ORDER BY 
    RN DESC
    
    
WITH RENTAL_DATA AS (
    SELECT 
        *
        -- COUNT(*) -- 5.350 --> 4.905
    FROM 
        CAR_RENTAL_DB.temp_car_rental_data
    WHERE 
        state_name <> 'TX'
)
,GEO_DATA AS (
    SELECT 
        *
        --DISTINCT 
        --    state_code
    FROM 
        CAR_RENTAL_DB.temp_geo_car_rental_data
    WHERE
        country = 'USA'
        and STATE_CODE <> 'TX' 
)
SELECT 
    R.latitude_int
    ,R.longitude_int
    ,R.STATE_NAME
    ,G.GEO_POINT
FROM 
    RENTAL_DATA as R 
    LEFT JOIN 
        GEO_DATA as G
        ON G.STATE_CODE = R.STATE_NAME    
    -- latitude_INT = 32041311
    -- AND longitude_INT = -81228966
    -- make = TOYOTA
    -- model = Supra
    -- vehicle_year = 2020
    -- OWNER_ID = 2459011
    -- AND MAKE = 'Toyota' 
    -- AND MODEL = 'Supra'
    
----------------------------------------------------------------------------   FIRTS ANALYSIS --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        
-------------------------------------------------------------------------------- ANALYSIS ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        
SELECT 
    *
    -- COUNT(*) -- 4.905
FROM 
    CAR_RENTAL_DB.car_rental_analytics
        
DESCRIBE CAR_RENTAL_DB.car_rental_analytics; 

------

-- 5.a
-- Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4

SELECT 
    SUM(rentertripstaken) AS Q_R -- 26.944
FROM 
    CAR_RENTAL_DB.car_rental_analytics
WHERE 
    FUELTYPE IN ('electric', 'hybrid')
    AND RATING >= 400
    

--  5.b
-- los 5 estados con menor cantidad de alquileres (mostrar query y visualización)
    
SELECT 
    state_name 
    ,SUM(rentertripstaken) AS Q_RENTALS
FROM 
    CAR_RENTAL_DB.car_rental_analytics
GROUP BY 
    state_name     
ORDER BY 
    Q_RENTALS
    ,state_name 
LIMIT 5

-- 5.c
-- los 10 modelos (junto con su marca) de autos más rentados (mostrar query y visualización)

SELECT 
    MAKE
    ,MODEL
    ,COUNT(*) AS Q_RENTALS
FROM 
    CAR_RENTAL_DB.car_rental_analytics
GROUP BY 
    MAKE
    ,MODEL
ORDER BY 
    Q_RENTALS DESC
LIMIT 10
    

SELECT 
    *
FROM 
    CAR_RENTAL_DB.car_rental_analytics
