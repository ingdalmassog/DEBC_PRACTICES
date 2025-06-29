import logging
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import to_timestamp, to_date, date_format, lower
# from pyspark.sql import HiveContext
# from pyspark.sql.functions import lower


def clean_car_rental_data(raw_df):
    # Casting and filtering the data
    clean_df = raw_df.select(
                lower(raw_df["fuelType"]).alias("fuelType"),
                (raw_df["rating"]*100).cast('int').alias("rating"),
                raw_df["renterTripsTaken"].cast('int').alias("renterTripsTaken"),
                raw_df["reviewCount"].cast('int').alias("reviewCount"),
                raw_df["`location.city`"].alias("city"),
                raw_df["`location.country`"].alias("country"),
                # raw_df["`location.latitude`"].alias("latitude"),
                # (raw_df["`location.latitude`"]*1000000).cast('int').alias("latitude_int"),
                # raw_df["`location.longitude`"].alias("longitude"),
                # (raw_df["`location.longitude`"]*1000000).cast('int').alias("longitude_int"),
                raw_df["`location.state`"].alias("state_code"),
                raw_df["`owner.id`"].cast('int').alias("owner_id"),
                raw_df["`rate.daily`"].cast('int').alias("rate_daily"),
                raw_df["`vehicle.make`"].alias("make"),
                raw_df["`vehicle.model`"].alias("model"),
                raw_df["`vehicle.year`"].cast('int').alias("year"),
                #raw_df["`vehicle.type`"].alias("type") 
                ).filter((raw_df["rating"].isNotNull()) & (raw_df["`location.state`"] != 'TX'))
    
    # Fill the Nulls with 0
    return clean_df 


def clean_car_rental_geo_data(raw_df):
    # Casting and filtering the data
    df1 = raw_df.select(
                # raw_df["Geo Point"].alias("geo_point"),
                # raw_df["Geo Shape"].alias("geo_shape"),
                # raw_df["Year"].cast('int'),
                raw_df["Official Name State"].alias("state_name"),
                raw_df["Iso 3166-3 Area Code"].alias("country"),
                raw_df["United States Postal Service state abbreviation"].alias("state_code"),
                ).filter((raw_df["United States Postal Service state abbreviation"]!='TX') & (raw_df["Iso 3166-3 Area Code"] == 'USA'))
   
    return df1


def main_process():
        
    try:
        #Create a Spark session
        # spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()
        sc = SparkContext('local')
        spark = SparkSession(sc)
        hc = HiveContext(sc)
        logging.info('###########################Started spark session!...')
    except Exception as e:
        logging.error("############################An error occurred starting the Spark session!!")
        logging.error(e.__str__())
        sys.exit(1) # Added to ensure an error output in Airflow
        
    # Load the .csv from HDFS in a df taking the header to get the column names
    rdf_crd = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
    rdf_gd = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")
    # rdf_f2022 = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
    # rdf_a = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")


    # Clean airports df
    dfa = clean_car_rental_data(rdf_crd)
    dfg = clean_car_rental_geo_data(rdf_gd)
    
    dfc = dfa.join(dfg, on="state_code", how="left")
    
    dff = dfc.select(
                    dfc["fuelType"],
                    dfc["rating"],
                    dfc["renterTripsTaken"],
                    dfc["reviewCount"],
                    dfc["city"],
                    dfc["state_name"],
                    dfc["owner_id"],
                    dfc["rate_daily"],
                    dfc["make"],
                    dfc["model"],
                    dfc["year"]
                    #dfc["`location.country`"].alias("country"),
                    # raw_df["`location.latitude`"].alias("latitude"),
                    # (raw_df["`location.latitude`"]*1000000).cast('int').alias("latitude_int"),
                    # raw_df["`location.longitude`"].alias("longitude"),
                    # (raw_df["`location.longitude`"]*1000000).cast('int').alias("longitude_int"),
                    #dfc["`location.state`"].alias("state_code"),
                    #raw_df["`vehicle.type`"].alias("type") 
                )
    
    
    dff.createOrReplaceTempView("car_rental_analytics")

    # dfa.createOrReplaceTempView("temp_car_rental_data")
    # df1.createOrReplaceTempView("temp_geo")
    
    #LOAD DATA IN DW(HIVE)
    try:
        #Uncomment to run the first time. 
        # hc.sql("CREATE TABLE CAR_RENTAL_DB.temp_geo_car_rental_data AS SELECT * FROM temp_geo;")
        # hc.sql("CREATE TABLE CAR_RENTAL_DB.temp_car_rental_data AS SELECT * FROM temp_car_rental_data;")
        # hc.sql("CREATE TABLE CAR_RENTAL_DB.car_rental_analytics AS SELECT * FROM car_rental_analytics;")
        
        hc.sql("TRUNCATE TABLE CAR_RENTAL_DB.car_rental_analytics;")
        hc.sql("\
                INSERT INTO CAR_RENTAL_DB.car_rental_analytics\
                    SELECT * FROM car_rental_analytics;\
           ")
    
        logging.info("ADDED DATA IN HIVE!")
        sys.exit(0)
    except Exception as e: 
        logging.error("#######An error occurred loAding the data in Hive!!")
        logging.error(e.__str__())
        sys.exit(1) # Added to ensure an error output in Airflow


if __name__ == "__main__": 
    main_process()





