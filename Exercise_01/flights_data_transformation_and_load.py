import logging
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, date_format
from pyspark.sql import HiveContext





def clean_flights_data(raw_df):
    # Casting and filtering the data
    raw_df = raw_df.select(
                to_date(raw_df["Fecha"], format="dd/MM/yyyy").alias("fecha"), 
                date_format(to_timestamp(raw_df["Hora UTC"], format="HH:mm"), "HH:mm").alias("horaUTC"),
                raw_df["Clase de Vuelo (todos los vuelos)"].alias("clase_de_vuelo"),
                raw_df["Clasificación Vuelo"].alias("clasificacion_de_vuelo"),
                raw_df["Tipo de Movimiento"].alias("tipo_de_movimiento"),
                raw_df["Aeropuerto"].alias("aeropuerto"),
                raw_df["Origen / Destino"].alias("origen_destino"),
                raw_df["Aerolinea Nombre"].alias("aerolinea_nombre"),
                raw_df["Aeronave"].alias("aeronave"),
                raw_df["Pasajeros"].cast('int').alias("pasajeros")
                ).filter((raw_df["Clasificación Vuelo"]=='Domestico') | (raw_df["Clasificación Vuelo"]=='Doméstico'))
    # Fill the Nulls with 0
    clean_df = raw_df.na.fill(value = 0, subset = 'pasajeros')
    return clean_df 


def clean_airports_data(raw_df):
    # Casting and filtering the data
    raw_df = raw_df.select(
                raw_df["local"].alias("aeropuerto"),
                raw_df["oaci"].alias("oac"),
                raw_df["iata"],
                raw_df["tipo"],
                raw_df["denominacion"],
                raw_df["coordenadas"],
                raw_df["latitud"],
                raw_df["longitud"],
                raw_df["elev"].cast('float'),
                raw_df["uom_elev"],
                raw_df["ref"],
                raw_df["distancia_ref"].cast('float'),
                raw_df["direccion_ref"],
                raw_df["condicion"],
                raw_df["control"],
                raw_df["region"],
                raw_df["uso"],
                raw_df["trafico"],
                raw_df["sna"],
                raw_df["concesionado"],
                raw_df["provincia"]
                )
    # Fill the Nulls with 0
    clean_df = raw_df.na.fill(value = 0, subset = 'distancia_ref')
   
    return clean_df


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
    rdf_f2021 = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
    rdf_f2022 = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
    rdf_a = spark.read.option("sep", ";").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

    # Clean flights df
    df2021 = clean_flights_data(rdf_f2021)
    df2022 = clean_flights_data(rdf_f2022)

    #Append both flights df 
    dff = df2021.unionAll(df2022)
    # dff = dff.orderBy(dff.fecha.desc())

    # Clean airports df
    dfa = clean_airports_data(rdf_a)

    dfa.createOrReplaceTempView("temp_aeropuertos")
    
    dff.createOrReplaceTempView("temp_flights")
    
    #LOAD DATA IN DW(HIVE)
    try:
        #Uncomment to run the first time. 
        # hc.sql("CREATE TABLE AS SELECT * FROM temp_aeropuertos;")
        
        hc.sql("TRUNCATE TABLE FLIGHTS_DB.aeropuerto_detalles_tabla;")
        hc.sql("\
                INSERT INTO FLIGHTS_DB.aeropuerto_detalles_tabla\
                    SELECT * FROM temp_aeropuertos;\
           ")
        
        #Uncomment to run the first time. 
        # hc.sql("CREATE TABLE AS SELECT * FROM temp_flights;")
        
        hc.sql("TRUNCATE TABLE FLIGHTS_DB.aeropuerto_tabla;")
        hc.sql("\
                INSERT INTO FLIGHTS_DB.aeropuerto_tabla\
                    SELECT * FROM temp_flights;\
            ")

        logging.info("ADDED DATA IN HIVE TABLES!")
        sys.exit(0)
    except Exception as e: 
        logging.error("#######An error occurred loAding the data in Hive!!")
        logging.error(e.__str__())
        sys.exit(1) # Added to ensure an error output in Airflow


if __name__ == "__main__": 
    main_process()





