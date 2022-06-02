import findspark
findspark.init()
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, SQLContext


# Covid-19 2020-08-08

# Iniciamos Spark de manera local
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Analisis Covid19 08-08")
         .getOrCreate())


# Leemos el dataset
df2 = spark.read.csv('COVID-19-geographic-disbtribution-worldwide-2020-08-08.csv',
                     inferSchema=True, header=True)

# Eliminamos columnas que no son relevantes en ningun analisis
df2_filtrado = df2.drop('geoId', 'countryterritoryCode')

# Procesamiento de los datos

# Casos y muertes por dia y mes

df2_filtrado2 = df2_filtrado.drop('year', 'countriesAndTerritories',
                                  'popData2019', 'continentExp', 'month'
                                  'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000')

df2_filtrado2.show()

# Suma total de casos y de muertes por dia

df2_filtrado2.groupBy("dateRep").sum("cases").show(truncate=False)

df2_filtrado2.groupBy("dateRep").sum("deaths").show(truncate=False)

# Casos y muertes por pais

df2_filtrado3 = df2_filtrado.drop('year', 'dateRep',
                                  'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000',
                                  'month', 'day')

df2_filtrado3.show()


# Suma total de casos y de muertes por pais
df2_filtrado3.groupBy("countriesAndTerritories").sum("cases")\
                      .show(truncate=False)

df2_filtrado3.groupBy("countriesAndTerritories").sum("deaths")\
                      .show(truncate=False)


# Suma de casos y muertes total

df2.agg({'cases': 'sum'}).show()

df2.agg({'deaths': 'sum'}).show()


# Creamos tablas SQL para subir a diferentes bases de datos

# Dataframe sin cambios
df2.registerTempTable('df')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM df
    """
).show()

# Dataframe de casos y muertes por dia y mes
df2_filtrado2.registerTempTable('Casos_muertes_por_dia_y_mes')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM Casos_muertes_por_dia_y_mes
    """
).show()

# Dataframe de casos y muertes por pais
df2_filtrado3.registerTempTable('Casos_muertes_por_pais')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM Casos_muertes_por_pais
    """
).show()
