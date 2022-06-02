import findspark
findspark.init() 
from pyspark.sql import SparkSession, SQLContext


# Covid-19 2020-08-02

# Iniciamos Spark de manera local
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Analisis Covid19 08-02")
         .getOrCreate())

# Leemos el dataset
df = spark.read.csv('COVID-19-geographic-disbtribution-worldwide-2020-08-02.csv',
                    inferSchema=True, header=True)


# Eliminamos columnas que no son relevantes en ningun analisis
df_filtrado = df.drop('geoId', 'countryterritoryCode')

# Procesamiento de los datos

# Casos y muertes por dia y mes

df_filtrado2 = df_filtrado.drop('year', 'countriesAndTerritories',
                                'popData2019', 'continentExp', 'month'
                                'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000')

df_filtrado2.show()


# Suma total de casos y de muertes por dia

df_filtrado2.groupBy("dateRep").sum("cases").show(truncate=False)

df_filtrado2.groupBy("dateRep").sum("deaths").show(truncate=False)

# Casos y muertes por pais

df_filtrado3 = df_filtrado.drop('year', 'dateRep',
                                'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000',
                                'month', 'day')

df_filtrado3.show()

# Suma total de casos y de muertes por pais

df_filtrado3.groupBy("countriesAndTerritories").sum("cases")\
                    .show(truncate=False)

df_filtrado3.groupBy("countriesAndTerritories").sum("deaths")\
                    .show(truncate=False)

# Suma de casos y muertes total

df.agg({'cases': 'sum'}).show()

df.agg({'deaths': 'sum'}).show()


# Creamos tablas SQL para subir a diferentes bases de datos

# Dataframe sin cambios
df.registerTempTable('df')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM df
    """
).show()

# Dataframe de casos y muertes por dia y mes
df_filtrado2.registerTempTable('Casos_muertes_por_dia_y_mes')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM Casos_muertes_por_dia_y_mes
    """
).show()

# Dataframe de casos y muertes por pais
df_filtrado3.registerTempTable('Casos_muertes_por_pais')

sqlContext = SQLContext(spark)

sqlContext.sql(
    """
        SELECT * FROM Casos_muertes_por_pais
    """
).show()
