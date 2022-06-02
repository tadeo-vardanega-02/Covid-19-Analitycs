# Analisis de COVID 19 

## Para este analisis se utilizo el lenguaje de programacion Python, el framework de Big data Spark

### Librerias y componentes necesarios para el proyecto:

* pyspark

* findspark

```
pip install pyspark, findspark, matplotlib
```

### Este es un analisis que tiene el objetivo de practicar Spark

### Explicacion de los datasets:

### Son datos de casos y muertes del covid 19 en el mundo, con fecha del 02-08-2020 y 08-08-2020

### Columnas:

* Date Rep: Fecha de los casos

* Day: Dia de los casos

* Month: Mes de los casos

* Year: Año de los casos (2020)

* Cases: Numero de casos

* Deaths: Numero de muertes

* Countries and Territories: Nombre de los paises

* Geo Id: Id de los paises

* Country Territory: Abreviatura de los paises

* Pop data 2019: Poblacion en 2019 de cada pais

* Continent exp: Continente

* Cumulative_number_for_14_days_of_COVID-19_cases_per_100000: Numero de casos de covid 19 en los primeros 14 dias

### Utilice 2 datasets con dias de diferencia para poder comparar la evolucion de los casos y muertes
