from pyspark.sql import SparkSession
import sys
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
from pyspark.ml.feature import VectorSlicer
from pyspark.sql.functions import concat, col, lit


csvlist =[]

#We create a list with the csv that were arguments.
n = len(sys.argv)
for i in range(1, n):
    csvlist.append(sys.argv[i])
print(csvlist)

#The spark session is created here.
spark = SparkSession.builder.appName('abc').getOrCreate()

#Check that the argument list is not empty
if not csvlist:
    print("There were no arguments to read, please add arguments like python nameoffile.py 1987.csv.")
    sys.exit(1)
#If the list was not empty we add all the csv that were arguments as a dataframe to work with them.
else:
    for i in csvlist:
        count = 1
        if count == 1:
            df = spark.read.csv(str(i),header = True, inferSchema = True)
            print("Leido")
        else:
            df2 = spark.read.csv(str(i),header = True, inferSchema = True)
            df = union(df2)
        count += 1


#LISTA DE COLUMNAS INNECESARIAS
colsToDelete = ("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")

#ELIMINAMOS COLUMNAS INNECESARIAS
df = df.drop(*colsToDelete)


#JUNTAMOS COLUMNAS  YEAR MONTH DAY
df = df.withColumn("Year",df["Year"].cast(StringType()))
df = df.withColumn("Month",df["Month"].cast(StringType()))
df = df.withColumn("DayofMonth",df["DayofMonth"].cast(StringType()))
df = df.withColumn("Date", concat(col("DayofMonth"), lit("-"), col("Month"), lit("-"), col("Year")))

print(df.head())
#TRATAMOS LOS TIPOS
#Hay que escalar los datos - Normalizar
"""
print("Changing tipe")
#df = df.withColumn("Month",df["Month"].cast(IntegerType()))

for column in df.columns:
    df = df.withColumn(str(column),df[str(column)].cast(IntegerType()))
"""
arr = ""
for field in df.schema.fields:
    #df = df.withColumn(field.name,df[field.name].cast(IntegerType()))
    print(field.name +" , "+str(field.dataType))

    arr = arr+ " " + field.name
arr = arr.split()
