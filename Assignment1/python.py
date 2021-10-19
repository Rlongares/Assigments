from pyspark.sql import SparkSession
import sys
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import IntegerType,BooleanType,DateType


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
            df = spark.read.csv(str(i),header = True)
            print("Leido")
        else:
            df2 = spark.read.csv(str(i),header = True)
            df = union(df2)
        count += 1

print(len(df.columns))
print(df.count())

#LISTA DE COLUMNAS INNECESARIAS
colsToDelete = ("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")

#ELIMINAMOS COLUMNAS INNECESARIAS
df = df.drop(*colsToDelete)
print(len(df.columns))

#TRATAMOS LOS TIPOS

print("Changing tipe")
df = df.withColumn("Month",df["Month"].cast(IntegerType()))


"""
for field in df.schema.fields:
    print(field.name +" , "+str(field.dataType))
"""
"""
vectorAssembler = VectorAssembler(inputCols = df.columns, outputCol = 'features')
tdf = vectorAssembler.transform(df)
splits = tdf.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]
lr = LinearRegression(featuresCol = 'features', labelCol='ArrDelay', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
"""
