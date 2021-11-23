from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("VectorAssemblerExample")\
        .getOrCreate()

    # $example on$

    dataset = spark.createDataFrame(
        [(0, 18, 1.0,"a",Vectors.dense([0.0, 10.0, 0.5]), 1.0),(1, 18, 1.0,"b",Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
        ["id", "hour", "mobile","letras", "userFeatures", "clicked"])


    String = StringIndexer(inputCol="letras", outputCol="categoryIndex")

    indexed = String.fit(dataset).transform(dataset)
    print("\n\n Voy a printear el indexed")
    indexed.show()

    assembler = VectorAssembler(
        inputCols=["hour", "mobile","categoryIndex", "userFeatures"],
        outputCol="features")

    output = assembler.transform(indexed)
    print("\n\n Voy a printear el output del vector assembler")
    output.show()
    print("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(truncate=False)
    # $example off$


    dataset = spark.createDataFrame(
        [(0, 18, 1.0,"a","aa","aaa",Vectors.dense([0.0, 10.0, 0.5]), 1.0),(1, 18, 1.0,"b","bb","bbb",Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
        ["id", "hour", "mobile","letras","letras2","letras3","userFeatures", "clicked"])
    print("PRUEBA VARIOS ")
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(dataset) for column in ("letras","letras2","letras3") ]

    pipeline = Pipeline(stages=indexers)
    df_r = pipeline.fit(dataset).transform(dataset)
    df_r.show()
    
    spark.stop()
