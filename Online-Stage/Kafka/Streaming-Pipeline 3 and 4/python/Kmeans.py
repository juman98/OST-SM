from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScalerModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
 
 
spark = SparkSession.builder.appName("KMeansPrediction").getOrCreate()
 
model = KMeansModel.load('model_2010-07-02')
#scaler = MinMaxScalerModel.load('scaler_2010-02-07')
def clustering(message):
 
    df = spark.createDataFrame([message])
    #feature_columns = ['AveragePowerFlow']
 
    #vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
 
    #feature_df = vector_assembler.transform(df)
 
    #scaled = scaler.transform(feature_df)
    #scaled = scaled.withColumn("NormalizedPowerFlowArray", vector_to_array("NormalizedPowerFlowValue"))
 
    #scaled = scaled.withColumn("NormalizedPowerFlowValue", col("NormalizedPowerFlowArray")[0])
 
    #scaled = scaled.drop("NormalizedPowerFlowArray")
 
    #scaled = scaled.drop("features")
    assembler = VectorAssembler(inputCols=["AveragePowerFlow"], outputCol="features")
 
    feature_df = assembler.transform(df)
 
    predict = model.transform(feature_df)
 
    return predict.collect()[0]["cluster"]