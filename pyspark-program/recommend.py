import findspark
from pyspark.sql import SparkSession
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
import gc
from pyspark.sql.session import SparkSession


findspark.init("/home/hadoop/spark-3.3.0-bin-hadoop3")

spark = SparkSession.builder.appName("recommend").getOrCreate()

full_df = spark.read.format("csv").option("header", True).option("separator", ",").load("hdfs:///new_ratings1.csv").toPandas()

#full_df = pd.read_csv('ratings.csv')
full_df.head()

import pyspark.sql.functions as sql_func
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator

data_schema = StructType([
    StructField('userId',IntegerType(), False),
    StructField('movieId',IntegerType(), False),
    StructField('rating',FloatType(), False),
    StructField('timestamp',TimestampType(), False)
])
final_stat = spark.read.csv(
    'hdfs:///new_ratings.csv', header=True, schema=data_schema
).cache()

ratings = (final_stat
    .select(
        'userId',
        'movieId',
        'rating',
    )
).cache()

(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=2, regParam=0.01, 
          userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop",
          implicitPrefs=True)
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=1, regParam=0.01, 
          userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop",
          implicitPrefs=False) #changed param!
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
userRecs.count()
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)
movieRecs.count()

movieRecs.show(10, False)

userRecs_df = userRecs.toPandas()
print(userRecs_df.shape)

movieRecs_df = movieRecs.toPandas()
print(movieRecs_df.shape)

userRecs_df.head()

movieRecs_df.head()

movieRecs_df.to_csv(path_or_buf='moveRecord.csv')



