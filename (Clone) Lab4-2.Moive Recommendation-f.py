# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark HW3 Moive Recommendation (Unity Catalog Version)
# MAGIC In this notebook, we will use an Alternating Least Squares (ALS) algorithm with Spark APIs to predict the ratings for the movies in [MovieLens small dataset](https://grouplens.org/datasets/movielens/latest/), loaded from a Unity Catalog Volume.
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import math

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part1: Data ETL and Data Exploration

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("movie_analysis_uc") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# COMMAND ----------

# Define the base path for your data in Unity Catalog Volumes
uc_volume_path = "/Volumes/1dt_team8_managed/movie/movie"

# Load data from Unity Catalog Volume
try:
    movies = spark.read.load(f"{uc_volume_path}/movies.csv", format='csv', header=True, inferSchema=True)
    ratings = spark.read.load(f"{uc_volume_path}/ratings.csv", format='csv', header=True, inferSchema=True)
    links = spark.read.load(f"{uc_volume_path}/links.csv", format='csv', header=True, inferSchema=True)
    tags = spark.read.load(f"{uc_volume_path}/tags.csv", format='csv', header=True, inferSchema=True)
    print("Data loaded successfully from Unity Catalog Volume.")
except Exception as e:
    print(f"Error loading data from Unity Catalog Volume: {e}")
    print(f"Please ensure CSV files (movies.csv, ratings.csv, links.csv, tags.csv) exist in {uc_volume_path}")
    # dbutils.fs.ls(uc_volume_path) # Uncomment to list files in the volume for debugging

# COMMAND ----------

movies.show(5)
#display(movies.limit(5))

# COMMAND ----------

ratings.show(5)

# COMMAND ----------

# Ensure ratings DataFrame is loaded before proceeding
if 'ratings' in locals():
    tmp1 = ratings.groupBy("userID").count().toPandas()['count'].min()
    tmp2 = ratings.groupBy("movieId").count().toPandas()['count'].min()
    print('For the users that rated movies and the movies that were rated:')
    print('Minimum number of ratings per user is {}'.format(tmp1))
    print('Minimum number of ratings per movie is {}'.format(tmp2))
else:
    print("Ratings DataFrame not loaded. Please check the data loading step.")

# COMMAND ----------

if 'ratings' in locals():
    tmp1 = sum(ratings.groupBy("movieId").count().toPandas()['count'] == 1)
    tmp2 = ratings.select('movieId').distinct().count()
    print('{} out of {} movies are rated by only one user'.format(tmp1, tmp2))
else:
    print("Ratings DataFrame not loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Spark SQL and OLAP

# COMMAND ----------

# MAGIC %md ### The number of Users

# COMMAND ----------

if 'ratings' in locals():
    tmp_q1 = ratings.select('userid').distinct().count()
    print('There totally have {} users'.format(tmp_q1))
else:
    print("Ratings DataFrame not loaded.")

# COMMAND ----------

# MAGIC %md ### The number of Movies

# COMMAND ----------

if 'movies' in locals():
    tmp_q2 = movies.select('movieid').distinct().count()
    print('There totally have {} movies'.format(tmp_q2))
else:
    print("Movies DataFrame not loaded.")

# COMMAND ----------

# MAGIC %md ### How many movies are rated by users? List movies not rated before

# COMMAND ----------

from pyspark.sql.functions import col, isnan # isnan might not be needed if rating is null

if 'movies' in locals() and 'ratings' in locals():
    tmp_q3_rated_count = ratings.select('movieid').distinct().count()
    total_movies_count = movies.select('movieid').distinct().count() # Re-calculate or use tmp_q2
    print('{} movies have not been rated'.format(total_movies_count - tmp_q3_rated_count))

    # Join movies with ratings to find movies with no ratings
    # Ensure movieId columns are of the same type if join issues occur. inferSchema should help.
    movies_with_ratings = movies.join(ratings, movies.movieId == ratings.movieId, "left_outer")

    # Select movies where the rating (from the ratings table) is null
    unrated_movies = movies_with_ratings.where(ratings.rating.isNull()) \
                                      .select(movies.movieId, movies.title).distinct() # Added distinct
    print("\nList of movies not rated before:")
    unrated_movies.show()
else:
    print("Movies or Ratings DataFrame not loaded.")

# COMMAND ----------

# MAGIC %md ### List Movie Genres

# COMMAND ----------

import pyspark.sql.functions as f

if 'movies' in locals():
    # Explode the genres string into an array, then explode the array into multiple rows
    all_genres_df = movies.withColumn("genre_array", f.split(col("genres"), "\|")) \
                          .select(f.explode(col("genre_array")).alias("genre")) \
                          .distinct()

    distinct_genres_list = [row.genre for row in all_genres_df.collect()]
    hashset = set(distinct_genres_list) 

    print("Distinct genres found:")
    print(hashset)
    print("Total number of distinct genres: {}".format(len(hashset)))
else:
    print("Movies DataFrame not loaded.")

# COMMAND ----------

# MAGIC %md ### Movie for Each Category
# MAGIC This part creates a one-hot encoded representation for genres.
# MAGIC The original implementation was a bit complex. A more PySpark-idiomatic way is to use pivot or conditional aggregation.

# COMMAND ----------

from pyspark.sql.functions import expr, when

if 'movies' in locals() and 'hashset' in locals() and len(hashset) > 0:
    q5_base = movies.select("movieid", "title", "genres")

    # Create a column for each genre, with 1 if the movie has that genre, 0 otherwise
    # This approach is more scalable and idiomatic in Spark than manual list iteration for DataFrame construction
    genre_expressions = [
        when(col("genres").rlike(genre.replace("(", "\\(").replace(")", "\\)")), 1).otherwise(0).alias(genre)
        for genre in hashset if genre != '(no genres listed)' # Handle special characters if any in genre names for alias
    ]
    # Add (no genres listed) separately if it exists, ensuring valid alias
    if '(no genres listed)' in hashset:
        genre_expressions.append(
            when(col("genres") == '(no genres listed)', 1).otherwise(0).alias("no_genres_listed")
        )

    if genre_expressions: # Check if there are any genres to process
        tmp_q5 = q5_base.select(col("movieid"), col("title"), *genre_expressions)
        print("\nMovies with one-hot encoded genres:")
        tmp_q5.show()

        # Example: List "Drama" movies
        # Adjust the alias if 'Drama' was changed (e.g., due to special characters)
        drama_alias = "Drama" # Assuming 'Drama' is a valid alias
        if drama_alias in tmp_q5.columns:
            tmp_drama = tmp_q5.filter(col(drama_alias) == 1).select("movieid", "title")
            print("\n{} movies are Drama, they are:".format(tmp_drama.count()))
            tmp_drama.show()
        else:
            print(f"\nColumn '{drama_alias}' not found. Available columns: {tmp_q5.columns}")
    else:
        print("\nNo genres found to process for one-hot encoding.")

else:
    print("Movies DataFrame or genre set not available for one-hot encoding.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part2: Spark ALS based approach for training model
# MAGIC We will use an RDD-based API from [pyspark.mllib](https://spark.apache.org/docs/2.1.1/mllib-collaborative-filtering.html) (older API) and then switch to MLlib's DataFrame-based API for ALS.
# MAGIC
# MAGIC Note: The original code uses `sc.textFile` which is for RDDs. For consistency and modern Spark, it's often better to read into a DataFrame first and then convert to RDD if absolutely necessary. However, we'll keep `sc.textFile` for this part to match the original structure, but load from the UC Volume.

# COMMAND ----------

# Load ratings data using Spark DataFrame API from Unity Catalog Volume

# Define the base path for your data in Unity Catalog Volumes
uc_volume_path = "/Volumes/dbricks_jhlee/default/movie"

# Load ratings data using Spark DataFrame API from Unity Catalog Volume
# This approach is compatible with Databricks Serverless compute.
ratings_file_path_uc = f"{uc_volume_path}/ratings.csv"
from pyspark.sql.functions import col

try:
    # 1. Read CSV into a DataFrame
    ratings_df_initial = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ratings_file_path_uc)

    # 2. Select necessary columns and explicitly cast to desired types.
    #    The original RDD code effectively took (userId, movieId, rating).
    #    We drop rows if any of these key columns are null after casting.
    #    This DataFrame will be directly used for splitting.
    ratings_for_split_df = ratings_df_initial.select(
        col("userId").cast("integer"),
        col("movieId").cast("integer"),
        col("rating").cast("float")
    ).na.drop() # Drop rows where userId, movieId, or rating is null

    print("Ratings data loaded into DataFrame for splitting:")
    ratings_for_split_df.show(5)
    ratings_for_split_df.printSchema()

except Exception as e:
    print(f"Error loading ratings.csv into DataFrame from {ratings_file_path_uc}: {e}")
    ratings_for_split_df = None # Ensure variable is defined for checks below

# COMMAND ----------

# MAGIC %md Now we split the data into training/validation/testing sets using a 6/2/2 ratio.

# COMMAND ----------

if ratings_for_split_df:
    # Split the DataFrame directly
    (train_df, validation_df, test_df) = ratings_for_split_df.randomSplit([0.6, 0.2, 0.2], seed=7856)

    # Cache the DataFrames for performance
    train_df.cache()
    validation_df.cache()
    test_df.cache()

    print("Data split into Training, Validation, and Test DataFrames.")
    print(f"Training DataFrame count: {train_df.count()}")
    print(f"Validation DataFrame count: {validation_df.count()}")
    print(f"Test DataFrame count: {test_df.count()}")
    print("\nSchema of training DataFrame:")
    train_df.printSchema()
    train_df.show(3)
else:
    print("ratings_for_split_df DataFrame not available for splitting.")
    # Define empty DataFrames or handle error appropriately if needed downstream
    train_df, validation_df, test_df = [spark.createDataFrame([], ratings_for_split_df.schema if ratings_for_split_df else spark.read.format("csv").load(ratings_file_path_uc).schema) for _ in range(3)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALS Model Selection and Evaluation
# MAGIC
# MAGIC With the ALS model, we can use a grid search to find the optimal hyperparameters.
# MAGIC We will now use the DataFrame-based API for ALS (`pyspark.ml.recommendation.ALS`), which is generally preferred.

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

def train_ALS_df(train_data, validation_data, num_iters, reg_params, ranks_list): # Renamed ranks to ranks_list to avoid conflict
    min_error = float('inf')
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank_val in ranks_list: # Use rank_val
        for reg in reg_params:
            als = ALS(rank=rank_val, maxIter=num_iters, regParam=reg,
                      userCol="userId", itemCol="movieId", ratingCol="rating",
                      coldStartStrategy="drop", # Important for handling new users/items in validation/test
                      seed=42) # Added seed for reproducibility
            try:
                model = als.fit(train_data)
                predictions = model.transform(validation_data)
                
                # Remove NaNs from predictions if any, as evaluator cannot handle them
                predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())
                
                evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
                
                if predictions_cleaned.count() == 0:
                    rmse_error = float('inf') # Or handle as a failed case
                    print(f"Warning: No valid predictions for rank={rank_val}, reg={reg}. All predictions were NaN or validation set was empty after coldStartStrategy.")
                else:
                    rmse_error = evaluator.evaluate(predictions_cleaned)

                print('Rank = {}, Regularization = {}: Validation RMSE = {}'.format(rank_val, reg, rmse_error))
                
                if rmse_error < min_error:
                    min_error = rmse_error
                    best_rank = rank_val
                    best_regularization = reg
                    best_model = model
            except Exception as e:
                print(f"Error training ALS with rank={rank_val}, reg={reg}: {e}")
                continue # Skip to next hyperparameter combination

    if best_model:
        print('\nThe best model has {} latent factors and regularization = {}'.format(best_rank, best_regularization))
    else:
        print('\nCould not find a best model. All training attempts failed or produced no valid predictions.')
    return best_model

# COMMAND ----------

if 'train_df' in locals() and 'validation_df' in locals():
    num_iterations = 10
    ranks_param = [6, 8, 10, 12] # Renamed from 'ranks'
    reg_params_param = [0.05, 0.1, 0.2, 0.4] # Renamed from 'reg_params'
    import time

    start_time = time.time()
    final_model = train_ALS_df(train_df, validation_df, num_iterations, reg_params_param, ranks_param)
    print('Total Runtime for Hyperparameter Tuning: {:.2f} seconds'.format(time.time() - start_time))
else:
    print("Training and validation DataFrames (train_df, validation_df) are not available.")
    final_model = None


# COMMAND ----------

# MAGIC %md
# MAGIC ### Learning Curve
# MAGIC The original `plot_learning_curve` used scikit-learn's `learning_curve` which is not directly applicable here.
# MAGIC We need to manually iterate through `maxIter` for ALS.

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import math # math.isinf 사용 가능

def train_ALS_df(train_data, validation_data, num_iters, reg_params_list, ranks_list):
    min_error = float('inf')
    best_rank_val = -1
    best_reg_param_val = 0.0 # float으로 초기화
    best_model_instance = None

    print(f"\n--- Starting Hyperparameter Tuning for ALS ---")
    print(f"Ranks to test: {ranks_list}")
    print(f"Regularization params to test: {reg_params_list}")
    print(f"Number of iterations for each model: {num_iters}")

    if train_data.count() == 0:
        print("Error: Training data is empty. Cannot train model.")
        return None, -1, 0.0
    if validation_data.count() == 0:
        print("Warning: Validation data is empty. RMSE will be Inf or NaN.")


    for rank_val_iter in ranks_list:
        for reg_param_iter in reg_params_list:
            print(f"  Training with Rank: {rank_val_iter}, RegParam: {reg_param_iter}")
            als = ALS(rank=rank_val_iter, maxIter=num_iters, regParam=reg_param_iter,
                      userCol="userId", itemCol="movieId", ratingCol="rating",
                      coldStartStrategy="drop", # NaN 평가 메트릭 방지
                      seed=42) # 재현성을 위한 시드 값
            try:
                model = als.fit(train_data)
                
                if validation_data.count() > 0: # 검증 데이터가 있을 때만 평가
                    predictions = model.transform(validation_data)
                    predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())
                    
                    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
                    
                    if predictions_cleaned.count() == 0:
                        # coldStartStrategy="drop"으로 인해 모든 검증 데이터가 제거된 경우
                        rmse_error = float('inf') 
                        print(f"    Warning: No valid predictions for rank={rank_val_iter}, reg={reg_param_iter} on validation set. All predictions were NaN or validation set became empty.")
                    else:
                        rmse_error = evaluator.evaluate(predictions_cleaned)
                else: # 검증 데이터가 처음부터 비어있는 경우
                    rmse_error = float('inf')
                    print(f"    Warning: Validation data is empty. RMSE for rank={rank_val_iter}, reg={reg_param_iter} is Inf.")


                print(f"    Rank = {rank_val_iter}, Regularization = {reg_param_iter}: Validation RMSE = {rmse_error}")
                
                # math.isinf 와 math.isnan 을 사용하여 유효한 에러 값인지 확인
                if not math.isinf(rmse_error) and not math.isnan(rmse_error) and rmse_error < min_error:
                    min_error = rmse_error
                    best_rank_val = rank_val_iter
                    best_reg_param_val = reg_param_iter
                    best_model_instance = model
                    print(f"    New best model found! RMSE: {min_error:.4f}, Rank: {best_rank_val}, Reg: {best_reg_param_val}")

            except Exception as e:
                print(f"    Error training ALS with rank={rank_val_iter}, reg={reg_param_iter}: {e}")
                continue

    if best_model_instance:
        print(f"\n--- Hyperparameter Tuning Finished ---")
        print(f"The best model has {best_rank_val} latent factors and regularization = {best_reg_param_val:.4f} with RMSE = {min_error:.4f}")
    else:
        print(f"\n--- Hyperparameter Tuning Finished ---")
        print("Could not find a best model. All training attempts might have failed or produced invalid RMSEs.")
    
    return best_model_instance, best_rank_val, best_reg_param_val

# COMMAND ----------

import matplotlib.pyplot as plt # 함수 내에서 plt를 사용하므로 임포트 확인
from pyspark.ml.recommendation import ALS # ALS 임포트 확인
from pyspark.ml.evaluation import RegressionEvaluator # Evaluator 임포트 확인

def plot_als_learning_curve(iter_array, train_data, validation_data, reg, rank_val):
    iter_num_plot, rmse_plot = [], []

    for iter_val in iter_array:
        als = ALS(rank=rank_val, maxIter=iter_val, regParam=reg,
                userCol="userId", itemCol="movieId", ratingCol="rating",
                coldStartStrategy="drop", seed=42)
        try:
            model = als.fit(train_data)
            predictions = model.transform(validation_data)
            # 누락된 예측값(NaN)을 가진 행은 평가 전에 제거하거나 처리해야 합니다.
            predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())

            evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
            
            if predictions_cleaned.count() == 0:
                # 모든 예측이 NaN이거나 validation_data가 coldStartStrategy에 의해 비워진 경우
                rmse_error = float('nan') 
                print(f"Warning: No valid predictions for iterations={iter_val}, rank={rank_val}, reg={reg}. All predictions were NaN or validation set was empty after coldStartStrategy.")
            else:
                rmse_error = evaluator.evaluate(predictions_cleaned)
            
            print('Iterations = {}, Rank = {}, Regularization = {}: Validation RMSE = {}'.format(iter_val, rank_val, reg, rmse_error))
            
            iter_num_plot.append(iter_val)
            rmse_plot.append(rmse_error)
        except Exception as e:
            print(f"Error training ALS for learning curve with iterations={iter_val}, rank={rank_val}, reg={reg}: {e}")
            iter_num_plot.append(iter_val)
            rmse_plot.append(float('nan')) # 오류 발생 시 NaN 추가
            continue

    # 데이터 유효성 검사 (플로팅 전)
    if not any(math.isfinite(x) for x in rmse_plot if isinstance(x, float)): # rmse_plot에 유효한 숫자가 있는지 확인
        print("No valid RMSE values to plot. Learning curve will not be generated.")
        return # 유효한 데이터가 없으면 플로팅하지 않음

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 6)) 
    ax.plot(iter_num_plot, rmse_plot, marker='o', linestyle='--')

    ax.set_xlabel("Number of Iterations")
    ax.set_ylabel("Validation RMSE")
    ax.set_title(f"ALS Learning Curve (Rank={rank_val}, RegParam={reg})")
    ax.set_xticks(iter_array) # x축 눈금을 iter_array 값으로 설정
    ax.grid(True)

    # Databricks 노트북에서 matplotlib 그림을 표시하려면 display() 함수를 사용합니다.
    display(fig)

    plt.show

# COMMAND ----------

if 'train_df' in locals() and 'validation_df' in locals():
    iter_array_plot = [1, 2, 5, 10, 15] # Extended a bit
    # Use parameters from the best model, or pick specific ones
    # If final_model exists and has these attributes (depends on how it's stored/returned)
    best_reg = 0.2 # Example, or final_model.bestRegParam if available
    best_rnk = 10  # Example, or final_model.bestRank if available
    
    if final_model is not None:
        print(f"Plotting learning curve for Rank={best_rnk}, RegParam={best_reg}")
        plot_als_learning_curve(iter_array_plot, train_df, validation_df, best_reg, best_rnk)
    else: # Fallback if final_model was not trained successfully
        print("final_model not available. Plotting with example parameters (Rank=10, Reg=0.2).")
        plot_als_learning_curve(iter_array_plot, train_df, validation_df, 0.2, 10)

else:
    print("Training and validation DataFrames (train_df, validation_df) are not available for plotting learning curve.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model testing
# MAGIC And finally, make a prediction and check the testing error using the `final_model`.

# COMMAND ----------

if final_model and 'test_df' in locals():
    predictions_test = final_model.transform(test_df)
    predictions_test_cleaned = predictions_test.filter(predictions_test.prediction.isNotNull())

    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    
    if predictions_test_cleaned.count() == 0:
        rmse_test = float('inf')
        print("Warning: No valid predictions on the test set.")
    else:
        rmse_test = evaluator.evaluate(predictions_test_cleaned)
    
    print("Test Set Root-mean-square error = " + str(rmse_test))

    # Generate top 10 movie recommendations for each user
    try:
        userRecs = final_model.recommendForAllUsers(10)
        print("\nTop 10 movie recommendations for each user (sample):")
        #userRecs.show(5, truncate=False)
        display(userRecs)
    except Exception as e:
        print(f"Error generating user recommendations: {e}")


    # Generate top 10 user recommendations for each movie
    try:
        movieRecs = final_model.recommendForAllItems(10)
        print("\nTop 10 user recommendations for each movie (sample):")
        #movieRecs.show(5, truncate=False)
        display(movieRecs)
    except Exception as e:
        print(f"Error generating movie recommendations: {e}")

else:
    print("Final model or test DataFrame (test_df) is not available for testing.")
