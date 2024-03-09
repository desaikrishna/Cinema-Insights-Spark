# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___genres.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
genres_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(genres_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___director.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
director_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(director_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___movie_director.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movie_director_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(movie_director_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___movie_cast.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movie_cast_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(movie_cast_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___movie_genres.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movie_genres_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(movie_genres_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___Actor.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
actor_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# display(actor_df)


cols = ("_c4","_c5","_c6","_c7","_c8","_c9")

actor_df=actor_df.drop(*cols)
actor_df = actor_df.dropna(subset=["act_id"])

display(actor_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___movie_reviewer.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movie_reviewer_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(movie_reviewer_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___movies.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movies_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(movies_df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movies/Movies___rating.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
movies_rating_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(movies_rating_df)
movies_rating_df.printSchema()

# COMMAND ----------

# Create a view or table
# dbutils.fs.mv("/FileStore/tables/Movies___movie_genres.csv", "/FileStore/tables/movies/Movies___movie_genres.csv")
dbutils.fs.ls("/FileStore/tables/movies")
# temp_table_name = "Movies-1_xlsx"
# df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

#1 Which year was American Beauty released
movies_df.where(movies_df.mov_title == "American Beauty").select("mov_title","mov_year").show()

# COMMAND ----------

#2 movies released before 1999 and in the year 199
#movies_df.where(movies_df.mov_year == "1999").select("mov_title","mov_year").show()
movies_df.where(movies_df.mov_year < "1999").select("mov_title","mov_year").show()

# COMMAND ----------

genres_df.union(movie_reviewer_df).show()

# COMMAND ----------

movie_reviewer_df.printSchema()
movies_rating_df.printSchema()

# COMMAND ----------

#3 Movies where the rating provided has more than 7 review stars
# movies_rating_df.where(movies_rating_df.rev_stars >= 7).select("rev_id").show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# result = movie_reviewer_df.filter(col("rev_id").isin(movies_rating_df.filter(col("rev_stars") > 7).select("rev_id").distinct().rdd.flatMap(lambda x: x).collect())).select("rev_id", "rev_name").show()
# result = xyz_df.filter(col("rev_id")
#                        .isin(abc_df
#                              .filter(col("stars") > 7)
#                              .select("rev_id")
#                              .distinct()
#                              .collect()))
#                              .select("rev_id", "rev_name")
movies_rating_df.join(movie_reviewer_df,movie_reviewer_df.rev_id == movies_rating_df.rev_id).filter((movies_rating_df.rev_stars >= 7) & (movie_reviewer_df.rev_name.isNotNull())).select(movie_reviewer_df.rev_name).show()

# COMMAND ----------

#4 Working with isin operator and where
#movies_df.where(movies_df.mov_id.isin('905','907','917')).select("mov_title").show()
#movies_df.where(movies_df.mov_title == 'Boogie Nights').select("mov_id").show()
actor_df.where((actor_df.act_fname == 'Woody') & (actor_df.act_lname == 'Allen')).select("act_id").show()

# COMMAND ----------

#5
mov_id=movies_df.filter(movies_df.mov_title=='Annie Hall').select("mov_id")
# value = mov_id.collect()
mov_id_value = mov_id.first()["mov_id"]
act_id=movie_cast_df.filter(movie_cast_df.mov_id == mov_id_value).select("act_id").first()["act_id"]
# act_id_value = act_id.first()["act_id"]
actor_df.filter(actor_df.act_id==act_id_value).select("act_fname","act_lname").show()

#6 Get actor_id of actor who has acted in Annie Hall
from pyspark.sql.functions import col
movie_cast_df.filter(col("mov_id") == (movies_df.filter(col("mov_title")=='Annie Hall').select("mov_id").distinct().first()["mov_id"])).select("act_id").show()

# COMMAND ----------

#7 Firstname of actor who is in move Annie Hall
actor_df.filter(
    actor_df.act_id == (
        movie_cast_df.filter(
            movie_cast_df.mov_id == (
                movies_df.filter(movies_df.mov_title == 'Annie Hall').select("mov_id").first()["mov_id"]
            )
        ).select("act_id").first()["act_id"]
    )
).select("act_fname").show()

# COMMAND ----------

director_df.printSchema()
movie_director_df.printSchema()
movie_cast_df.printSchema()
# actor_df.filter(
#     actor_df.act_id == (
#         movie_cast_df.filter(
#             movie_cast_df.mov_id == (
#                 movies_df.filter(movies_df.mov_title == 'Annie Hall').select("mov_id").first()["mov_id"]
#             )
#         ).select("act_id").first()["act_id"]
#     )
# ).select("act_fname").show()

# COMMAND ----------

#8 
director_df.filter(
    director_df.dir_id == (
        movie_director_df.filter(
            movie_director_df.mov_id == (
                movie_cast_df.filter(
                    movie_cast_df.role == 'John Scottie Ferguson').select("mov_id").first()["mov_id"])).select("dir_id").first()["dir_id"])).select("dir_fname").show()

# COMMAND ----------

# movie_cast_df.filter(movie_cast_df.role=='John Scottie Ferguson').select("mov_id").first()["mov_id"]
movie_director_df.filter(movie_director_df.mov_id=='901').select("mov_id").first()["mov_id"]

# COMMAND ----------

#9
director_df.filter(
    director_df.dir_id == (
        movie_director_df.filter(
            movie_director_df.mov_id == (
                movie_cast_df.filter(
                    movie_cast_df.role == 'John Scottie Ferguson').select("mov_id").first()["mov_id"])).select("dir_id").first()["dir_id"])).select("dir_fname").show()

# COMMAND ----------

#10
director_df.filter(
    director_df.dir_id == (
        movie_director_df.filter(
            movie_director_df.mov_id == (
                movie_cast_df.filter(
                    movie_cast_df.mov_id == (
                        movies_df.filter(
                            movies_df.mov_title == 'Eyes Wide Shut').select("mov_id").first()["mov_id"])).select("mov_id").first()["mov_id"])).select("dir_id").first()["dir_id"])).select("dir_fname","dir_lname").show()