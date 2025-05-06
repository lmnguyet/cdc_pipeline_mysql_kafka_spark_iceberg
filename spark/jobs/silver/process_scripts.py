from pyspark.sql import functions as f

"""
    INSERT: 0
    UPDATE: 1
    DELETE: 2
"""

def convert_change_type(df):
    df = df.withColumn("_change_type", f.when(f.col("_change_type") == "INSERT", 0) \
        .when(f.col("_change_type") == "UPDATE_AFTER", 1) \
        .otherwise(2))
    return df

def process_slv_dim_films(spark, df):
    df = convert_change_type(df)

    columns_to_keep = [
        df["number"].alias("id"),
        "film",
        "release_date", 
        "run_time", 
        "film_rating", 
        "plot",
        "_change_type"
    ]

    df = df.select(*columns_to_keep)
    return df

def process_slv_dim_genres(spark, df):
    df = convert_change_type(df)

    df.createOrReplaceTempView("tmp_slv_dim_genres")

    df = spark.sql(f"""
        WITH CTE AS (
            SELECT DISTINCT value AS genre
            FROM tmp_slv_dim_genres
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY NULL DESC) + COALESCE(
                (SELECT MAX(id) FROM spark_catalog.default.slv_dim_genres), 0
            ) AS id,
            genre,
            0 AS _change_type
        FROM CTE 
        WHERE genre NOT IN (SELECT DISTINCT genre FROM spark_catalog.default.slv_dim_genres)
    """)

    return df

def process_slv_fact_film_genres(spark, df):
    df = convert_change_type(df)

    df.createOrReplaceTempView("tmp_slv_fact_film_genres")
    df = spark.sql(f"""
        SELECT 
            sf.id AS film_id,
            sg.id AS genre_id,
            CASE WHEN bg.category = 'Genre' THEN 1 ELSE 0 END AS is_primary_genre,
            bg._change_type
        FROM tmp_slv_fact_film_genres bg
        LEFT JOIN spark_catalog.default.slv_dim_films sf
        ON bg.film = sf.film
        LEFT JOIN spark_catalog.default.slv_dim_genres sg
        ON bg.value = sg.genre
    """)
    return df

def process_slv_fact_film_ratings(spark, df):
    df = convert_change_type(df)

    df.createOrReplaceTempView("tmp_slv_fact_film_ratings")

    df = spark.sql(f"""
        SELECT 
            sf.id,
            bf.rotten_tomatoes_score,
            bf.rotten_tomatoes_counts,
            bf.metacritic_score,
            bf.metacritic_counts,
            bf.cinema_score,
            bf.imdb_score,
            bf.imdb_counts,
            bf._change_type
        FROM tmp_slv_fact_film_ratings bf
        LEFT JOIN spark_catalog.default.slv_dim_films sf
        ON bf.film = sf.film
    """)
    return df

def process_slv_fact_box_office(spark, df):
    df = convert_change_type(df)

    df.createOrReplaceTempView("tmp_slv_fact_box_office")

    df = spark.sql(f"""
        SELECT 
            sf.id,
            bbo.budget,
            bbo.box_office_us_canada,
            bbo.box_office_other,
            bbo.box_office_worldwide,
            bbo._change_type
        FROM tmp_slv_fact_box_office bbo
        LEFT JOIN spark_catalog.default.slv_dim_films sf
        ON bbo.film = sf.film
    """)
    return df