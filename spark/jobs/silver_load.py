from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()
        
BUCKET_NAME = "pixarfilms"
WAREHOUSE_PATH = f"s3a://{BUCKET_NAME}"
LAYER_NAME = "silver"

DATABASE_NAME = "spark_catalog.default"

SCHEMAS = {
    "slv_dim_films" : """
        id INT NOT NULL,
        film STRING,
        release_date DATE,
        run_time INT,
        film_rating STRING,
        plot STRING,
        start_date DATE,
        end_date DATE,
        is_current INT
    """,
    "slv_dim_genres": """
        id INT NOT NULL,
        genre STRING,
        start_date DATE,
        end_date DATE,
        is_current INT
    """,
    "slv_fact_film_ratings": """
        id INT NOT NULL,
        rotten_tomatoes_score INT,
        rotten_tomatoes_counts INT,
        metacritic_score INT,
        metacritic_counts INT,
        cinema_score STRING,
        imdb_score DECIMAL(3,1),
        imdb_counts INT
    """,
    "slv_fact_film_genres": """
        film_id INT NOT NULL,
        genre_id INT NOT NULL,
        is_primary_genre INT
    """,
    "slv_fact_box_office": """
        id INT NOT NULL,
        budget INT,
        box_office_us_canada INT,
        box_office_other INT,
        box_office_worldwide INT
    """
}

def create_silver_tables():
    for table_name in SCHEMAS.keys():
        SPARK.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name} (
                {SCHEMAS[table_name]}
            ) USING iceberg
            LOCATION '{WAREHOUSE_PATH}/{LAYER_NAME}/{table_name[4:]}'
            """)

# this is for dim tables from dim source (films)
def load_silver_dim(brz_df, slv_table, key_column="id"):
    tmp_view = f"tmp_{slv_table}"
    brz_df.createOrReplaceTempView(tmp_view)

    # record is deleted in source
    SPARK.sql(f"""
        UPDATE {DATABASE_NAME}.{slv_table}
        SET 
            is_current = 0,
            end_date = (SELECT MIN(start_date) FROM {tmp_view})
        WHERE {key_column} NOT IN (SELECT DISTINCT {key_column} FROM {tmp_view})
    """)

    # update old records
    SPARK.sql(f"""
        MERGE INTO {DATABASE_NAME}.{slv_table} target
        USING {tmp_view} source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED 
            AND source.start_date > target.start_date 
            AND target.is_current = 1 
            AND target.end_date IS NULL
        THEN UPDATE SET 
            target.is_current = 0,
            target.end_date = source.start_date
        WHEN NOT MATCHED THEN INSERT *
    """)

    # insert new records after update ol ones
    SPARK.sql(f"""
        WITH CTE AS (
            SELECT source.* 
            FROM {tmp_view} source
            INNER JOIN {DATABASE_NAME}.{slv_table} target
            ON target.{key_column} = source.{key_column} 
                AND source.start_date > target.start_date 
                AND target.is_current = 0 
                AND target.end_date IS NOT NULL
        )
        INSERT INTO {DATABASE_NAME}.{slv_table}
        SELECT * FROM CTE
    """)

# this is for dim tables from fact source (genres)
def load_silver_dim_from_fact(brz_df, slv_table, key_column="genre"):
    tmp_view = f"tmp_{slv_table}"
    brz_df.createOrReplaceTempView(tmp_view)

    # record is deleted in source
    SPARK.sql(f"""
        UPDATE {DATABASE_NAME}.{slv_table}
        SET 
            is_current = 0,
            end_date = (SELECT MIN(start_date) FROM {tmp_view})
        WHERE {key_column} NOT IN (SELECT DISTINCT {key_column} FROM {tmp_view})
    """)

    # update old records and insert new ones
    SPARK.sql(f"""
        MERGE INTO {DATABASE_NAME}.{slv_table} target
        USING {tmp_view} source
        ON target.{key_column} = source.{key_column}
        WHEN NOT MATCHED THEN INSERT *
    """)

def load_silver_fact(brz_df, slv_table, key_columns=["id"]):
    tmp_view = f"tmp_{slv_table}"
    brz_df.createOrReplaceTempView(tmp_view)

    join_str = " AND ".join([f"target.{key} = source.{key}" for key in key_columns])

    SPARK.sql(f"""
        MERGE INTO {DATABASE_NAME}.{slv_table} target
        USING {tmp_view} source
        ON {join_str}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def process_films():
    df = SPARK.sql(f"""
        SELECT 
            number AS id,
            film,
            release_date, 
            run_time, 
            film_rating, 
            plot, 
            CAST(FROM_UNIXTIME(sequence_number / 1000) AS DATE) AS start_date,
            NULL AS end_date,
            1 AS is_current
        FROM {DATABASE_NAME}.brz_films
    """)
    return df

def process_genres():
    df = SPARK.sql(f"""
        WITH CTE AS (
            SELECT 
                value AS genre,
                CAST(FROM_UNIXTIME(MIN(sequence_number) / 1000) AS DATE) AS start_date
            FROM {DATABASE_NAME}.brz_genres
            GROUP BY value
        )
        SELECT
            (SELECT COALESCE(MAX(id), 0) FROM {DATABASE_NAME}.slv_dim_genres) + ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS id,
            genre,
            start_date,
            NULL AS end_date,
            1 AS is_current
        FROM CTE
    """)
    return df

def process_film_genres():
    df = SPARK.sql(f"""
        SELECT 
            sf.id AS film_id,
            sg.id AS genre_id,
            CASE WHEN bg.category = 'Genre' THEN 1 ELSE 0 END AS is_primary_genre
        FROM {DATABASE_NAME}.brz_genres bg
        LEFT JOIN {DATABASE_NAME}.slv_dim_films sf
        ON bg.film = sf.film AND sf.is_current = 1
        LEFT JOIN {DATABASE_NAME}.slv_dim_genres sg
        ON bg.value = sg.genre AND sg.is_current = 1
    """)
    return df

def process_film_ratings():
    df = SPARK.sql(f"""
        SELECT 
            sf.id,
            bf.rotten_tomatoes_score,
            bf.rotten_tomatoes_counts,
            bf.metacritic_score,
            bf.metacritic_counts,
            bf.cinema_score,
            bf.imdb_score,
            bf.imdb_counts
        FROM {DATABASE_NAME}.brz_film_ratings bf
        LEFT JOIN {DATABASE_NAME}.slv_dim_films sf
        ON bf.film = sf.film AND sf.is_current = 1
    """)
    return df

def process_box_office():
    df = SPARK.sql(f"""
        SELECT 
            sf.id,
            bbo.budget,
            bbo.box_office_us_canada,
            bbo.box_office_other,
            bbo.box_office_worldwide
        FROM {DATABASE_NAME}.brz_box_office bbo
        LEFT JOIN {DATABASE_NAME}.slv_dim_films sf
        ON bbo.film = sf.film AND sf.is_current = 1
    """)
    return df

def main():
    create_silver_tables()

    print(f"===== LOADING TABLE slv_dim_films ======")
    df_films = process_films()
    # df_films.show(40)
    load_silver_dim(df_films, "slv_dim_films", key_column="id")
    # SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.slv_dim_films").show(40)
    print(f"===== FINISHED LOADING TABLE slv_dim_films ======")
    
    print(f"===== LOADING TABLE slv_dim_genres ======")
    df_genres = process_genres()
    # df_genres.show(40)
    load_silver_dim_from_fact(df_genres, "slv_dim_genres", key_column="genre")
    # SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.slv_dim_genres").show(40)
    print(f"===== FINISHED LOADING TABLE slv_dim_genres ======")

    print(f"===== LOADING TABLE slv_fact_film_genres ======")
    df_film_genres = process_film_genres()
    # df_film_genres.show(40)
    load_silver_fact(df_film_genres, "slv_fact_film_genres", key_columns=["film_id", "genre_id"])
    # SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.slv_fact_film_genres").show(40)
    print(f"===== FINISHED LOADING TABLE slv_fact_film_genres ======")

    print(f"===== LOADING TABLE slv_fact_film_ratings ======")
    df_film_ratings = process_film_ratings()
    # df_film_ratings.show(40)
    load_silver_fact(df_film_ratings, "slv_fact_film_ratings", key_columns=["id"])
    # SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.slv_fact_film_ratings").show(40)
    print(f"===== FINISHED LOADING TABLE slv_fact_film_ratings ======")
    
    print(f"===== LOADING TABLE slv_fact_box_office ======")
    df_box_office = process_box_office()
    # df_box_office.show(40)
    load_silver_fact(df_box_office, "slv_fact_box_office", key_columns=["id"])
    # SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.slv_fact_box_office").show(40)
    print(f"===== FINISHED LOADING TABLE slv_fact_box_office ======")

    SPARK.stop()
    
if __name__ == "__main__":
    main()