from datasets import load_dataset
from pyspark.sql import functions as F
import time


start_time = time.time()
dataset = load_dataset(
    "cfilt/iitb-english-hindi",
    split="train",
    streaming=True
)
# Collect Data
data = []

for i, row in enumerate(dataset):

    english = row["translation"]["en"]
    hindi = row["translation"]["hi"]

    data.append((english, hindi))

    if i % 10000 == 0:
        print(f"Collected {i} rows")

    # Remove later for full-scale processing
    if i == 1000000:
        break

# Create Spark DataFrame
df = spark.createDataFrame(
    data,
    ["english", "hindi"]
)

print(f"Initial Rows: {df.count()}")

# CLEANING PIPELINE

df = df.withColumn(
    "clean_english",
    F.lower(F.col("english"))
)

df = df.withColumn(
    "clean_english",
    F.regexp_replace(
        "clean_english",
        r"http\S+",
        ""
    )
)

df = df.withColumn(
    "clean_english",
    F.regexp_replace(
        "clean_english",
        r"[^\w\s]",
        ""
    )
)

df = df.withColumn(
    "clean_english",
    F.regexp_replace(
        "clean_english",
        r"\s+",
        " "
    )
)

# QUALITY SCORING

df = df.withColumn(
    "english_score",
    F.when(
        F.length("clean_english") < 5,
        70
    ).otherwise(100)
)
# DEDUPLICATION

before_dedup = df.count()

df = df.dropDuplicates(
    ["clean_english"]
)

after_dedup = df.count()

duplicates_removed = (
    before_dedup - after_dedup
)

# GOOD / BAD DATA

good_df = df.filter(
    F.col("english_score") > 70
)

bad_df = df.filter(
    F.col("english_score") <= 70
)

# SAVE DELTA TABLES

good_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(
        "workspace.ai_pipeline.curated_dataset"
    )

bad_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(
        "workspace.ai_pipeline.rejected_dataset"
    )

# -----------------------------------
# FINAL METRICS
# -----------------------------------

good_count = good_df.count()
bad_count = bad_df.count()

total_count = (
    good_count + bad_count
)

end_time = time.time()

print("\nPROCESSING COMPLETE")
print("-" * 50)

print(f"Total Rows       : {total_count}")
print(f"Curated Rows     : {good_count}")
print(f"Rejected Rows    : {bad_count}")
print(f"Duplicates Removed: {duplicates_removed}")

print(
    f"Execution Time: "
    f"{end_time - start_time:.2f} seconds"
)

# SAMPLE OUTPUT

display(
    good_df.select(
        "english",
        "clean_english",
        "english_score"
    )
)
