# big_data_wordcount.py
# ---------------------
# A simple PySpark program to perform word count on a large dataset.

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("BigDataWordCount") \
            .getOrCreate()

        # Path to your large dataset (can be local or HDFS/S3)
        input_path = "data/large_text_file.txt"  # Change to your file path

        # Read the text file into a DataFrame
        df = spark.read.text(input_path)

        # Split lines into words, explode into rows, and clean up
        words_df = df.select(
            explode(
                split(col("value"), r"\s+")
            ).alias("word")
        ).filter(col("word") != "")

        # Count occurrences of each word
        word_counts = words_df.groupBy("word").count().orderBy(col("count").desc())

        # Show top 20 words
        word_counts.show(20, truncate=False)

        # Save results to output folder
        output_path = "output/wordcount_results"
        word_counts.write.mode("overwrite").csv(output_path)

        print(f"Word count results saved to: {output_path}")

        # Stop Spark session
        spark.stop()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
