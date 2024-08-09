import yaml
from pyspark.sql import DataFrame
import os

def load_yaml_config(file_path):
    """
    Load configuration from a YAML file.
    :param file_path: Path to the YAML configuration file
    :return: Dictionary containing configuration details
    """
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def read_csv_as_df(spark, file_path):
    """
    Load CSV data into a Spark DataFrame.
    :param spark: Spark session instance
    :param file_path: Path to the CSV file
    :return: Spark DataFrame
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def save_df_as_csv(df: DataFrame, path: str, format_type: str = "csv"):
    """
    Save DataFrame to a CSV file.
    :param df: DataFrame to be saved
    :param path: Output path where the CSV will be saved
    :param format_type: File format, e.g., "csv"
    """
    # Ensure the directory exists
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    print(f"Saving DataFrame to path: {path} with format: {format_type}")
    try:
        df.repartition(1).write.format(format_type).mode("overwrite").option("header", "true").save(path)
        print(f"DataFrame successfully saved to {path}")
    except FileNotFoundError as e:
        print(f"FileNotFoundError: {e}")
    except PermissionError as e:
        print(f"PermissionError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while saving the DataFrame: {e}")
