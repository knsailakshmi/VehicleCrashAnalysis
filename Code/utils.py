import yaml

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


def save_df_as_csv(df, file_path, format_type):
    """
    Save a Spark DataFrame to a CSV file.
    :param df: Spark DataFrame to save
    :param file_path: Path to the output CSV file
    :param format_type: File format (e.g., csv)
    :return: None
    """
    # Uncomment the line below if you want to consolidate the output into a single file
    # df = df.coalesce(1)
    df.repartition(1).write.format(format_type).mode("overwrite").option(
        "header", "true"
    ).save(file_path)