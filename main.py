from pyspark.sql import SparkSession
from Code.utils import load_yaml_config, read_csv_as_df, save_df_as_csv
from Code.AccidentAnalysis import AccidentAnalysis  # Updated import name

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("VehicleCrashAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load configuration from YAML file
    config_file_name = "config.yaml"
    config = load_yaml_config(config_file_name)

    # Extract output file path and file format from config
    output_file_paths = config.get("OUTPUT_DIRECTORIES")
    file_format = config.get("FILE_FORMAT")

    # Initialize AccidentAnalysis with Spark session and configuration
    analysis = AccidentAnalysis(spark, config)

    # 1. Find the number of crashes where the number of males killed is greater than 2
    print(
        "Q 1:",
        analysis.male_driver_crashes(
            output_file_paths.get("analysis_1"), file_format.get("Output")
        )
    )

    # 2. Count the number of distinct crashes involving motorcycles
    print(
        "Q 2:",
        analysis.motorcycle_crashes(
            output_file_paths.get("analysis_2"), file_format.get("Output")
        ),
    )

    # 3. Determine the top 5 vehicle brands involved in crashes where the driver died and airbags did not deploy
    print(
        "Q 3:",
        analysis.top_brands_in_fatal_crashes(
            output_file_paths.get("analysis_3"), file_format.get("Output")
        ),
    )

    # 4. Count the number of hit-and-run crashes where the driver has a valid license
    print(
        "Q 4:",
        analysis.count_hit_and_run_crashes(
            output_file_paths.get("analysis_4"), file_format.get("Output")
        ),
    )

    # 5. Determine the state with the highest number of accidents where females are not involved
    print(
        "Q 5:",
        analysis.states_with_no_female_involvement(
            output_file_paths.get("analysis_5"), file_format.get("Output")
        ),
    )

    # 6. Find the top 3rd to 5th vehicle makes based on the total number of injuries including deaths
    print(
        "Q 6:",
        analysis.top_vehicle_ids_by_injuries(
            output_file_paths.get("analysis_6"), file_format.get("Output")
        ),
    )

    # 7. For each vehicle body style, determine the top ethnic group involved in crashes
    print("Q 7:")
    analysis.top_ethnicities_by_vehicle_body_style(
        output_file_paths.get("analysis_7"), file_format.get("Output")
    ).show(truncate=False)

    # 8. Find the top 5 zip codes with the highest number of crashes where alcohol was a contributing factor
    print(
        "Q 8:",
        analysis.zip_codes_with_alcohol_involvement(
            output_file_paths.get("analysis_8"), file_format.get("Output")
        ),
    )

    # 9. Count distinct crashes where no damaged property was observed, damage level is above 4, and the car has insurance
    print(
        "Q 9:",
        analysis.count_crashes_with_no_damage(
            output_file_paths.get("analysis_9"), file_format.get("Output")
        ),
    )

    # 10. Determine the top 5 vehicle makes involved in speeding offenses, with licensed drivers, using top 10 colors, and registered in the top 25 states with the highest number of offenses
    print(
        "Q 10:",
        analysis.top_5_vehicle_speeding_offenses(
            output_file_paths.get("analysis_10"), file_format.get("Output")
        ),
    )

    # Stop the Spark session
    spark.stop()