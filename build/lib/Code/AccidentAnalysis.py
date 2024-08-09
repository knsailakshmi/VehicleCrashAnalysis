from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from Code.utils import read_csv_as_df, save_df_as_csv

class AccidentAnalysis:
    def __init__(self, spark, config):
        """
        Initialize the AccidentAnalysis class with Spark session and configuration.
        :param spark: Spark session object
        :param config: Configuration dictionary with file paths
        """
        # Load raw data from source files specified in the configuration
        input_file_paths = config.get("INPUT_FILES")
        self.df_charges = read_csv_as_df(spark, input_file_paths.get("Charges"))
        self.df_damages = read_csv_as_df(spark, input_file_paths.get("Damages"))
        self.df_endorsements = read_csv_as_df(spark, input_file_paths.get("Endorsements"))
        self.df_restrictions = read_csv_as_df(spark, input_file_paths.get("Restrictions"))
        self.df_primary_persons = read_csv_as_df(spark, input_file_paths.get("Primary_Persons"))
        self.df_units = read_csv_as_df(spark, input_file_paths.get("Units"))

    def male_driver_crashes(self, output_path, output_format):
        """
        Find the number of crashes where the number of males killed is greater than 2.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: Count of such crashes
        """
        df = self.df_primary_persons.filter(
            self.df_primary_persons.PRSN_GNDR_ID == "MALE"
        ).filter(self.df_primary_persons.DEATH_CNT > 2)
        save_df_as_csv(df, output_path, output_format)
        return df.count()

    def motorcycle_crashes(self, output_path, output_format):
        """
        Count the number of distinct crashes involving motorcycles.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: Count of distinct motorcycle crashes
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")).select('CRASH_ID').distinct()
        save_df_as_csv(df, output_path, output_format)
        return df.count()

    def top_brands_in_fatal_crashes(self, output_path, output_format):
        """
        Determine the top 5 vehicle brands involved in crashes where the driver died and airbags did not deploy.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: List of top 5 vehicle brands
        """
        df = (
            self.df_units.join(self.df_primary_persons, on=["CRASH_ID"], how="inner")
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED")
                & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (col("VEH_MAKE_ID") != "NA")
            )
            .groupBy("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_df_as_csv(df, output_path, output_format)
        return [row[0] for row in df.collect()]

    def count_hit_and_run_crashes(self, output_path, output_format):
        """
        Count the number of crashes involving hit and run where the driver has a valid license.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: Count of such crashes
        """
        df = (
            self.df_units.select("CRASH_ID", "VEH_HNR_FL")
            .join(
                self.df_primary_persons.select("CRASH_ID", "DRVR_LIC_TYPE_ID"),
                on=["CRASH_ID"],
                how="inner")
            .filter(col("VEH_HNR_FL") == "Y")
            .filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .select('CRASH_ID').distinct()
        )
        save_df_as_csv(df, output_path, output_format)
        return df.count()

    def states_with_no_female_involvement(self, output_path, output_format):
        """
        Determine the state with the highest number of accidents where females are not involved.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: State ID with the highest number of such accidents
        """
        df = (
            self.df_primary_persons.filter(
                self.df_primary_persons.PRSN_GNDR_ID != "FEMALE"
            )
            .groupBy("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        save_df_as_csv(df, output_path, output_format)
        return df.first().DRVR_LIC_STATE_ID

    def top_vehicle_ids_by_injuries(self, output_path, output_format):
        """
        Find the top 3rd to 5th vehicle makes based on the total number of injuries including deaths.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: List of top 3rd to 5th vehicle makes
        """
        df = (
            self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", self.df_units['TOT_INJRY_CNT'] + self.df_units['DEATH_CNT'])
            .groupBy("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )
        df_top_3_to_5 = df.limit(5).subtract(df.limit(2)).select("VEH_MAKE_ID")
        save_df_as_csv(df_top_3_to_5, output_path, output_format)
        return [veh[0] for veh in df_top_3_to_5.collect()]

    def top_ethnicities_by_vehicle_body_style(self, output_path, output_format):
        """
        For each vehicle body style, determine the top ethnic group involved in crashes.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: DataFrame of top ethnic groups by body style
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = (
            self.df_units.join(self.df_primary_persons, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.df_primary_persons.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
            .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )
        save_df_as_csv(df, output_path, output_format)
        return df

    def zip_codes_with_alcohol_involvement(self, output_path, output_format):
        """
        Find the top 5 zip codes with the highest number of crashes where alcohol was a contributing factor.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: List of top 5 zip codes
        """
        df = (
            self.df_units.join(self.df_primary_persons, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupBy("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .select('DRVR_ZIP')
            .limit(5)
        )
        save_df_as_csv(df, output_path, output_format)
        return [row[0] for row in df.collect()]

    def count_crashes_with_no_damage(self, output_path, output_format):
        """
        Count distinct crashes where no damaged property was observed, damage level is above 4, and the car has insurance.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: Count of such crashes
        """
        df = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(((self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                     & (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
                    | ((self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                       & (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
                    )
            .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
            .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
            .join(self.df_charges, on=["CRASH_ID"], how="left")
            .filter(~col("CHARGE").contains("INSURANCE"))
            .select('CRASH_ID').distinct()
        )
        save_df_as_csv(df, output_path, output_format)
        return df.count()

    def top_5_vehicle_speeding_offenses(self, output_path, output_format):
        """
        Determine the top 5 vehicle makes involved in speeding offenses, with licensed drivers, using top 10 vehicle colors, and registered in the top 25 states with the highest number of offenses.
        :param output_path: Path to save the output CSV
        :param output_format: Format to save the output CSV
        :return: List of top 5 vehicle makes
        """
        top_25_states = [
            row[0]
            for row in self.df_units.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupBy("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary_persons, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary_persons.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_states))
            .groupBy("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_df_as_csv(df, output_path, output_format)
        return [row[0] for row in df.collect()]