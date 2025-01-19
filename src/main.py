from pyspark.sql import SparkSession
import yaml
from analysis import perform_analyses

def load_config():
    with open("config/config.yaml", 'r') as file:
        return yaml.safe_load(file)

def main():
    config = load_config()
    spark = SparkSession.builder.appName("BCG Assignment").getOrCreate()

    # Load datasets
    restrict_df = spark.read.csv(config['input_data']['restrict_use'], header=True, inferSchema=True)
    charges_df = spark.read.csv(config['input_data']['charges_use'], header=True, inferSchema=True)
    units_df = spark.read.csv(config['input_data']['units_use'], header=True, inferSchema=True)
    primary_person_df = spark.read.csv(config['input_data']['primary_person_use'], header=True, inferSchema=True)
    damages_df = spark.read.csv(config['input_data']['damages_use'], header=True, inferSchema=True)
    endorse_df = spark.read.csv(config['input_data']['endorse_use'], header=True, inferSchema=True)

    # Perform analyses
    results = perform_analyses(restrict_df, charges_df, units_df, primary_person_df, damages_df, endorse_df)

    # Output results (you can format this as needed)
    with open(config['output_data']['analysis_results'], 'w') as outfile:
        outfile.write(str(results))

if __name__ == "__main__":
    main()
