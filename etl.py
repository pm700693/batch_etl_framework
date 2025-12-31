import yaml
from pyspark.sql import SparkSession

def run_job(config_path):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    spark = SparkSession.builder.appName('etl-framework').getOrCreate()
    for job in cfg['jobs']:
        df = spark.read.json(job['source'])
        # transformations...
        df.write.parquet(job['target'])

if __name__ == '__main__':
    run_job('jobs.yaml')
