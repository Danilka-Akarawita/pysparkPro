from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import explode, split, col, count, trim, lower, regexp_replace, collect_list
import logging
import argparse
from typing import List, Tuple
from datetime import datetime
import yaml
import pyspark
print(pyspark.__version__)
from huggingface_hub import login
import os
from datasets import load_dataset, load_from_disk



logger = logging.getLogger(__name__)

class NewsDataProcessor:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self._authenticate_huggingface()
    
    def _authenticate_huggingface(self):
        """Logs in to Hugging Face Hub using an API token"""
        
        login(token=self.config['data']['huggingface_token'])
        logger.info("Authenticated to Hugging Face Hub")

    def _load_data(self) -> DataFrame:
        """Load the data from the dataset and return a Spark DataFrame."""
        logger.info("Downloading dataset from Hugging Face Hub")
        dataset = load_dataset(self.config['data']['dataset_name'], split='test')

        # Debugging logs
        print(f"Dataset Type: {type(dataset)}")
        print(f"Number of Rows: {len(dataset)}")
        print(f"Sample Row: {dataset[0]}")

        return self.spark.createDataFrame(dataset)
    
    def _process_wordCounts(self,df:DataFrame,target_words:List[str]=None)->DataFrame:
    
        """
        Process the word counts for the target words and return a dataframe
        Returns:
            _type_: _description_
        """
        df = df.withColumn("clean_description",trim(lower(regexp_replace(col("description"), "[^a-zA-Z\\s]", ""))))
        df = df.withColumn("word", explode(split(col("description"), " ")))
        print("Dataframe:----------------")
        df.show()
        
        if target_words:
            df=df.filter(col("word").isin(target_words))
            
        return df.groupBy("word").agg(count("word").alias("count"))
    
    
    def generate_wordCounts(self, target_words:List[str]=None,all_words:bool=False)->None:
        
        """Generate word counts for the target words and save the result to the output path as a parquet file

        Args:
            target_words (List[str], optional): _description_. Defaults to None.
            all_words (bool, optional): _description_. Defaults to False.
        """
        print("Generating word counts")
        df=self._load_data()
        result=self._process_wordCounts(df,target_words)
        if(all_words):
            result=result.agg(collect_list("word").alias("words"),collect_list("count").alias("counts"))
            
        date_str=datetime.now().strftime("%Y-%m-%d")
        filename = f"word_count_all_{date_str}.parquet" if all_words else f"word_count_{date_str}.parquet"
        result.write.parquet(f"{self.config['data']['output_path']}/{filename}",mode="overwrite")
        word_counts_df = self.spark.read.parquet(f"{self.config['data']['output_path']}/{filename}")
        word_counts_df.show()
        logger.info(f"Word count data saved to {filename}")
        
    # def generate_wordCountsAll(self, target_words:List[str]=None)->None:
    #     logger.info("Generating word counts for all words")
    #     df=self._load_data()
    #     result=self._process_wordCounts(df,target_words)
    #     aggredated_result=result.agg(collect_list("word").alias("words"),collect_list("count").alias("counts"))
        
    #     date_str=datetime.now().strftime("%Y-%m-%d")
    #     filename = f"word_count_all_{date_str}.parquet"
    #     aggredated_result.write.parquet(f"{self.config['data']['output_path']}/{filename}",mode="overwrite")
    #     word_counts_df = self.spark.read.parquet(f"{self.config['data']['output_path']}/{filename}")
    #     word_counts_df.show()
    #     logger.info(f"Word count data saved to {filename}")
        
    
        """
        Create a spark session  configuration

        Returns:
            _type_: _description_
        """
    @staticmethod
    def create_spark_session(config: dict) -> SparkSession:
        spark = SparkSession.builder \
            .appName(config['spark']['app_name']) \
            .master(config['spark']['master']) \
            .config("spark.driver.memory", config['spark']['memory']) \
            .config("spark.executor.memory", "4g") \
            .config("spark.network.timeout", "10000000") \
            .getOrCreate()

        return spark

    
    
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="News Data Processor")
#     parser.add_argument("--config", required=True, help="Path to the configuration file")
#     args = parser.parse_args()
#     print(args.config)

#     with open(args.config, 'r') as file:
#         config = yaml.safe_load(file)

#     spark = NewsDataProcessor.create_spark_session(config)
#     processor = NewsDataProcessor(spark, config)
#     processor.generate_wordCounts()   
    
