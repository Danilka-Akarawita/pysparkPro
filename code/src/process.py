from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import explode, split, col, count, trim, lower, regexp_replace, collect_list
import logging
from typing import List
from datetime import datetime
import pyspark
print(pyspark.__version__)
from huggingface_hub import login
from datasets import load_dataset

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
        logger.debug(f"Dataset Type: {type(dataset)}")
        logger.debug(f"Number of Rows: {len(dataset)}")
        logger.debug(f"Sample Row: {dataset[0]}")

        return self.spark.createDataFrame(dataset)
    
    def _process_wordCounts(self,df:DataFrame,target_words:List[str]=None)->DataFrame:
    
        """
        Process the word counts for the target words and return a dataframe
        
        Args:
            df (DataFrame): _description_
            target_words (List[str], optional): _description_. Defaults to None.
        Returns:
            _type_: _description_
        """
        
        df = df.withColumn("clean_description",trim(lower(regexp_replace(col("description"), "[^a-zA-Z\\s]", ""))))
        df = df.withColumn("word", explode(split(col("description"), " ")))
        
        if target_words:
            df=df.filter(col("word").isin(target_words))
            
        return df.groupBy("word").agg(count("word").alias("count"))
    
    
    def generate_wordCounts(self, target_words:List[str]=None,all_words:bool=False)->None:
        
        """Generate word counts for the target words and save the result to the output path as a parquet file

        Args:
            target_words (List[str], optional): _description_. Defaults to None.
            all_words (bool, optional): _description_. Defaults to False.
            
        """
        logger.debug("Generating word counts")
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
        
        
    @staticmethod
    def create_spark_session(config: dict) -> SparkSession:
        """
        Create a spark session  configuration     
        Args:
            config (dict): _description_

        Returns:
            _type_: _description_
        """
        spark = SparkSession.builder \
            .appName(config['spark']['app_name']) \
            .master(config['spark']['master']) \
            .config("spark.driver.memory", config['spark']['memory']) \
            .config("spark.executor.memory", "4g") \
            .config("spark.network.timeout", "10000000") \
            .getOrCreate()

        return spark

