import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from processAgnews import AGNewsData
from typing import Dict, Any

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("AGNewsTest").getOrCreate()  

@pytest.fixture()
def sample_config():
    
    return {
        "spark": {
            "app_name": "AGNewsTest",
            "master": "local[2]",
            "memory": "4g"
        },
        "data": {
            "huggingface_token": "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" , 
            "dataset_name": "ag_news",
            "output_path": "output"
        }
    }

def test_wordCounts(spark, sample_config):
    processor = AGNewsData(spark, sample_config)
    test_data = [Row(description="A test case for get the test values ")]
    df = spark.createDataFrame(test_data)

    target_words = ["test", "the", "values"]
    result = processor.process_wordCounts(df, target_words)
    counts = {row["word"]: row["count"] for row in result.collect()}

    assert counts.get("values") == 1
    assert counts.get("the") == 1
    assert counts.get("test") == 2
    
    print("Test passed with counts:", counts)
