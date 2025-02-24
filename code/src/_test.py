import pytest
from pyspark.sql import SparkSession
from process import NewsDataProcessor
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
            "input_path": "sh0416/ag_news",
            "output_path": "output"
        }
    }
    
def test_wordCounts(spark, sample_config):
    processor = NewsDataProcessor(spark, sample_config)
    test_data=[("president the Asia make","Asia make")]
    df = processor._load_data()
    result = processor._process_wordCounts(df, ["president", "the", "Asia"])
    counts={row["word"]:row["count"] for row in result.collect()}
    assert counts.get("president") == 1
    assert counts.get("the") == 1
    assert counts.get("Asia") == 2
    print(counts)
    print("Test passed")