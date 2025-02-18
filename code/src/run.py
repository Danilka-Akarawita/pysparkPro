import argparse
import yaml
from typing import Dict, Any
from pathlib import Path
import logging
from process import NewsDataProcessor 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="AG News Data Processor")
    subparsers = parser.add_subparsers(dest='command')

    """ Process data command  for target words """
    process_parser = subparsers.add_parser('process_data')
    process_parser.add_argument('--cfg', required=True)
    process_parser.add_argument('--dataset', default='news')
    process_parser.add_argument('--dirout', required=True)

    """ Process data command for all words """
    process_all_parser = subparsers.add_parser('process_data_all')
    process_all_parser.add_argument('--cfg', required=True)
    process_all_parser.add_argument('--dataset', default='news')
    process_all_parser.add_argument('--dirout', required=True)

    args = parser.parse_args()
    
    """ Load configuration file """
    config = load_config(args.cfg)
    spark = NewsDataProcessor.create_spark_session(config)
    processor = NewsDataProcessor(spark, config)
    
    Path(args.dirout).mkdir(parents=True, exist_ok=True)
    
    if args.command == 'process_data':
        processor.generate_wordCounts(
        target_words=["president", "the", "Asia","make"],  
        all_words=False
        )
    elif args.command == 'process_data_all':
        processor.generate_wordCounts(
            all_words=True
        )


if __name__ == "__main__":
    main()