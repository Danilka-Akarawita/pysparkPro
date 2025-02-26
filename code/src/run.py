import argparse
import yaml
from typing import Dict, Any
from pathlib import Path
import logging
from processAgnews import AGNewsData 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
    
def add_command_line_arguments(subparser: argparse.ArgumentParser):
    """
    Add command line arguments to the parser
    
    Args:
        parser (argparse.ArgumentParser): Argument parser
    """
    subparser.add_argument('--cfg', required=True, help='Path to the configuration file')
    subparser.add_argument('--dataset', default='news', help='Dataset name')
    subparser.add_argument('--dirout', required=True, help='Output directory')

def main():
    """
    parse the command line arguments and load the configuration file and run the appropriate command
    """
    parser = argparse.ArgumentParser(description="AG News Data Processor")
    subparsers = parser.add_subparsers(dest='command')
    
    # Process data command for target words
    process_parser = subparsers.add_parser('process_data')
    add_command_line_arguments(process_parser)

    # Process data command for all words
    process_all_parser = subparsers.add_parser('process_data_all')
    add_command_line_arguments(process_all_parser)

    args = parser.parse_args()
    
    
    #if no command is provided display the help message and exit
    if not args.command:
        parser.print_help()
        exit(1)
    
    
    # Load configuration
    config = load_config(args.cfg)
    spark = AGNewsData.create_spark_session(config)
    AGNews= AGNewsData(spark, config)
    
    Path(args.dirout).mkdir(parents=True, exist_ok=True)
    
    if args.command == 'process_data':
        AGNews.generate_wordCounts(
        target_words=["president", "the", "Asia"],  
        all_words=False
        )
    elif args.command == 'process_data_all':
        AGNews.generate_wordCounts(
            all_words=True
        )


if __name__ == "__main__":
    main()