#!/usr/bin/env python3
"""
JSONL Splitter and Compressor CLI Tool

Splits large JSON Lines files into multiple smaller compressed files.
Each output file contains minified JSON entries and is compressed with gzip.
"""

import json
import os
import gzip
import argparse
import sys


def split_jsonl_and_minify(input_file_path, output_directory, max_records_per_file):
    """
    Splits a large JSON Lines file into multiple smaller JSONL files,
    with each output file containing minified JSON entries and compressed with gzip.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Use a file counter for output filenames
    file_count = 1
    # Use a records counter for the current chunk
    records_in_chunk = 0
    # Use a list to hold records for the current chunk
    chunk_data = []

    print(f"Starting to process {input_file_path} and splitting into minified gzipped JSONL files...")

    # Read the file line by line
    with open(input_file_path, 'r', encoding='utf-8') as infile:
        for line in infile:
            try:
                # Parse the JSON object from the current line
                record = json.loads(line)
                chunk_data.append(record)
                records_in_chunk += 1

                # If the chunk is full, write it to a new gzipped file
                if records_in_chunk >= max_records_per_file:
                    _write_gzipped_chunk(chunk_data, file_count, output_directory)
                    
                    # Reset the chunk and counters
                    chunk_data = []
                    records_in_chunk = 0
                    file_count += 1
                    
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line.strip()}. Skipping this line. Error: {e}")
                
    # Write any remaining records in the last chunk
    if chunk_data:
        _write_gzipped_chunk(chunk_data, file_count, output_directory)
        
    print("Processing complete.")


def _write_gzipped_chunk(chunk_data, file_count, output_directory, prefix):
    """
    Writes chunk data directly to a gzipped JSONL file.
    """
    output_file_name = f"{prefix}_part_{file_count:04d}.json.gz"
    output_file_path = os.path.join(output_directory, output_file_name)
    
    # Write directly to gzipped file (minified JSONL format)
    with gzip.open(output_file_path, 'wt', encoding='utf-8', compresslevel=9) as outfile:
        for item in chunk_data:
            # json.dumps without 'indent' and using separators for compactness
            minified_line = json.dumps(item, separators=(',', ':'))
            outfile.write(minified_line + '\n')
    
    # Get compressed file size
    compressed_size = os.path.getsize(output_file_path)
    
    print(f"Created {output_file_name} with {len(chunk_data):,} records. "
          f"Size: {compressed_size / (1024*1024):.2f}MB")


def main():
    parser = argparse.ArgumentParser(
        description='Split large JSONL files into smaller compressed chunks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.json -o output_dir -n 10000
  %(prog)s reviews.json --output compressed_reviews --max-records 500000
  %(prog)s data.json  # Uses default values
        """
    )
    
    parser.add_argument(
        'input_file',
        help='Path to the input JSONL file'
    )
    
    parser.add_argument(
        '-o', '--output',
        dest='output_dir',
        default='output_minified_gzip',
        help='Output directory for compressed files (default: output_minified_gzip)'
    )
    
    parser.add_argument(
        '-n', '--max-records',
        dest='max_records',
        type=int,
        default=100000,
        help='Maximum records per output file (default: 100000)'
    )
    
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # Validate max_records is positive
    if args.max_records <= 0:
        print(f"Error: max-records must be a positive integer.", file=sys.stderr)
        sys.exit(1)
    
    try:
        split_jsonl_and_minify(args.input_file, args.output_dir, args.max_records)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
