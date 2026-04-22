#!/usr/bin/env python3
"""
CSV parsing and validation logic for UK House Prices data ingestion.

Handles parsing of HM Land Registry CSV format, transaction validation,
and geographic filtering for target counties.
"""

import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum

# Load target counties from environment
TARGET_COUNTIES = [
    county.strip().upper() 
    for county in os.getenv('TARGET_COUNTIES', 'ESSEX,HERTFORDSHIRE,KENT,SURREY,CAMBRIDGESHIRE').split(',')
]


class ParseResult(Enum):
    """Result types for parsing operations."""
    SUCCESS = "success"
    INVALID_FORMAT = "invalid_format"
    PARSE_ERROR = "parse_error"
    FILTERED_OUT = "filtered_out"


def parse_csv_line(line: str) -> List[str]:
    """Parse a single CSV line, handling quoted fields."""
    # Use Python's CSV parser for proper handling
    csv_reader = csv.reader([line])
    try:
        return next(csv_reader)
    except StopIteration:
        return []


def parse_transaction_row(row: List[str]) -> Tuple[ParseResult, Optional[Dict], Optional[str]]:
    """
    Parse a single transaction row from CSV.
    
    Returns:
        Tuple of (result_type, transaction_dict, error_message)
    """
    if len(row) != 16:
        return ParseResult.INVALID_FORMAT, None, f"Expected 16 fields, got {len(row)}"
        
    try:
        # Parse date
        date_str = row[2].strip()
        if date_str:
            transaction_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M').date()
        else:
            transaction_date = None
            
        # Parse price
        try:
            price = int(row[1]) if row[1].strip() else 0
        except ValueError:
            price = 0
        
        transaction = {
            'transaction_id': row[0].strip(),
            'price': price,
            'date': transaction_date,
            'postcode': row[3].strip().upper() if row[3] else None,
            'property_type': row[4].strip().upper() if row[4] else None,
            'new_build': row[5].strip().upper() if row[5] else None,
            'tenure': row[6].strip().upper() if row[6] else None,
            'paon': row[7].strip() if row[7] else None,
            'saon': row[8].strip() if row[8] else None,
            'street': row[9].strip() if row[9] else None,
            'locality': row[10].strip() if row[10] else None,
            'town': row[11].strip() if row[11] else None,
            'district': row[12].strip() if row[12] else None,
            'county': row[13].strip().upper() if row[13] else None,
            'ppd_type': row[14].strip().upper() if row[14] else None,
            'record_status': row[15].strip().upper() if row[15] else 'A'
        }
        
        return ParseResult.SUCCESS, transaction, None
        
    except Exception as e:
        return ParseResult.PARSE_ERROR, None, str(e)


def should_include_transaction(transaction: Dict, target_counties: Optional[List[str]] = None) -> bool:
    """Check if transaction should be included based on target counties."""
    if target_counties is None:
        target_counties = TARGET_COUNTIES
        
    county = transaction.get('county')
    if not county:
        return False
        
    # Include if county matches target counties
    return county in target_counties