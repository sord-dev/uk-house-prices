import os

import structlog
from dotenv import load_dotenv

load_dotenv()

LAND_REGISTRY_URLS = {
    'full': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-complete.csv',
    'yearly': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-{year}.csv',
    'monthly': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv',
}

ONS_POSTCODE_URL = 'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-latest-centroids.csv'

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'dbname': os.getenv('POSTGRES_DB', 'house_prices'),
    'user': os.getenv('POSTGRES_USER', 'prices'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}


class IngestionError(Exception):
    pass

class DatabaseError(IngestionError):
    pass

class DownloadError(IngestionError):
    pass

class ValidationError(IngestionError):
    pass


structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
