"""
Producers package for flink-kafka-store
Contains data generators and Kafka producers for sales data injection
"""

from .sales_producer import SalesModel
from .amazon_data_generator import AmazonDataGenerator

__all__ = [
    'SalesModel',
    'AmazonDataGenerator'
]