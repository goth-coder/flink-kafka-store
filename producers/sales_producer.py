"""
SalesModel - Model for sales ingestion into Kafka
Refactored to use realistic Amazon data
Responsibility: Focus only on Kafka ingestion, data comes from AmazonDataGenerator
"""

import json
import time
from confluent_kafka import Producer
from collections import defaultdict
from .amazon_data_generator import AmazonDataGenerator


class SalesModel:
    """
    Model responsible for generating realistic sales data and sending to Kafka.
    
    Characteristics:
    - Uses real Amazon data via AmazonDataGenerator
    - Smart partitioning by seller  
    - Real-time distribution metrics
    - Production-ready configuration with professional data
    """
    
    def __init__(self, kafka_servers='localhost:9092', topic='sales'):
        self.topic = topic
        
        # Optimized producer configuration
        config = {
            'bootstrap.servers': kafka_servers,
            'client.id': 'sales_producer_mvc',
            'acks': 'all',  # Ensure durability
            'retries': 3,   # Retry on failure
            'batch.size': 16384,  # Throughput optimization
            'compression.type': 'snappy'  # Compression for efficiency
        }
        self.producer = Producer(config)
        
        # Initialize real Amazon data generator
        self.amazon_generator = AmazonDataGenerator()
        products_count = len(self.amazon_generator.products_df) if self.amazon_generator.products_df is not None else 0
        print(f"✅ SalesModel initialized with {products_count} Amazon products")
        
        # Distribution metrics
        self.partition_stats = defaultdict(int)
        self.seller_stats = defaultdict(int)
        self.total_messages = 0
    
    def generate_sale(self):
        """
        Generate a sale using real Amazon data via AmazonDataGenerator.
        
        Returns:
            dict: Sale data formatted for Kafka
        """
        # Use Amazon generator to get realistic sale
        realistic_sale = self.amazon_generator.generate_realistic_sale()
        
        # Convert to format expected by Kafka/Flink (maintain compatibility)
        kafka_sale = {
            "id_sale": realistic_sale["id_sale"],
            "id_product": realistic_sale["asin"],
            "nm_product": realistic_sale["nm_product"],
            "nm_category": realistic_sale["nm_category"],
            "nm_brand": realistic_sale["nm_brand"],
            "seller": realistic_sale["seller"],  # Seller for partitioning
            "price": realistic_sale["price"],
            "quantity": realistic_sale["quantity"],
            "tp_payment": realistic_sale["tp_payment"],
            "sale_date": realistic_sale["sale_date"]
        }
        
        # Update seller statistics
        self.seller_stats[realistic_sale["seller"]] += 1
        
        return kafka_sale
    
    def send_sale(self, sale_data=None):
        """
        Send a sale to Kafka with partitioning by seller.
        
        Args:
            sale_data (dict, optional): Sale data. If None, generates fake sale.
            
        Returns:
            dict: Sent sale data
        """
        if sale_data is None:
            sale_data = self.generate_sale()
        
        seller = sale_data['seller']
        sale_data_json = json.dumps(sale_data).encode('utf-8')
        
        # Partitioning key by seller
        partition_key = seller.encode('utf-8')
        
        print(f"📤 Sending sale from seller '{seller}' (partition key)")
        
        self.producer.produce(
            self.topic,
            key=partition_key,  # Business key for smart distribution
            value=sale_data_json,
            callback=self._send_callback
        )
        self.producer.poll(0)
        
        return sale_data
    
    def _send_callback(self, err, msg):
        """Optimized callback with distribution metrics"""
        if err is not None:
            print(f'❌ Message sending failed: {err}')
        else:
            partition = msg.partition()
            self.partition_stats[partition] += 1
            self.total_messages += 1
            
            # Detailed log with distribution info
            print(f'✅ Message #{self.total_messages} → Topic: {msg.topic()} | Partition: [{partition}]')
            
            # Statistics every 10 messages
            if self.total_messages % 10 == 0:
                print(f"\n📊 CURRENT DISTRIBUTION (Total: {self.total_messages})")
                for p in sorted(self.partition_stats.keys()):
                    percentage = (self.partition_stats[p] / self.total_messages) * 100
                    print(f"   Partition {p}: {self.partition_stats[p]} msgs ({percentage:.1f}%)")
                print("-" * 50)
    
    def run_continuous_producer(self, interval=8):
        """
        Run producer continuously sending sales.
        
        Args:
            interval (int): Interval in seconds between sends
        """
        print("🚀 MVC PRODUCER - SELLER PARTITIONING")
        print("="*65)
        print("🎯 Strategy: Stable business key (seller)")
        print("📊 Kafka Partitions: 6")
        print("⚡ Configuration: Production-ready with compression")
        print("📈 Metrics: Real-time distribution")
        print("="*65)
        print()
        
        try:
            while True:
                self.send_sale()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n🛑 Interrupted by user")
            self._show_final_statistics()
        finally:
            self.producer.flush()
            print("\n✅ MVC Producer finalized successfully!")
            print("🎯 Distribution by business key implemented!")
    
    def _show_final_statistics(self):
        """Show final distribution statistics"""
        print(f"\n📊 FINAL STATISTICS:")
        print(f"   Total messages: {self.total_messages}")
        print(f"   Distribution by partition:")
        for p in sorted(self.partition_stats.keys()):
            percentage = (self.partition_stats[p] / self.total_messages) * 100 if self.total_messages > 0 else 0
            print(f"     Partition {p}: {self.partition_stats[p]} msgs ({percentage:.1f}%)")
        
        print(f"\n👥 Top 5 most active sellers:")
        top_sellers = sorted(self.seller_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        for seller, count in top_sellers:
            percentage = (count / self.total_messages) * 100 if self.total_messages > 0 else 0
            print(f"     {seller}: {count} sales ({percentage:.1f}%)")
    
    def get_stats(self):
        """
        Return current producer statistics.
        
        Returns:
            dict: Distribution statistics
        """
        return {
            'total_messages': self.total_messages,
            'partition_stats': dict(self.partition_stats),
            'seller_stats': dict(self.seller_stats)
        }


# Direct execution script (compatibility)
if __name__ == "__main__":
    model = SalesModel()
    model.run_continuous_producer()