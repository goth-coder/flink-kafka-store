"""
AmazonDataGenerator - Realistic data generator based on Amazon dataset
Responsibility: Read local CSVs and generate realistic sales for LinkedIn project
"""

import pandas as pd
import random
import os
from datetime import datetime, UTC
from faker import Faker


class AmazonDataGenerator:
    """
    Realistic sales data generator based on Amazon Products 2023 dataset.
    
    Characteristics:
    - Reads real Amazon product data from local CSVs
    - Combines with simulated Brazilian seller data
    - Generates sales with real prices, categories and products
    - Maintains consistency for Kafka partitioning
    """
    
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.fake = Faker('pt_BR')
        
        # Brazilian seller data (maintains consistency for partitioning)
        self.brazilian_sellers = [
            'ana.silva', 'bruno.costa', 'carla.santos', 'diego.lima',
            'elena.rocha', 'fabio.gomes', 'giana.alves', 'hugo.melo',
            'iris.nunes', 'joao.souza', 'karen.dias', 'lucas.reis',
            'marina.oliveira', 'nicolas.pereira', 'olivia.martins', 'pedro.almeida'
        ]
        
        # Brazilian payment methods
        self.payment_methods = [
            'PIX', 'Credit Card Cash', 'Credit Card Installments',
            'Bank Slip', 'Debit', 'Digital Wallet'
        ]
        
        # Load CSV data
        self.products_df = None
        self.categorias_df = None
        self._load_amazon_data()
        
        print("🚀 AmazonDataGenerator initialized!")
        print(f"   📦 {len(self.products_df)} Amazon products loaded") # type: ignore
        print(f"   🏪 {len(self.categorias_df)} categories available") # type: ignore
        print(f"   👥 {len(self.brazilian_sellers)} Brazilian sellers")
    
    def _load_amazon_data(self):
        """Load data from Amazon CSVs"""
        try:
            # Load products
            products_path = os.path.join(self.data_dir, "amazon_products.csv")
            if os.path.exists(products_path):
                # Load sample for performance (first 10k products)
                self.products_df = pd.read_csv(products_path, nrows=10000)
                print(f"✅ Products loaded: {len(self.products_df)} items")
            else:
                print(f"⚠️  File not found: {products_path}")
                self._create_fallback_data()
                return
            
            # Load categories
            categories_path = os.path.join(self.data_dir, "amazon_categories.csv")
            if os.path.exists(categories_path):
                self.categorias_df = pd.read_csv(categories_path)
                print(f"✅ Categories loaded: {len(self.categorias_df)} categories")
            else:
                print(f"⚠️  File not found: {categories_path}")
                self._create_fallback_categories()
            
            # Clean NaN data
            self._clean_data()
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            self._create_fallback_data()
    
    def _clean_data(self):
        """Clean and prepare data for use"""
        if self.products_df is not None:
            # Remove products without price or with price 0
            self.products_df = self.products_df[
                (self.products_df['price'] > 0) & 
                (self.products_df['price'].notna())
            ]
            
            # Ensure title is not NaN
            self.products_df = self.products_df[self.products_df['title'].notna()]
            
            print(f"🧹 Data cleaned: {len(self.products_df)} valid products")
    
    def _create_fallback_data(self):
        """Create fallback data if CSVs are not available"""
        print("📋 Creating fallback data...")
        
        fallback_products = [
            {"asin": "B001", "title": "Samsung Galaxy Smartphone", "price": 1299.99, "category_id": 1},
            {"asin": "B002", "title": "Dell Inspiron Notebook", "price": 2499.99, "category_id": 2},
            {"asin": "B003", "title": "LG 55-inch Smart TV", "price": 1899.99, "category_id": 3},
            {"asin": "B004", "title": "Sony WH-1000XM4 Headphones", "price": 899.99, "category_id": 4},
            {"asin": "B005", "title": "Apple iPad Air", "price": 2199.99, "category_id": 5},
            {"asin": "B006", "title": "Logitech MX Master Mouse", "price": 299.99, "category_id": 6},
            {"asin": "B007", "title": "Corsair Mechanical Keyboard", "price": 459.99, "category_id": 7},
            {"asin": "B008", "title": "ASUS 27-inch Gaming Monitor", "price": 1199.99, "category_id": 8},
            {"asin": "B009", "title": "DXRacer Gaming Chair", "price": 899.99, "category_id": 9},
            {"asin": "B010", "title": "Blue Yeti Microphone", "price": 649.99, "category_id": 10}
        ]
        
        self.products_df = pd.DataFrame(fallback_products)
        print("✅ Fallback products created")
    
    def _create_fallback_categories(self):
        """Create fallback categories"""
        fallback_categories = [
            {"id": 1, "category_name": "Smartphones & Mobile"},
            {"id": 2, "category_name": "Computers & Notebooks"},
            {"id": 3, "category_name": "TV & Home Theater"},
            {"id": 4, "category_name": "Audio & Headphones"},
            {"id": 5, "category_name": "Tablets & E-readers"},
            {"id": 6, "category_name": "Peripherals & Accessories"},
            {"id": 7, "category_name": "Gaming & Keyboards"},
            {"id": 8, "category_name": "Monitors & Displays"},
            {"id": 9, "category_name": "Furniture & Chairs"},
            {"id": 10, "category_name": "Audio Equipment"}
        ]
        
        self.categorias_df = pd.DataFrame(fallback_categories)
    
    def generate_realistic_sale(self):
        """
        Generate realistic sale combining Amazon data with Brazilian sellers.
        
        Returns:
            dict: Generated sale data with real Amazon products
        """
        # Select random product
        product = self.products_df.sample(n=1).iloc[0] # type: ignore
        
        # Find category
        category_name = "General Category"
        if self.categorias_df is not None:
            category_row = self.categorias_df[self.categorias_df['id'] == product['category_id']]
            if not category_row.empty:
                category_name = category_row.iloc[0]['category_name']
        
        # Select Brazilian seller (for consistent partitioning)
        seller = random.choice(self.brazilian_sellers)
        
        # Generate quantity (favors smaller sales, more realistic)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        
        # Calculate realistic discount (if listPrice exists)
        original_price = product.get('listPrice', product['price'])
        if original_price > product['price']:
            discount_pct = ((original_price - product['price']) / original_price) * 100
        else:
            discount_pct = 0
        
        # Create realistic sale
        sale = {
            'id_sale': self.fake.uuid4(),
            'asin': product['asin'],
            'nm_product': product['title'][:100],  # Limit size
            'nm_category': category_name,
            'nm_brand': self._extract_brand(product['title']),
            'seller': seller,
            'price': round(float(product['price']), 2),
            'quantity': quantity,
            'total_value': round(float(product['price']) * quantity, 2),
            'discount_percentage': round(discount_pct, 1),
            'tp_payment': random.choice(self.payment_methods),
            'product_rating': product.get('stars', round(random.uniform(3.5, 5.0), 1)),
            'sale_date': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            'seller_region': self._generate_brazilian_region(),
            'sales_channel': random.choice(['Website', 'Mobile App', 'Marketplace', 'Physical Store'])
        }
        
        return sale
    
    def _extract_brand(self, title):
        """Extract brand from product title"""
        known_brands = [
            'Apple', 'Samsung', 'Dell', 'HP', 'Lenovo', 'LG', 'Sony', 'ASUS',
            'Corsair', 'Logitech', 'Microsoft', 'Google', 'Amazon', 'Xiaomi'
        ]
        
        for brand in known_brands:
            if brand.lower() in title.lower():
                return brand
        
        # If no known brand found, use first word
        words = title.split()
        return words[0] if words else "Generic"
    
    def _generate_brazilian_region(self):
        """Generate Brazilian region for seller"""
        regions = [
            'São Paulo - SP', 'Rio de Janeiro - RJ', 'Belo Horizonte - MG',
            'Brasília - DF', 'Salvador - BA', 'Fortaleza - CE',
            'Recife - PE', 'Porto Alegre - RS', 'Curitiba - PR', 'Goiânia - GO'
        ]
        return random.choice(regions)
    def generate_sales_batch(self, quantity=10):
        """
        Generate a batch of sales for mass simulation.
        
        Args:
            quantity (int): Number of sales to generate
            
        Returns:
            list: List of generated sales
        """
        return [self.generate_realistic_sale() for _ in range(quantity)]
    
    def get_statistics(self):
        """Return statistics of loaded data"""
        if self.products_df is not None:
            min_price = self.products_df['price'].min()
            max_price = self.products_df['price'].max()
            avg_price = self.products_df['price'].mean()
            
            return {
                'total_products': len(self.products_df),
                'total_categories': len(self.categorias_df) if self.categorias_df is not None else 0,
                'minimum_price': f"R$ {min_price:.2f}",
                'maximum_price': f"R$ {max_price:.2f}",
                'average_price': f"R$ {avg_price:.2f}",
                'total_sellers': len(self.brazilian_sellers)
            }
        return {}


# Generator test
if __name__ == "__main__":
    # Create generator
    generator = AmazonDataGenerator()
    
    # Test sale generation
    print("\n🧪 SALE GENERATION TEST:")
    print("=" * 60)
    
    test_sale = generator.generate_realistic_sale()
    for key, value in test_sale.items():
        print(f"{key}: {value}")
    
    # Show statistics
    print("\n📊 DATA STATISTICS:")
    print("=" * 40)
    stats = generator.get_statistics()
    for key, value in stats.items():
        print(f"{key}: {value}")