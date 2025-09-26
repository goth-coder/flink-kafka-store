"""
SaleModel - Data model for sales analytics
Represents the structure and business logic of sales data
Responsibility: Define data schema, validation, and business rules
"""

from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import uuid


@dataclass
class SaleModel:
    """
    Data model representing a sale transaction.
    
    This model defines the canonical structure for sales data across the system,
    ensuring consistency between Kafka ingestion and Flink processing.
    
    Attributes:
        id_sale: Unique identifier for the sale
        id_product: Product identifier (ASIN)
        nm_product: Product name/title
        nm_category: Product category
        nm_brand: Product brand
        seller: Seller identifier
        price: Unit price value
        quantity: Number of items sold
        tp_payment: Payment type
        sale_date: Sale timestamp
        proctime: Processing time (auto-generated)
    """
    
    # Core identifiers
    id_sale: str = field(default_factory=lambda: str(uuid.uuid4()))
    id_product: str = ""
    
    # Product information
    nm_product: str = ""
    nm_category: str = "General Category"
    nm_brand: str = "Generic"
    
    # Transaction details
    seller: str = ""
    price: float = 0.0
    quantity: int = 1
    tp_payment: str = "Credit Card"
    
    # Temporal data
    sale_date: str = field(default_factory=lambda: datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
    proctime: Optional[str] = None
    
    # Extended attributes (not required for Kafka table)
    total_value: float = field(init=False)
    discount_percentage: float = 0.0
    product_rating: float = 0.0
    seller_region: str = ""
    sales_channel: str = "Website"
    
    def __post_init__(self):
        """Calculate derived fields after initialization"""
        self.total_value = self.price * self.quantity
        if not self.proctime:
            self.proctime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    
    def to_kafka_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary format expected by Kafka table schema.
        
        Returns:
            dict: Sale data formatted for Kafka ingestion
        """
        return {
            "id_sale": self.id_sale,
            "id_product": self.id_product,
            "nm_product": self.nm_product,
            "nm_category": self.nm_category,
            "nm_brand": self.nm_brand,
            "seller": self.seller,
            "price": self.price,
            "quantity": self.quantity,
            "tp_payment": self.tp_payment,
            "sale_date": self.sale_date
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SaleModel':
        """
        Create SaleModel instance from dictionary data.
        
        Args:
            data: Dictionary containing sale data
            
        Returns:
            SaleModel: Instantiated sale model
        """
        return cls(
            id_sale=data.get('id_sale', str(uuid.uuid4())),
            id_product=data.get('id_product', data.get('asin', '')),
            nm_product=data.get('nm_product', ''),
            nm_category=data.get('nm_category', 'General Category'),
            nm_brand=data.get('nm_brand', 'Generic'),
            seller=data.get('seller', ''),
            price=float(data.get('price', 0.0)),
            quantity=int(data.get('quantity', 1)),
            tp_payment=data.get('tp_payment', 'Credit Card'),
            sale_date=data.get('sale_date', data.get('date', datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'))),
            discount_percentage=float(data.get('discount_percentage', 0.0)),
            product_rating=float(data.get('product_rating', 0.0)),
            seller_region=data.get('seller_region', ''),
            sales_channel=data.get('sales_channel', 'Website')
        )
    
    def validate(self) -> tuple[bool, str]:
        """
        Validate sale data according to business rules.
        
        Returns:
            tuple: (is_valid, error_message)
        """
        if not self.id_sale:
            return False, "Sale ID is required"
        
        if not self.id_product:
            return False, "Product ID is required"
        
        if self.price <= 0:
            return False, "Sale price must be positive"
        
        if self.quantity <= 0:
            return False, "Quantity must be positive"
        
        if not self.seller:
            return False, "Seller is required"
        
        return True, "Valid"
    
    def calculate_metrics(self) -> Dict[str, float]:
        """
        Calculate business metrics for this sale.
        
        Returns:
            dict: Dictionary with calculated metrics
        """
        discount_amount = (self.total_value * self.discount_percentage) / 100
        final_value = self.total_value - discount_amount
        
        return {
            'total_value': self.total_value,
            'discount_amount': discount_amount,
            'final_value': final_value,
            'unit_price': self.price,
            'avg_rating': self.product_rating
        }


@dataclass
class SalesAnalytics:
    """
    Analytics model for aggregated sales data.
    
    Used for Flink sliding window results and real-time analytics.
    """
    
    window_start: str
    window_end: str
    sales_count: int = 0
    total_revenue: float = 0.0
    unique_sellers: int = 0
    avg_sale_value: float = 0.0
    top_category: str = ""
    top_seller: str = ""
    
    def __post_init__(self):
        """Calculate derived metrics"""
        if self.sales_count > 0:
            self.avg_sale_value = self.total_revenue / self.sales_count


# Business validation rules
class SalesBusinessRules:
    """
    Business rules and validation logic for sales data.
    """
    
    # Configuration constants
    MIN_SALE_VALUE = 0.01
    MAX_SALE_VALUE = 50000.0
    MAX_QUANTITY = 1000
    
    # Valid payment types
    VALID_PAYMENT_TYPES = {
        'PIX', 'Credit Card Cash', 'Credit Card Installments',
        'Bank Slip', 'Debit', 'Digital Wallet'
    }
    
    @classmethod
    def validate_sale_value(cls, value: float) -> bool:
        """Validate if sale value is within acceptable range"""
        return cls.MIN_SALE_VALUE <= value <= cls.MAX_SALE_VALUE
    
    @classmethod
    def validate_quantity(cls, quantity: int) -> bool:
        """Validate if quantity is within acceptable range"""
        return 1 <= quantity <= cls.MAX_QUANTITY
    
    @classmethod
    def validate_payment_type(cls, payment_type: str) -> bool:
        """Validate if payment type is accepted"""
        return payment_type in cls.VALID_PAYMENT_TYPES
    
    @classmethod
    def validate_complete_sale(cls, sale: SaleModel) -> tuple[bool, list[str]]:
        """
        Comprehensive validation of a sale model.
        
        Returns:
            tuple: (is_valid, list_of_errors)
        """
        errors = []
        
        # Basic validation
        is_valid, base_error = sale.validate()
        if not is_valid:
            errors.append(base_error)
        
        # Business rule validations
        if not cls.validate_sale_value(sale.price):
            errors.append(f"Sale price {sale.price} outside acceptable range ({cls.MIN_SALE_VALUE}-{cls.MAX_SALE_VALUE})")
        
        if not cls.validate_quantity(sale.quantity):
            errors.append(f"Quantity {sale.quantity} outside acceptable range (1-{cls.MAX_QUANTITY})")
        
        if not cls.validate_payment_type(sale.tp_payment):
            errors.append(f"Invalid payment type: {sale.tp_payment}")
        
        return len(errors) == 0, errors


# Example usage and testing
if __name__ == "__main__":
    print("🧪 TESTING SALES DATA MODEL")
    print("=" * 50)
    
    # Create sample sale
    sample_sale = SaleModel(
        id_product="B001",
        nm_product="Samsung Galaxy Smartphone",
        nm_category="Electronics",
        nm_brand="Samsung",
        seller="ana.silva",
        price=1299.99,
        quantity=2,
        tp_payment="PIX"
    )
    
    print(f"📦 Sample Sale Created:")
    print(f"   ID: {sample_sale.id_sale}")
    print(f"   Product: {sample_sale.nm_product}")
    print(f"   Total Value: R$ {sample_sale.total_value:.2f}")
    
    # Validate sale
    is_valid, errors = SalesBusinessRules.validate_complete_sale(sample_sale)
    print(f"\n✅ Validation: {'PASSED' if is_valid else 'FAILED'}")
    if errors:
        for error in errors:
            print(f"   ❌ {error}")
    
    # Convert to Kafka format
    kafka_data = sample_sale.to_kafka_dict()
    print(f"\n📤 Kafka Format:")
    for key, value in kafka_data.items():
        print(f"   {key}: {value}")
    
    # Calculate metrics
    metrics = sample_sale.calculate_metrics()
    print(f"\n📊 Metrics:")
    for metric, value in metrics.items():
        print(f"   {metric}: {value}")