"""
ConsoleView - View for terminal presentation
Responsibility: Output formatting and presentation
Centralizes all prints in a presentation layer
"""

from datetime import datetime


class ConsoleView:
    """
    View responsible for presenting formatted data in console.
    
    Characteristics:
    - Centralizes all terminal outputs
    - Consistent and visually attractive formatting
    - Clear separation between data and presentation
    - Reuse of standardized layouts
    """
    
    def __init__(self):
        self.output_count = 0
    
    def show_initial_banner(self):
        """Show application initialization banner"""
        print("🚀 FLINK SLIDING WINDOWS - EVENT-DRIVEN MVC ARCHITECTURE")
        print("=" * 75)
        print("📊 CHARACTERISTICS:")
        print("   ✅ MVC architecture with separation of concerns")
        print("   ✅ Event-Driven: Only outputs with new data")
        print("   ✅ Adaptive Cooldown: 0.8-4.0s based on activity")
        print("   ✅ Separate and reusable services")
        print()
        print("🔄 SLIDING WINDOWS - CONFIGURATION:")
        print("   → Window: 15 minutes (analysis period)")
        print("   → Detection: 1 second (high sensitivity)")
        print("   → Parallelism: 3 distributed threads")
        print("=" * 75)
        print()
    
    def show_event_detected(self, window_data, event_info, insights):
        """
        Show detected event with complete formatting.
        
        Args:
            window_data (tuple): Flink window data
            event_info (dict): Event information from EventTracker
            insights (list): List of insights from EventAnalyzer
        """
        self.output_count += 1
        
        current_timestamp = datetime.now().strftime("%H:%M:%S")
        growth = event_info.get('growth_rate', 0)
        count_growth = event_info.get('count_growth', 0)
        
        print(f"\n🆕 EVENT DETECTED #{self.output_count} - {current_timestamp}")
        print(f"📈 GROWTH: +{growth:.1f}% ({count_growth:+d} sales)")
        
        # Window data
        print(f"⏰ PERIOD: {window_data[0]} → {window_data[1]}")
        print(f"🛍️  SALES: {window_data[2]}")
        print(f"💰 REVENUE: ${window_data[3]:,.2f}")
        print(f"🎯 AVERAGE TICKET: ${window_data[4]:,.2f}")
        print(f"👥 SELLERS: {window_data[5]}")
        print(f"📦 CATEGORIES: {window_data[6]}")
        
        # Largest sale (if available)
        if len(window_data) > 8:
            print(f"🏆 LARGEST SALE: ${window_data[8]:,.2f}")
        
        # EventAnalyzer insights
        if insights:
            print("💡 INSIGHTS:")
            for insight in insights:
                print(f"   {insight}")
        
        print("=" * 65)
    
    def show_waiting_for_data(self):
        """Show initial waiting message"""
        print("⏳ Waiting for first data from producer...")
        print("💡 Execute: python models/sales_producer.py (or original producer)")
        print()
    
    def show_services_initialization(self):
        """Show services initialization"""
        print("🎯 AnalyticsController initialized with MVC architecture!")
        print("   → EventTracker: New event detection")  
        print("   → AdaptiveCooldown: Spam protection")
        print("   → EventAnalyzer: Smart insights")
    
    def show_flink_configuration(self):
        """Show Flink configuration"""
        print("🚀 Configuring Apache Flink for Sliding Windows...")
        print("⚡ Parallelism: 3 threads (optimized for 6 partitions)")
        print("⏰ Specializing in moving averages and temporal analysis")
        print("✅ Flink environment configured with CONTROLLED parallelism 3!")
        print("🔧 Early-fire DISABLED, CONSERVATIVE watermarks!")
        print("⚡ Configured for maximum STABILITY with performance!")
    
    def show_table_configuration(self):
        """Show Kafka table configuration"""
        print("📊 Table configured for Sliding Windows (6 partitions, parallelism 3)")
        print("🔑 Waiting for partitioning keys by seller")
    
    def show_error(self, error):
        """Show formatted error message"""
        print(f"❌ Error in Analytics Controller: {error}")
    
    def show_interruption(self):
        """Show interruption message"""
        print(f"\n🛑 Stopping Analytics Controller...")
    
    def show_finalization(self):
        """Show finalization message"""
        print("🏁 Analytics Controller finalized!")
    
    def show_statistics(self, stats):
        """
        Show detailed service statistics.
        
        Args:
            stats (dict): Service statistics
        """
        print("\n📊 SERVICE STATISTICS:")
        print("-" * 50)
        
        for service_name, service_stats in stats.items():
            print(f"\n🔧 {service_name.upper().replace('_', ' ')}:")
            
            if isinstance(service_stats, dict):
                for key, value in service_stats.items():
                    print(f"   {key}: {value}")
            else:
                print(f"   Value: {service_stats}")
    
    def show_producer_banner(self):
        """Specific banner for producer (SalesModel)"""
        print("🚀 MVC PRODUCER - SELLER PARTITIONING")
        print("="*65)
        print("🎯 Strategy: Stable business key (seller)")
        print("📊 Kafka Partitions: 6")
        print("⚡ Configuration: Production-ready with compression")
        print("📈 Metrics: Real-time distribution")
        print("="*65)
        print()
    
    def show_info(self, message):
        """Show simple informative message"""
        print(f"💬 {message}")
    
    def show_success(self, message):
        """Show success message"""
        print(f"✅ {message}")
    
    def show_generated_sales(self, sales):
        """
        Shows formatted list of generated sales.
        
        Args:
            sales (list): List of sales to display
        """
        print(f"📊 {len(sales)} SALES GENERATED WITH AMAZON DATA:")
        print("-" * 50)
        
        for i, sale in enumerate(sales, 1):
            print(f"\n🛍️ SALE {i}:")
            print(f"   📦 {sale['nm_produto'][:50]}...")
            print(f"   🏷️ Category: {sale['nm_categoria']}")
            print(f"   🏪 Brand: {sale['nm_marca']}")
            print(f"   👤 Vendor: {sale['vendedor']}")
            print(f"   💰 Value: R$ {sale['valor']:.2f} x {sale['quantidade']} = R$ {sale['valor'] * sale['quantidade']:.2f}")
            print(f"   💳 Payment: {sale['tp_pagamento']}")
            
        print(f"\n✅ Total of {len(sales)} sales displayed")
    
    def show_amazon_summary(self, total_products, total_categories):
        """
        Shows summary of loaded Amazon data.
        
        Args:
            total_products (int): Total loaded products
            total_categories (int): Total available categories
        """
        print("🌐 AMAZON DATASET LOADED:")
        print(f"   📦 {total_products:,} real products")
        print(f"   🏷️ {total_categories} categories")
        print("   ✅ Professional data for LinkedIn")
    
    def clear_screen(self):
        """Clears screen (optional)"""
        import os
        os.system('clear' if os.name == 'posix' else 'cls')