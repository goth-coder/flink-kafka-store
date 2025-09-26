#!/usr/bin/env python3
"""
Main - Single entry point for the MVC application
Replaces the original 03_flink_real_time_analytics.py
Architecture: MVC with separation of responsibilities
Manual Kafka control via --kafka

Usage:
    python main.py              # Run analytics (assumes Kafka is already running)
    python main.py --producer   # Run producer (assumes Kafka is already running)
    python main.py --kafka      # Start only Kafka
    python main.py --stats      # Show statistics
"""

import sys
import argparse
import time
from controllers.analytics_controller import AnalyticsController
from producers.sales_producer import SalesModel
from views.console_view import ConsoleView
from utils.kafka_manager import KafkaManager


class MainApplication:
    """
    Main application that coordinates the entire MVC architecture.
    
    Responsibilities:
    - Single application entry point
    - Command routing (analytics, producer, kafka, stats)
    - MVC architecture initialization
    - Command line argument handling
    - Manual Kafka management via --kafka
    """
    
    def __init__(self):
        self.view = ConsoleView()
        self.controller = None
        self.model = None
        self.kafka_manager = KafkaManager()
    
    def _check_kafka_available(self):
        """
        Check if Kafka is available (without auto-starting).
        
        Returns:
            bool: True if Kafka is ready for use
        """
        if self.kafka_manager.kafka_is_ready(timeout=3):
            self.view.show_success("Kafka is available!")
            return True
        else:
            self.view.show_error("Kafka is not available")
            self.view.show_info("💡 Use 'python main.py --kafka' to start Kafka first")
            return False
    
    def run_analytics(self):
        """Run analytics (assumes Kafka is already running)"""
        try:
            # Check if Kafka is available (without auto-start)
            if not self._check_kafka_available():
                return
            
            self.view.show_initial_banner()
            
            # Initialize controller (with dependency injection)
            self.controller = AnalyticsController()
            
            # Execute main processing
            self.controller.run_analytics()
            
        except KeyboardInterrupt:
            self.view.show_interruption()
        except Exception as e:
            self.view.show_error(str(e))
        finally:
            self.view.show_finalization()
    
    def run_producer(self):
        """Run sales producer (assumes Kafka is already running)"""
        try:
            # Check if Kafka is available (without auto-start)
            if not self._check_kafka_available():
                return
            
            self.view.show_producer_banner()
            
            # Initialize model
            self.model = SalesModel()
            
            # Main producer loop
            counter = 0
            try:
                while counter < 1000:  # Limit for demonstration
                    sale = self.model.generate_sale()
                    self.model.send_sale(sale)
                    
                    counter += 1
                    time.sleep(2)  # 2 seconds between sales
                    
                    # Show statistics every 10 sales
                    if counter % 10 == 0:
                        stats = {
                            "total_sales": counter,
                            "seller_stats": dict(self.model.seller_stats)
                        }
                        self.view.show_statistics(stats)
            except KeyboardInterrupt:
                self.view.show_interruption()
                print(f"\n📊 Total sales sent: {counter}")
                raise
        except Exception as e:
            self.view.show_error(str(e))
        finally:
            if self.model:
                # Final producer flush
                self.model.producer.flush()
            self.view.show_finalization()
    
    def run_kafka(self):
        """Start only the Kafka server"""
        try:
            self.view.show_info("KAFKA MODE - Server Control")
            self.view.show_info("=" * 40)
            
            # Check if already running first
            if self.kafka_manager.kafka_is_ready(timeout=3):
                self.view.show_info("Kafka is already running")
                
                # Show summarized status
                status = self.kafka_manager.get_status()
                self.view.show_info(f"Status: process={status['process_running']}, port={status['port_open']}")
                self.view.show_info("Use Ctrl+C to exit")
                
            else:
                # Try to initialize Kafka (prints are done by KafkaManager)
                if self.kafka_manager.ensure_kafka_running():
                    self.view.show_success("Kafka started successfully!")
                    
                    # Show summarized status
                    status = self.kafka_manager.get_status()
                    self.view.show_info(f"Server: {status['bootstrap_server']}")
                else:
                    self.view.show_error("Failed to initialize Kafka")
                    self.view.show_info("Check logs in /home/airflow/kafka/kafka.log")
                    return
            
            # Keep monitoring (more silent)
            self.view.show_info("Kafka running. Ctrl+C to exit (Kafka will continue)")
            
            try:
                check_count = 0
                while True:
                    time.sleep(30)  # Check every 30s (was 10s)
                    check_count += 1
                    
                    if not self.kafka_manager.kafka_is_running():
                        self.view.show_error("Kafka stopped unexpectedly!")
                        break
                    else:
                        # Show status only every 5 checks (2.5 min)
                        if check_count % 5 == 0:
                            self.view.show_info(f"Kafka OK ({check_count * 30}s running)")
            except KeyboardInterrupt:
                self.view.show_info("Exiting... (Kafka continues running)")
                
        except Exception as e:
            self.view.show_error(f"Error in Kafka mode: {str(e)}")
        finally:
            self.view.show_info("Kafka mode finalized")
    
    def show_statistics(self):
        """Show service statistics (if available)"""
        if self.controller is None:
            print("⚠️ Controller not initialized. Run analytics first.")
            return
        
        try:
            stats = self.controller.get_stats()
            self.view.show_statistics(stats)
        except Exception as e:
            self.view.show_error(f"Error getting statistics: {e}")
    
    def show_help(self):
        """Show usage information"""
        print("🚀 FLINK KAFKA STORE - MVC ARCHITECTURE")
        print("=" * 50)
        print("AVAILABLE COMMANDS:")
        print("  python main.py              # Run analytics (default)")
        print("  python main.py --analytics  # Run analytics (explicit)")
        print("  python main.py --producer   # Run sales producer")
        print("  python main.py --kafka      # Start only Kafka")  
        print("  python main.py --stats      # Show statistics")
        print("  python main.py --help       # Show this help")
        print()
        print("RECOMMENDED WORKFLOW:")
        print("  Terminal 1: python main.py --kafka      # Start Kafka")
        print("  Terminal 2: python main.py --producer   # Produce data")
        print("  Terminal 3: python main.py --analytics  # Consume/analyze")
        print()
        print("ARCHITECTURE:")
        print("  📁 models/      # SalesModel (data and Kafka)")
        print("  📁 views/       # ConsoleView (presentation)")
        print("  📁 controllers/ # AnalyticsController (coordination)")
        print("  📁 services/    # EventTracker, AdaptiveCooldown, EventAnalyzer")
        print("  📁 utils/       # KafkaManager (manual control)")
        print()
        print("FUNCTIONALITY:")
        print("  ✅ Event-Driven: Only outputs with new data")
        print("  ✅ Adaptive Cooldown: 0.8-4.0s based on activity")
        print("  ✅ Sliding Windows: 15min analysis, 1s detection")
        print("  ✅ Partitioning: 6 partitions by seller")
        print("  ✅ Manual control: Each terminal runs what it needs")
        print("  ✅ Amazon data: Real products for LinkedIn")


def create_parser():
    """Create parser for command line arguments"""
    parser = argparse.ArgumentParser(
        description='Flink Kafka Store - Event-Driven MVC Architecture (manual Kafka control)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Mutually exclusive group for main commands
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--analytics', action='store_true',
                      help='Run Flink analytics (assumes Kafka is running)')
    group.add_argument('--producer', action='store_true', 
                      help='Run sales producer (assumes Kafka is running)')
    group.add_argument('--kafka', action='store_true',
                      help='Start only the Kafka server')
    group.add_argument('--stats', action='store_true',
                      help='Show service statistics')
    group.add_argument('--help-app', action='store_true',
                      help='Show detailed application help')
    
    return parser


def main():
    """Main function"""
    parser = create_parser()
    args = parser.parse_args()
    
    app = MainApplication()
    
    try:
        if args.producer:
            app.run_producer()
        elif args.kafka:
            app.run_kafka()
        elif args.stats:
            app.show_statistics()
        elif args.help_app:
            app.show_help()
        else:
            # Default behavior: analytics
            app.run_analytics()
            
    except Exception as e:
        app.view.show_error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()