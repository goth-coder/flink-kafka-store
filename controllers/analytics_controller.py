"""
AnalyticsController - Main controller to coordinate analytics
Responsibility: Orchestrate services and coordinate event-driven flow
"""

import sys
import signal
import time
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Separated service imports
from services.event_tracker import EventTracker
from services.adaptive_cooldown import AdaptiveCooldown
from services.event_analyzer import EventAnalyzer


class AnalyticsController:
    """
    Main controller that coordinates event-driven Flink processing.
    
    Responsibilities:
    - Orchestrate services (EventTracker, AdaptiveCooldown, EventAnalyzer)
    - Configure optimized Flink environment
    - Control main event-driven loop
    - Manage application lifecycle
    """
    
    def __init__(self, kafka_server='localhost:9092', topic='sales', consumer_group='flink-sliding-window'):
        """Initialize controller with dependency injection"""
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer_group = consumer_group
        self.running = True
        
        # Initialize services (dependency injection)
        self.event_tracker = EventTracker()
        self.cooldown_manager = AdaptiveCooldown()
        self.event_analyzer = EventAnalyzer()
        
        self._setup_flink_environment()
        self._setup_signal_handler()
        
        print("🎯 AnalyticsController initialized with MVC architecture!")
        print("   → EventTracker: New event detection")  
        print("   → AdaptiveCooldown: Spam protection")
        print("   → EventAnalyzer: Smart insights")
    
    def _setup_signal_handler(self):
        """Configura handler para Ctrl+C"""
        def signal_handler(signum, frame):
            print(f"\n🛑 Parando Analytics Controller...")
            self.running = False
            sys.exit(0)
            
        signal.signal(signal.SIGINT, signal_handler)
    
    def _setup_flink_environment(self):
        """Configures optimized Flink environment for Sliding Windows with parallelism"""
        print("🚀 Configuring Apache Flink for Sliding Windows...")
        print("⚡ Parallelism: 3 threads (optimized for 6 partitions)")
        print("⏰ Specializing in moving averages and temporal analysis")
        
        # Optimized configuration for temporal windowing with parallelism
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(3)  # Optimized parallelism for 6 partitions
        
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.tbl_env = StreamTableEnvironment.create(
            stream_execution_environment=self.env,
            environment_settings=settings
        )
        
        # Adicionar JAR do conector Kafka
        kafka_jar = '/home/airflow/flink-2.1.0/lib/flink-sql-connector-kafka-4.0.1-2.0.jar'
        self.tbl_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar}")
        
        # Configure for CONTROLLED temporal windowing with parallelism
        config = self.tbl_env.get_config().get_configuration()
        config.set_string("table.exec.state.ttl", "20 min")  # TTL maior que janela
        
        # Mini-batch CONTROLADO (menos agressivo)
        config.set_string("table.exec.mini-batch.enabled", "true")  
        config.set_string("table.exec.mini-batch.allow-latency", "2s")  # 2s latência (mais controlado)
        config.set_string("table.exec.mini-batch.size", "1000")  # Batch size maior
        
        # Settings for STABLE parallelism
        config.set_string("taskmanager.memory.process.size", "2gb")
        config.set_string("taskmanager.numberOfTaskSlots", "3")  
        config.set_string("parallelism.default", "3")
        
        # Watermarks CONSERVADORAS (evita duplicações)
        config.set_string("table.exec.source.idle-timeout", "5s")  # Timeout maior
        config.set_string("pipeline.watermark-alignment.allow-unaligned-splits", "false")  # Alinhamento forçado
        
        # DESABILITAR Early Fire (principal causa do problema!)
        config.set_string("table.exec.emit.early-fire.enabled", "false")  # ❌ NO early trigger
        
        # State backend otimizado
        config.set_string("state.backend", "hashmap")
        config.set_string("table.exec.state.ttl", "25 min")
        
        print("✅ Flink environment configured with CONTROLLED parallelism 3!")
        print("🔧 Early-fire DISABLED, CONSERVATIVE watermarks!")
        print("⚡ Configured for maximum STABILITY with performance!")
    
    def _setup_kafka_table(self):
        """Creates optimized Kafka table for temporal windowing with 6 partitions"""
        create_kafka_source_ddl = f"""
            CREATE TABLE tb_sales_stream (
                id_sale STRING,
                id_product STRING,
                nm_product STRING,
                nm_category STRING,
                nm_brand STRING,
                seller STRING,
                price DOUBLE,
                quantity INT,
                tp_payment STRING,
                sale_date STRING,
                proctime AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.topic}',
                'properties.bootstrap.servers' = '{self.kafka_server}',
                'properties.group.id' = '{self.consumer_group}',
                'scan.startup.mode' = 'latest-offset',
                'properties.auto.offset.reset' = 'latest',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'scan.parallelism' = '3'
            )
        """
        self.tbl_env.execute_sql(create_kafka_source_ddl)
        print("📊 Table configured for Sliding Windows (6 partitions, parallelism 3)")
        print("🔑 Waiting for seller partitioning keys")
    
    def _create_sliding_window_query(self):
        """Creates optimized query for sliding windows (15min/1s)"""
        query_sql = f"""
        SELECT 
            HOP_START(proctime, INTERVAL '1' SECOND, INTERVAL '15' MINUTE) as window_start,
            HOP_END(proctime, INTERVAL '1' SECOND, INTERVAL '15' MINUTE) as window_end,
            COUNT(*) as sales_in_window,
            ROUND(SUM(price * quantity), 2) as revenue_15min,
            ROUND(AVG(price * quantity), 2) as avg_ticket,
            COUNT(DISTINCT seller) as active_sellers,
            COUNT(DISTINCT nm_category) as active_categories,
            COUNT(DISTINCT nm_product) as active_products,
            MAX(price * quantity) as biggest_sale
        FROM tb_sales_stream 
        GROUP BY HOP(proctime, INTERVAL '1' SECOND, INTERVAL '15' MINUTE)
        """
        
        result_table = self.tbl_env.sql_query(query_sql)
        return result_table.execute().collect()
    
    def run_analytics(self):
        """
        Execute main event-driven analytics loop.
        Replaces the main() function from original code.
        """
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
        
        # Configure Flink environment
        self._setup_kafka_table()
        
        print("⏳ Waiting for first producer data...")
        print("💡 Execute: python main.py --producer (or run producer)")
        print()
        
        # Loop principal event-driven (extrai para view depois)
        try:
            iterator = self._create_sliding_window_query()
            
            for window_data in iterator:
                if not self.running:
                    break
                
                # Check if it's a new event using EventTracker
                current_count = int(window_data[2]) if window_data[2] is not None else 0
                current_revenue = float(window_data[3]) if window_data[3] is not None else 0.0 
                current_time = datetime.now()
                
                event_info = self.event_tracker.is_new_event(current_count, current_revenue, current_time)
                
                if not event_info['is_new']:
                    continue  # Skip without new event
                
                # Check cooldown using AdaptiveCooldown
                should_emit, cooldown_msg = self.cooldown_manager.should_output(event_info['events_since_last'])
                
                if not should_emit:
                    continue  # Skip due to cooldown
                
                # Analyze event using EventAnalyzer
                insights = self.event_analyzer.analyze_event(window_data)
                
                # Output formatted (will move to View later)
                self._display_window_event(window_data, event_info, insights)
                
                # Update EventTracker state
                self.event_tracker.update_state(current_count, current_revenue)
                
        except Exception as e:
            print(f"❌ Error in Analytics Controller: {e}")
        finally:
            print("🏁 Analytics Controller finalized!")
    
    def _display_window_event(self, window_data, event_info, insights):
        """
        Temporary display - will be moved to the View layer later.
        Keeps the same formatting as the original code.
        """
        timestamp_current = datetime.now().strftime("%H:%M:%S")
        growth = event_info.get('growth_rate', 0)
        count_growth = event_info.get('count_growth', 0)
        print(f"\n🆕 EVENT DETECTED - {timestamp_current}")
        print(f"📈 GROWTH: +{growth:.1f}% ({count_growth:+d} sales)")
        print(f"⏰ PERIOD: {window_data[0]} → {window_data[1]}")
        print(f"🛍️  SALES: {window_data[2]}")
        print(f"💰 REVENUE: R$ {window_data[3]:,.2f}")
        print(f"🎯 AVERAGE TICKET: R$ {window_data[4]:,.2f}")
        print(f"👥 SELLERS: {window_data[5]}")
        print(f"📦 CATEGORIES: {window_data[6]}")
        print(f"🏆 BIGGEST SALE: R$ {window_data[8]:,.2f}")
        if insights:
            print("💡 INSIGHTS:")
            for insight in insights:
                print(f"   {insight}")
        
        print("=" * 65)
    
    def get_stats(self):
        """Returns statistics from all services"""
        return {
            'event_tracker': self.event_tracker.get_stats(),
            'cooldown_manager': self.cooldown_manager.get_stats(),
            'event_analyzer': self.event_analyzer.get_stats()
        }