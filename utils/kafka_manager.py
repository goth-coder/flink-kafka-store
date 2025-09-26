"""
KafkaManager - Utility for automatic Kafka management
Responsibility: Detect Kafka status and initialize automatically
"""

import subprocess
import time
import os
import signal
import socket
from pathlib import Path


class KafkaManager:
    """
    Automatic Kafka manager for the MVC system.
    
    Features:
    - Automatic Kafka status detection
    - Automatic initialization via shell script
    - Connectivity verification
    - Configurable timeout for startup
    """
    
    def __init__(self, kafka_home="/home/airflow/kafka", project_dir=None):
        self.kafka_home = kafka_home
        if project_dir is None:
            project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.project_dir = project_dir
        # Use project script
        self.script_path = os.path.join(self.project_dir, "scripts", "start_kafka_kraft.sh")
        self.bootstrap_server = "localhost:9092"
    
    def kafka_is_running(self):
        """
        Checks if Kafka process is running.
        
        Returns:
            bool: True if Kafka is running
        """
        try:
            # Check Kafka process using ps aux (more robust)
            result = subprocess.run(
                ['ps', 'aux'],
                capture_output=True, text=True, timeout=10
            )
            return 'kafka.Kafka' in result.stdout and 'server.properties' in result.stdout
        except Exception as e:
            print(f"❌ Error checking Kafka process: {e}")
            return False
            
            # Fallback: check KafkaRaftServer process
            result2 = subprocess.run(
                ["pgrep", "-f", "KafkaRaftServer"], 
                capture_output=True, 
                text=True
            )
            
            return result2.returncode == 0
        except:
            return False
    
    def port_is_open(self, host="localhost", port=9092, timeout=3):
        """
        Checks if Kafka port is open and accepting connections.
        
        Args:
            host (str): Kafka host
            port (int): Kafka port
            timeout (int): Connection test timeout
            
        Returns:
            bool: True if port is open
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception:
            return False
    
    def kafka_is_ready(self, timeout=10):
        """
        Checks if Kafka is completely ready (process + connectivity).
        
        Args:
            timeout (int): Connectivity test timeout
            
        Returns:
            bool: True if Kafka is fully ready
        """
        if not self.kafka_is_running():
            return False
        
        if not self.port_is_open(timeout=timeout):
            return False
        
        # Optional test with kafka-topics (more fault tolerant)
        try:
            result = subprocess.run([
                f"{self.kafka_home}/bin/kafka-topics.sh",
                "--bootstrap-server", self.bootstrap_server,
                "--list"
            ], 
            capture_output=True, 
            text=True, 
            timeout=3  # Shorter timeout
            )
            # Return True if command executed without error
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
            # If it fails but process and port are OK, assume it's working
            # (kafka-topics command can be slow to respond)
            return True
    
    def wait_for_kafka_ready(self, max_wait_time=30):
        """
        Waits for Kafka to become completely ready after initialization.
        
        Args:
            max_wait_time (int): Maximum time to wait in seconds
            
        Returns:
            bool: True if Kafka became ready within time limit
        """
        start_time = time.time()
        check_count = 0
        
        while time.time() - start_time < max_wait_time:
            check_count += 1
            
            # Basic check: process + port (more reliable)
            if self.kafka_is_running() and self.port_is_open(timeout=3):
                
                # Try kafka-topics command only once, without blocking
                try:
                    result = subprocess.run([
                        f"{self.kafka_home}/bin/kafka-topics.sh",
                        "--bootstrap-server", self.bootstrap_server,
                        "--list"
                    ], 
                    capture_output=True, 
                    text=True, 
                    timeout=3  # Shorter timeout
                    )
                    
                    if result.returncode == 0:
                        elapsed = int(time.time() - start_time)
                        print(f"✅ Kafka ready! (took {elapsed}s)")
                        return True
                        
                except Exception:
                    # If command fails but process and port OK, assume ready
                    elapsed = int(time.time() - start_time)
                    print(f"✅ Kafka ready! Process and port OK (took {elapsed}s)")
                    return True
            
            # Progressive feedback only every 10 seconds
            if check_count % 5 == 0 and check_count > 2:
                elapsed = int(time.time() - start_time)
                print(f"⏳ Still waiting... ({elapsed}s/{max_wait_time}s)")
            
            time.sleep(2)
        
        print(f"❌ Timeout: Kafka didn't become ready in {max_wait_time}s")
        return False
    
    def start_kafka(self, max_attempts=3):
        """
        Starts Kafka using shell script.
        
        Args:
            max_attempts (int): Maximum initialization attempts
            
        Returns:
            bool: True if initialization was successful
        """
        print("🚀 Starting Kafka...")
        
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                print(f"🔄 Attempt {attempt}/{max_attempts}")
            
            try:
                # Run initialization script in background
                process = subprocess.Popen([
                    "bash", self.script_path
                ], 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
                )
                
                print("📋 Script started, waiting for process to appear...")
                
                # Wait up to 15 seconds for process to appear
                for wait_time in range(15):
                    if self.kafka_is_running():
                        print("✅ Kafka process detected!")
                        return True
                    time.sleep(1)
                
                print("❌ Kafka process didn't appear within expected time")
                    
            except Exception as e:
                print(f"❌ Execution error: {str(e)}")
            
            if attempt < max_attempts:
                print("⏳ Waiting before next attempt...")
                time.sleep(5)
        
        return False
    
    def ensure_kafka_running(self):
        """
        Ensures Kafka is running, starting automatically if necessary.
        
        Returns:
            bool: True if Kafka is running at the end
        """
        print("🔍 Checking Kafka...")
        
        # Quick first check
        if self.kafka_is_ready(timeout=3):
            print("✅ Kafka is already ready!")
            return True
        
        if self.kafka_is_running():
            print("⏳ Process running, waiting to be ready...")
            return self.wait_for_kafka_ready(max_wait_time=30)
        
        print("❌ Kafka is not running")
        
        # Check if script exists
        if not os.path.exists(self.script_path):
            print(f"❌ Script not found: {self.script_path}")
            return False
        
        # Start Kafka
        if self.start_kafka():
            return self.wait_for_kafka_ready(max_wait_time=45)
        else:
            print("❌ Failed to start Kafka")
            return False
    
    def get_status(self):
        """
        Returns detailed Kafka status.
        
        Returns:
            dict: Detailed status
        """
        return {
            "process_running": self.kafka_is_running(),
            "port_open": self.port_is_open(),
            "fully_ready": self.kafka_is_ready(timeout=5),
            "bootstrap_server": self.bootstrap_server,
            "kafka_home": self.kafka_home
        }