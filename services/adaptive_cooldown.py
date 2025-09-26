"""
AdaptiveCooldown Service - Intelligent cooldown system
Extracted from: 03_flink_real_time_analytics.py (lines 52-93) 
Responsibility: Adaptive anti-spam control for outputs
"""

import time


class AdaptiveCooldown:
    """
    Intelligent cooldown system that adapts frequency based on event rate.
    
    Features:
    - Prevents spam when there's high activity
    - Maintains responsiveness with low activity
    - Dynamic range: 0.8-4.0s based on behavior
    - Intelligent protection against visual pollution
    """
    
    def __init__(self):
        self.min_cooldown = 0.8  # 800ms minimum
        self.max_cooldown = 4.0  # 4s maximum
        self.current_cooldown = 1.5  # Balanced start
        self.last_output_time = 0
        self.event_rate_history = []
        self.outputs_blocked = 0
        print("🛡️ AdaptiveCooldown initialized - Intelligent protection active!")
    
    def should_output(self, events_since_last):
        """
        Determines if output should be made based on adaptive cooldown.
        
        Args:
            events_since_last (int): Number of events since last output
            
        Returns:
            tuple: (should_emit: bool, message: str)
        """
        current_time = time.time()
        time_since_last = current_time - self.last_output_time
        
        # Adapt cooldown based on activity
        if events_since_last > 5:  # High activity
            self.current_cooldown = min(self.max_cooldown, self.current_cooldown * 1.2)
        elif events_since_last <= 2:  # Low activity  
            self.current_cooldown = max(self.min_cooldown, self.current_cooldown * 0.9)
        
        if time_since_last >= self.current_cooldown:
            self.last_output_time = current_time
            return True, f"✅ Cooldown OK ({self.current_cooldown:.1f}s)"
        else:
            self.outputs_blocked += 1
            remaining = self.current_cooldown - time_since_last
            return False, f"🛡️ Cooldown active ({remaining:.1f}s remaining)"
    
    def get_stats(self):
        """
        Returns cooldown statistics.
        
        Returns:
            dict: Cooldown efficiency metrics
        """
        total_attempts = self.outputs_blocked + 10  # Conservative estimate
        efficiency = (1 - self.outputs_blocked / max(1, total_attempts)) * 100
        
        return {
            'current_cooldown': self.current_cooldown,
            'outputs_blocked': self.outputs_blocked,
            'efficiency': f"{efficiency:.1f}%",
            'min_cooldown': self.min_cooldown,
            'max_cooldown': self.max_cooldown
        }
    
    def reset(self):
        """Resets cooldown state"""
        self.current_cooldown = 1.5
        self.last_output_time = 0
        self.outputs_blocked = 0
        print("🔄 AdaptiveCooldown reset")