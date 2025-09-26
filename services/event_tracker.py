"""
EventTracker Service - New event detection
Responsibility: Event-driven logic for sliding windows
"""

from datetime import datetime


class EventTracker:
    """
    Service to detect new events and control when sliding windows should be emitted.
    
    Features:
    - Event-Driven: Only emits windows when there's actually new data
    - Multiple metrics: Count + revenue for robustness  
    - Growth tracking: Calculates growth rate
    - Zero false positives: Precise change detection
    """
    
    def __init__(self):
        self.last_window_count = 0
        self.last_event_time = None
        self.last_revenue = 0.0
        self.events_since_last_output = 0
        print("🎯 EventTracker initialized - Event-Driven mode active!")
    
    def is_new_event(self, current_count, current_revenue, current_time):
        """
        Detects if there's a new event based on multiple metrics.
        
        Args:
            current_count (int): Number of sales in current window
            current_revenue (float): Revenue of current window  
            current_time (datetime): Current timestamp
            
        Returns:
            dict: Information about detected event
        """
        # Check if there was real change in data
        count_changed = current_count > self.last_window_count
        revenue_changed = abs(current_revenue - self.last_revenue) > 0.01
        
        if count_changed or revenue_changed:
            self.events_since_last_output += 1
            growth_rate = 0
            if self.last_window_count > 0:
                growth_rate = ((current_count - self.last_window_count) / self.last_window_count) * 100
            
            return {
                'is_new': True,
                'count_growth': current_count - self.last_window_count,
                'growth_rate': growth_rate,
                'events_since_last': self.events_since_last_output
            }
        
        return {'is_new': False}
    
    def update_state(self, current_count, current_revenue):
        """
        Updates state after emitting a window.
        
        Args:
            current_count (int): Current count for next comparison
            current_revenue (float): Current revenue for next comparison
        """
        self.last_window_count = current_count
        self.last_revenue = current_revenue
        self.last_event_time = datetime.now()
        self.events_since_last_output = 0
    
    def get_stats(self):
        """
        Returns EventTracker statistics.
        
        Returns:
            dict: Event detection statistics
        """
        return {
            'last_window_count': self.last_window_count,
            'last_revenue': self.last_revenue,
            'events_since_last_output': self.events_since_last_output,
            'last_event_time': self.last_event_time
        }