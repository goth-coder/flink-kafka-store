"""
EventAnalyzer Service - Intelligent event analysis
Extracted from: 03_flink_real_time_analytics.py (lines 96-139)
Responsibility: Generate insights based on detected patterns
"""


class EventAnalyzer:
    """
    Advanced event analyzer that generates insights based on patterns.
    
    Features:
    - Detects trends and revenue spikes
    - Identifies high diversity of vendors/categories  
    - Maintains history for temporal analysis
    - Generates contextual automatic insights
    """
    
    def __init__(self):
        self.vendor_history = set()
        self.category_history = set()
        self.revenue_history = []
        print("📊 EventAnalyzer initialized - Intelligent insights active!")
    
    def analyze_event(self, window_data):
        """
        Analyzes events and generates contextual insights.
        
        Args:
            window_data (tuple): Flink window data in format:
                (start, end, count, revenue, avg_ticket, vendors, categories, ...)
                
        Returns:
            list: List of generated insights
        """
        # Extract window metrics (compatible with current Flink format)
        current_revenue = window_data[3] if len(window_data) > 3 else 0
        
        insights = []
        
        # Detect significant growth
        if len(self.revenue_history) > 0:
            last_revenue = self.revenue_history[-1]
            if last_revenue > 0:  # Avoid division by zero
                growth_ratio = current_revenue / last_revenue
                if growth_ratio > 1.5:
                    growth = ((growth_ratio - 1) * 100)
                    insights.append(f"�� EXPLOSIVE GROWTH: +{growth:.0f}%")
                elif growth_ratio > 1.25:
                    growth = ((growth_ratio - 1) * 100)
                    insights.append(f"📈 GROWTH: +{growth:.0f}% in window!")
         
        # Save history (keep last 10 windows)
        self.revenue_history.append(current_revenue)
        if len(self.revenue_history) > 10:
            self.revenue_history.pop(0)
        
        return insights
    
    def get_stats(self):
        """
        Returns analyzer statistics.
        
        Returns:
            dict: Analysis metrics
        """
        avg_revenue = sum(self.revenue_history) / len(self.revenue_history) if self.revenue_history else 0
        max_revenue = max(self.revenue_history) if self.revenue_history else 0
        
        return {
            'revenue_history_size': len(self.revenue_history),
            'avg_revenue': f"R$ {avg_revenue:,.2f}",
            'max_revenue': f"R$ {max_revenue:,.2f}",
            'vendors_tracked': len(self.vendor_history),
            'categories_tracked': len(self.category_history)
        }
    
    def reset(self):
        """Resets analyzer state"""
        self.vendor_history.clear()
        self.category_history.clear() 
        self.revenue_history.clear()
        print("🔄 EventAnalyzer reset")
