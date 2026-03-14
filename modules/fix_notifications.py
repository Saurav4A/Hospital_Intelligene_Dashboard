"""
Quick Fix for notifications.json
Run this if you get JSON errors
"""

import json

# Create a fresh notifications.json with sample data
sample_notifications = [
    {
        "id": 1,
        "title": "🎉 New Business Insights Dashboard",
        "message": "We've added a new Business Insights panel showing daily average billing split by Cash/Cashless across all visit types. Check it out from the main dashboard!",
        "type": "feature",
        "link": "/business_insights",
        "created_at": "2025-01-13T09:00:00"
    },
    {
        "id": 2,
        "title": "📊 Volume Tracker Improvements",
        "message": "The Volume Tracker now features better chart layouts with improved label positioning and space utilization for easier data viewing.",
        "type": "update",
        "link": "/volume",
        "created_at": "2025-01-12T14:30:00"
    },
    {
        "id": 3,
        "title": "🔔 Welcome to Notifications!",
        "message": "Stay updated with the latest features and improvements. You'll see notifications here for 7 days after they're posted.",
        "type": "announcement",
        "link": "",
        "created_at": "2025-01-13T10:00:00"
    }
]

try:
    with open('notifications.json', 'w', encoding='utf-8') as f:
        json.dump(sample_notifications, f, indent=2, ensure_ascii=False)
    
    print("✅ notifications.json created successfully!")
    print(f"✅ Added {len(sample_notifications)} sample notifications")
    print("\nYou can now run: python manage_notifications.py")
    
except Exception as e:
    print(f"❌ Error: {e}")