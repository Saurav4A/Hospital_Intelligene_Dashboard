"""
Simple Notification Manager
Run this script to easily add new notifications

Usage:
  python manage_notifications.py
"""

import json
from datetime import datetime
import os

NOTIFICATIONS_FILE = 'notifications.json'


def load_notifications():
    """Load existing notifications"""
    if not os.path.exists(NOTIFICATIONS_FILE):
        return []
    
    try:
        with open(NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return []
            return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"\n⚠️  Warning: notifications.json has invalid JSON syntax!")
        print(f"   Error: {e}")
        
        # Ask user if they want to reset the file
        reset = input("\n   Would you like to reset the file? (y/n): ").strip().lower()
        if reset == 'y':
            with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump([], f)
            print("   ✅ File reset successfully!")
            return []
        else:
            print("   ❌ Please fix the JSON file manually and try again.")
            exit(1)
    except Exception as e:
        print(f"\n⚠️  Error reading notifications file: {e}")
        return []


def save_notifications(notifications):
    """Save notifications to file"""
    with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(notifications, f, indent=2, ensure_ascii=False)


def get_next_id(notifications):
    """Get next available ID"""
    if not notifications:
        return 1
    return max(notif['id'] for notif in notifications) + 1


def add_notification():
    """Add a new notification"""
    notifications = load_notifications()
    
    print("\n" + "="*60)
    print("📢 ADD NEW NOTIFICATION")
    print("="*60)
    
    # Get notification details
    title = input("\n📌 Title (e.g., 'New Feature Added'): ").strip()
    if not title:
        print("❌ Title is required!")
        return
    
    message = input("\n💬 Message (details about the change): ").strip()
    if not message:
        print("❌ Message is required!")
        return
    
    link = input("\n🔗 Link (optional - page to redirect to, e.g., /volume): ").strip()
    
    print("\n🏷️  Type of notification:")
    print("  1. feature     - New feature added")
    print("  2. update      - Improvement or update")
    print("  3. maintenance - System maintenance")
    print("  4. announcement- General announcement")
    
    type_choice = input("\nSelect type (1-4): ").strip()
    type_map = {
        '1': 'feature',
        '2': 'update',
        '3': 'maintenance',
        '4': 'announcement'
    }
    
    notif_type = type_map.get(type_choice, 'announcement')
    
    # Add emoji based on type
    emoji_map = {
        'feature': '🎉',
        'update': '📊',
        'maintenance': '🔧',
        'announcement': '📢'
    }
    
    if not title.startswith(emoji_map[notif_type]):
        title = f"{emoji_map[notif_type]} {title}"
    
    # Create new notification
    new_notification = {
        'id': get_next_id(notifications),
        'title': title,
        'message': message,
        'type': notif_type,
        'link': link,
        'created_at': datetime.now().isoformat()
    }
    
    # Add to list and save
    notifications.insert(0, new_notification)  # Add at beginning
    save_notifications(notifications)
    
    print("\n" + "="*60)
    print("✅ Notification added successfully!")
    print("="*60)
    print(f"\n📋 Preview:")
    print(f"   ID: {new_notification['id']}")
    print(f"   Title: {new_notification['title']}")
    print(f"   Message: {new_notification['message']}")
    print(f"   Type: {new_notification['type']}")
    print(f"   Created: {new_notification['created_at']}")
    print("\n⏰ This notification will be visible for 7 days.")
    print("="*60 + "\n")


def view_notifications():
    """View all current notifications"""
    notifications = load_notifications()
    
    if not notifications:
        print("\n❌ No notifications found!\n")
        return
    
    print("\n" + "="*60)
    print("📋 CURRENT NOTIFICATIONS")
    print("="*60)
    
    current_time = datetime.now()
    
    for notif in notifications:
        created_date = datetime.fromisoformat(notif['created_at'])
        days_old = (current_time - created_date).days
        status = "✅ ACTIVE" if days_old <= 7 else "❌ EXPIRED"
        
        print(f"\n🆔 ID: {notif['id']} | {status} ({days_old} days old)")
        print(f"   📌 {notif['title']}")
        print(f"   💬 {notif['message']}")
        print(f"   🏷️  Type: {notif['type']}")
        if notif.get('link'):
            print(f"   🔗 Link: {notif['link']}")
        print(f"   📅 Created: {notif['created_at']}")
    
    print("\n" + "="*60 + "\n")


def delete_notification():
    """Delete a notification by ID"""
    notifications = load_notifications()
    
    if not notifications:
        print("\n❌ No notifications to delete!\n")
        return
    
    view_notifications()
    
    try:
        notif_id = int(input("Enter notification ID to delete (0 to cancel): ").strip())
        
        if notif_id == 0:
            print("❌ Cancelled\n")
            return
        
        # Find and remove notification
        notifications = [n for n in notifications if n['id'] != notif_id]
        save_notifications(notifications)
        
        print(f"\n✅ Notification #{notif_id} deleted successfully!\n")
        
    except ValueError:
        print("❌ Invalid ID!\n")


def clean_expired():
    """Remove notifications older than 7 days"""
    notifications = load_notifications()
    current_time = datetime.now()
    
    active_notifications = []
    expired_count = 0
    
    for notif in notifications:
        created_date = datetime.fromisoformat(notif['created_at'])
        days_old = (current_time - created_date).days
        
        if days_old <= 7:
            active_notifications.append(notif)
        else:
            expired_count += 1
    
    if expired_count > 0:
        save_notifications(active_notifications)
        print(f"\n✅ Removed {expired_count} expired notification(s)!\n")
    else:
        print("\n✅ No expired notifications to clean.\n")


def validate_json():
    """Validate and fix the notifications.json file"""
    print("\n" + "="*60)
    print("🔍 VALIDATING notifications.json")
    print("="*60)
    
    if not os.path.exists(NOTIFICATIONS_FILE):
        print("\n📝 Creating new notifications.json file...")
        with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=2)
        print("✅ File created successfully!\n")
        return
    
    try:
        with open(NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                print("\n⚠️  File is empty. Initializing...")
                with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
                    json.dump([], f, indent=2)
                print("✅ File initialized successfully!\n")
                return
            
            notifications = json.loads(content)
            print(f"\n✅ JSON is valid! Found {len(notifications)} notification(s).\n")
            
    except json.JSONDecodeError as e:
        print(f"\n❌ Invalid JSON detected!")
        print(f"   Error: {e}")
        print(f"   Location: Line {e.lineno}, Column {e.colno}")
        
        reset = input("\n   Reset file to empty array? (y/n): ").strip().lower()
        if reset == 'y':
            with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump([], f, indent=2)
            print("   ✅ File reset successfully!\n")
        else:
            print("   ⚠️  Please fix the file manually.\n")


def main():
    """Main menu"""
    while True:
        print("\n" + "="*60)
        print("📢 NOTIFICATION MANAGER - Asarfi Hospital Dashboard")
        print("="*60)
        print("\n1. 📝 Add New Notification")
        print("2. 👀 View All Notifications")
        print("3. 🗑️  Delete Notification")
        print("4. 🧹 Clean Expired Notifications")
        print("5. 🔍 Validate/Fix JSON File")
        print("6. 🚪 Exit")
        
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == '1':
            add_notification()
        elif choice == '2':
            view_notifications()
        elif choice == '3':
            delete_notification()
        elif choice == '4':
            clean_expired()
        elif choice == '5':
            validate_json()
        elif choice == '6':
            print("\n👋 Goodbye!\n")
            break
        else:
            print("\n❌ Invalid option! Please select 1-6.\n")


if __name__ == '__main__':
    main()