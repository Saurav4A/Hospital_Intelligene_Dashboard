"""
Notification System API Routes
Add these routes to your Flask app (app.py or main.py)
"""

from flask import jsonify, request, session
from datetime import datetime, timedelta
import json
import os

# Get the directory where this file is located (modules folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to notifications data files - now using absolute paths
NOTIFICATIONS_FILE = os.path.join(BASE_DIR, 'notifications.json')
USER_NOTIFICATIONS_FILE = os.path.join(BASE_DIR, 'user_notifications.json')


def load_notifications():
    """Load notifications from JSON file"""
    if not os.path.exists(NOTIFICATIONS_FILE):
        return []
    try:
        with open(NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
            notifications = json.load(f)
    except Exception:
        return []
    
    # Filter out expired notifications (older than 7 days)
    active_notifications = []
    
    for notif in notifications:
        try:
            created_date = datetime.fromisoformat(notif['created_at'])
            now_ts = datetime.now(tz=created_date.tzinfo) if created_date.tzinfo else datetime.now()
            days_old = (now_ts - created_date).days
        except Exception:
            continue
        
        if days_old <= 7:  # Only show notifications from last 7 days
            active_notifications.append(notif)
    
    return active_notifications


def load_user_read_status(username):
    """Load which notifications a user has read"""
    if not os.path.exists(USER_NOTIFICATIONS_FILE):
        return {}
    
    with open(USER_NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
        all_user_data = json.load(f)
    
    return all_user_data.get(username, {})


def save_user_read_status(username, read_notifications):
    """Save which notifications a user has read"""
    all_user_data = {}
    
    if os.path.exists(USER_NOTIFICATIONS_FILE):
        with open(USER_NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
            all_user_data = json.load(f)
    
    all_user_data[username] = read_notifications
    
    with open(USER_NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_user_data, f, indent=2)


def register_notification_routes(app):
    """
    Register notification routes with the Flask app
    
    Usage in your main app:
        from modules.notification_routes import register_notification_routes
        register_notification_routes(app)
    """
    
    @app.route('/api/notifications')
    def get_notifications():
        """Get all active notifications for current user"""
        username = session.get('username') or session.get('user') or 'default_user'
        
        # Load all active notifications
        all_notifications = load_notifications()
        
        # Load user's read status
        user_read_notifications = load_user_read_status(username)
        
        # Add read status to each notification
        formatted_notifications = []
        unread_count = 0
        
        for notif in all_notifications:
            notif_id = str(notif['id'])
            target_user = notif.get("user")
            if target_user and str(target_user).lower() != str(username).lower():
                continue
            is_read = user_read_notifications.get(notif_id, False)
            
            # Format date for display
            try:
                created_date = datetime.fromisoformat(notif['created_at'])
                now_ts = datetime.now(tz=created_date.tzinfo) if created_date.tzinfo else datetime.now()
                days_ago = (now_ts - created_date).days
            except Exception:
                days_ago = 0
            
            if days_ago == 0:
                date_str = "Today"
            elif days_ago == 1:
                date_str = "Yesterday"
            else:
                date_str = f"{days_ago} days ago"
            
            formatted_notifications.append({
                'id': notif_id,
                'title': notif['title'],
                'message': notif['message'],
                'type': notif['type'],
                'date': date_str,
                'read': is_read,
                'link': notif.get('link', '')  # Add link field
            })
            
            if not is_read:
                unread_count += 1
        
        # Sort by date (newest first)
        formatted_notifications.sort(key=lambda x: x['id'], reverse=True)
        
        return jsonify({
            'notifications': formatted_notifications,
            'unreadCount': unread_count
        })


    @app.route('/api/notifications/<notification_id>/read', methods=['POST'])
    def mark_notification_read(notification_id):
        """Mark a specific notification as read"""
        username = session.get('username') or session.get('user') or 'default_user'
        
        user_read_notifications = load_user_read_status(username)
        user_read_notifications[notification_id] = True
        save_user_read_status(username, user_read_notifications)
        
        return jsonify({'success': True})


    @app.route('/api/notifications/mark-all-read', methods=['POST'])
    def mark_all_read():
        """Mark all notifications as read for current user"""
        username = session.get('username') or session.get('user') or 'default_user'
        
        all_notifications = load_notifications()
        user_read_notifications = {}
        
        for notif in all_notifications:
            user_read_notifications[str(notif['id'])] = True
        
        save_user_read_status(username, user_read_notifications)
        
        return jsonify({'success': True})

    @app.route('/api/notifications/clear', methods=['POST'])
    def clear_notifications():
        """Mark all notifications as read and remove user-specific ones."""
        username = session.get('username') or session.get('user') or 'default_user'
        # Mark all as read
        all_notifications = load_notifications()
        user_read_notifications = {}
        for notif in all_notifications:
            user_read_notifications[str(notif.get('id'))] = True
        save_user_read_status(username, user_read_notifications)

        # Also purge user-scoped notifications from the store for this user
        try:
            if os.path.exists(NOTIFICATIONS_FILE):
                with open(NOTIFICATIONS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                filtered = [n for n in data if str(n.get("user", "")).lower() != str(username).lower()]
                with open(NOTIFICATIONS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, indent=2)
        except Exception:
            pass

        return jsonify({'success': True})


    @app.route('/api/notifications/unread_count')
    def unread_count():
        """Lightweight unread count for badge polling."""
        username = session.get('username') or session.get('user') or 'default_user'
        all_notifications = load_notifications()
        user_read_notifications = load_user_read_status(username)
        unread = 0
        for notif in all_notifications:
            target_user = notif.get("user")
            if target_user and str(target_user).lower() != str(username).lower():
                continue
            notif_id = str(notif.get("id"))
            if not user_read_notifications.get(notif_id, False):
                unread += 1
        return jsonify({"unread": unread})
