import os
import sqlite3
from typing import List, Dict
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import schedule
import time
import flask
from flask import Flask, request, jsonify
import threading

class SlackArchiver:
    def __init__(self, slack_token: str, database_path: str):
        """
        Initialize the Slack Archiver with authentication and database connection
        
        :param slack_token: Slack API token with appropriate permissions
        :param database_path: Path to SQLite database for storing archived messages
        """
        if not slack_token:
            raise ValueError("No Slack token provided.")
        
        self.client = WebClient(token=slack_token)
        self.db_path = database_path
        self.setup_database()
    
    def setup_database(self):
        """
        Create database schema for storing Slack messages
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    channel_name TEXT,
                    last_archived_timestamp TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY,
                    channel_id TEXT,
                    timestamp TEXT,
                    user TEXT,
                    text TEXT,
                    FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
                )
            ''')
            conn.commit()
    
    def add_channel_to_archive(self, channel_id: str):
        """
        Add a channel to the list of channels to be archived
        
        :param channel_id: ID of the Slack channel
        """
        try:
            # Directly use the conversations.info method with the channel ID
            channel_info = self.client.conversations_info(channel=channel_id)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO channels 
                    (channel_id, channel_name, last_archived_timestamp) 
                    VALUES (?, ?, ?)
                ''', (
                    channel_id, 
                    channel_info['channel']['name'], 
                    None
                ))
                conn.commit()
            print(f"Successfully added channel: {channel_info['channel']['name']}")
        except SlackApiError as e:
            print(f"Error adding channel {channel_id}: {e}")
    
    def archive_channel_messages(self, channel_id: str):
        """
        Extract messages from a specific channel
        
        :param channel_id: ID of the Slack channel
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get last archived timestamp
            cursor.execute(
                'SELECT last_archived_timestamp FROM channels WHERE channel_id = ?', 
                (channel_id,)
            )
            result = cursor.fetchone()
            last_timestamp = result[0] if result else None
            
            # Retrieve messages (respecting 50 messages/minute rate limit)
            try:
                messages_result = self.client.conversations_history(
                    channel=channel_id,
                    oldest=last_timestamp or '0',
                    limit=50  # Rate limit compliance
                )
                
                for message in messages_result['messages']:
                    cursor.execute('''
                        INSERT OR IGNORE INTO messages 
                        (message_id, channel_id, timestamp, user, text) 
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        message['ts'],
                        channel_id,
                        message['ts'],
                        message.get('user', 'N/A'),
                        message.get('text', '')
                    ))
                
                # Update last archived timestamp
                if messages_result['messages']:
                    cursor.execute('''
                        UPDATE channels 
                        SET last_archived_timestamp = ? 
                        WHERE channel_id = ?
                    ''', (
                        messages_result['messages'][0]['ts'],
                        channel_id
                    ))
                
                conn.commit()
            except SlackApiError as e:
                print(f"Error archiving channel {channel_id}: {e}")
    
    def get_channel_messages(self, channel_id: str, limit: int = 100, offset: int = 0):
        """
        Retrieve messages for a specific channel from the archive
        
        :param channel_id: ID of the Slack channel
        :param limit: Maximum number of messages to retrieve
        :param offset: Starting point for message retrieval
        :return: List of messages
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT message_id, timestamp, user, text 
                FROM messages 
                WHERE channel_id = ? 
                ORDER BY timestamp 
                LIMIT ? OFFSET ?
            ''', (channel_id, limit, offset))
            
            messages = cursor.fetchall()
            return [
                {
                    'message_id': msg[0],
                    'timestamp': msg[1],
                    'user': msg[2],
                    'text': msg[3]
                } for msg in messages
            ]
    
    def export_channel_to_file(self, channel_id: str, export_path: str):
        """
        Export archived messages for a channel to a file
        
        :param channel_id: ID of the Slack channel
        :param export_path: Path to export the messages
        """
        messages = self.get_channel_messages(channel_id, limit=None)
        
        with open(export_path, 'w', encoding='utf-8') as f:
            for message in messages:
                f.write(f"{message}\n")
    
    def schedule_archiving(self):
        """
        Schedule periodic archiving for all registered channels
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT channel_id FROM channels')
            channels = cursor.fetchall()
        
        for (channel_id,) in channels:
            self.archive_channel_messages(channel_id)
        
        # Wait to respect rate limits
        time.sleep(60)  # 1 minute between batch jobs

def create_app(archiver):
    """
    Create Flask application with API endpoints
    
    :param archiver: SlackArchiver instance
    :return: Flask app
    """
    app = Flask(__name__)
    
    @app.route('/channels/<channel_id>/messages', methods=['GET'])
    def get_messages(channel_id):
        """
        API endpoint to retrieve messages for a specific channel and store in a file.
        """
        limit = request.args.get('limit', default=100, type=int)
        offset = request.args.get('offset', default=0, type=int)

        try:
            messages = archiver.get_channel_messages(channel_id, limit, offset)
            
            # Save the result to a file
            export_path = f"{channel_id}_messages.json"
            archiver.export_channel_to_file(channel_id, export_path)
            
            return jsonify({
                'message': f'Messages have been exported to {export_path}',
                'total_count': len(messages)
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    return app

def main():
    #I have to remove this to upload to github
    slack_token = 'YOUR_TOKEN_HERE'
    
    archiver = SlackArchiver(
        slack_token=slack_token,
        database_path='slack_archive.db'
    )
    
    # Use the FULL channel ID from the Slack URL
    archiver.add_channel_to_archive('C0842M4F0AW')
    
    # Create Flask app
    app = create_app(archiver)
    
    # Start archiving in a separate thread
    archiving_thread = threading.Thread(target=lambda: schedule.every(1).hour.do(archiver.schedule_archiving))
    archiving_thread.start()
    
    # Run Flask app
    app.run(debug=True, port=5000)

# Call main() when the script is run directly
if __name__ == '__main__':
    main()