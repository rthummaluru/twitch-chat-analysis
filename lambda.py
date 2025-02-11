import json
import boto3
import os
import requests
from collections import Counter

# Environment variables for Twitch API
TWITCH_CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
TWITCH_AUTH_TOKEN = os.environ["TWITCH_AUTH_TOKEN"]
BROADCASTER_ID = os.environ["BROADCASTER_ID"]
KEYWORDS = ["LOL", "OMG", "WOW", "hype", "W"]  # Add more keywords as needed
CHAT_THRESHOLD = 20  # Number of keyword occurrences to trigger a clip

def create_twitch_clip():
    """Call Twitch API to create a clip."""
    url = "https://api.twitch.tv/helix/clips"
    headers = {
        "Authorization": f"Bearer {TWITCH_AUTH_TOKEN}",
        "Client-Id": TWITCH_CLIENT_ID,
    }
    params = {"broadcaster_id": BROADCASTER_ID, "has_delay": False}
    response = requests.post(url, headers=headers, params=params)
    
    if response.status_code == 200:
        clip_data = response.json()
        clip_id = clip_data["data"][0]["id"]
        print(f"Clip created successfully: {clip_id}")
        return clip_id
    else:
        print(f"Error creating clip: {response.json()}")
        return None

def process_chat_records(event):
    """Process chat records from Kinesis and detect clippable moments."""
    chat_messages = []
    
    # Extract messages from Kinesis records
    for record in event["Records"]:
        payload = json.loads(record["kinesis"]["data"])
        chat_messages.append(payload["message"])

    # Analyze chat messages for keyword spikes
    keyword_counts = Counter()
    for message in chat_messages:
        for keyword in KEYWORDS:
            if keyword.lower() in message.lower():
                keyword_counts[keyword] += 1

    # Check if any keyword exceeds the threshold
    total_keyword_hits = sum(keyword_counts.values())
    if total_keyword_hits >= CHAT_THRESHOLD:
        print(f"Keyword spike detected: {keyword_counts}")
        create_twitch_clip()
    else:
        print(f"No spike detected. Keyword counts: {keyword_counts}")

def lambda_handler(event, context):
    """AWS Lambda entry point."""
    try:
        process_chat_records(event)
        return {"statusCode": 200, "body": "Success"}
    except Exception as e:
        print(f"Error processing chat data: {e}")
        return {"statusCode": 500, "body": str(e)}
