import socket
import requests
import os
import boto3
import json
from flask import Flask, request, redirect
from dotenv import load_dotenv

load_dotenv()

# Load environment variables for client_id and client_secret
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION") 
KINESIS_STREAM_NAME = "twitch-chat-stream"
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

app = Flask(__name__)
user_access_token = None
# Step 1: Redirect User to Twitch Authorization Page
@app.route("/login")
def login():
    auth_url = (
        f"https://id.twitch.tv/oauth2/authorize?"
        f"client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=chat:read"
    )
    return redirect(auth_url)

# Step 2: Handle Callback and Get User Access Token
@app.route("/callback")
def callback():
    global user_access_token
    code = request.args.get("code")
    token_url = "https://id.twitch.tv/oauth2/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    }
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    user_access_token = response.json().get("access_token")
    print(f"User Access Token: {user_access_token}")
    update_lambda_env(user_access_token)
    return ('welcome to chat app. go to connect route to connect to chat')

# Step 3: Connect to Twitch IRC and Fetch Chat Messages
@app.route("/connect")
def connect_to_chat():
    global user_access_token
    if not user_access_token:
        return "Error: No User Access Token available. Authenticate first by visiting /login."

    SERVER = "irc.chat.twitch.tv"
    PORT = 6667
    NICKNAME = "your_twitch_username"  # Replace with your Twitch username
    TOKEN = f"oauth:{user_access_token}"  # The User Access Token
    CHANNEL = "#Jynxzi"  # Replace with the streamer's channel name

    try:
        # Create a socket and connect to Twitch IRC
        sock = socket.socket()
        sock.connect((SERVER, PORT))
        sock.send(f"PASS {TOKEN}\n".encode("utf-8"))
        sock.send(f"NICK {NICKNAME}\n".encode("utf-8"))
        sock.send(f"JOIN {CHANNEL}\n".encode("utf-8"))

        print(f"Connected to {CHANNEL}'s chat!\n")

        # Keep reading chat messages
        while True:
            response = sock.recv(2048).decode("utf-8")
            if response.startswith("PING"):
                sock.send("PONG\n".encode("utf-8"))  # Respond to PING to stay connected
            else:
                # Process raw IRC response
                process_chat_response(response, CHANNEL)

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}"
    finally:
        sock.close()
        return f"Disconnected from {CHANNEL}'s chat."


def process_chat_response(response, channel):
    """
    Process raw Twitch IRC responses, clean them, and send valid messages to Kinesis.
    """
    lines = response.split("\r\n")  # Split response into individual lines
    for line in lines:
        if "PRIVMSG" in line:
            # Parse the chat message
            parts = line.split(":", 2)
            if len(parts) > 2:
                username = parts[1].split("!")[0]  # Extract the username
                if 'bot' in username.lower():
                    continue
                message = parts[2].strip()  # Extract the chat message
                data = {
                    "username": username,
                    "message": message,
                    "channel": channel.strip("#"),
                }
                print(f"Processed Message: {data}")
                send_to_kinesis(data)
        else:
            # Log non-chat messages for debugging
            if line.strip():
                print(f"Ignored non-chat message: {line}")


def send_to_kinesis(data):
    """
    Send cleaned and structured data to Kinesis.
    """
    try:
        kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(data),  # Convert the data to JSON
            PartitionKey=data["username"],  # Partition by username
        )
        print(f"Successfully sent to Kinesis: {data}")
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")

    
def update_lambda_env(oauth_token):
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    response = lambda_client.update_function_configuration(
        FunctionName="TwitchChatProcessor",
        Environment={
            "Variables": {
                "TWITCH_AUTH_TOKEN": oauth_token,
                "TWITCH_CLIENT_ID": CLIENT_ID,
                "TWITCH_CLIENT_SECRET": CLIENT_SECRET,
                "BROADCASTER_ID": "Jynxzi"
            }
        }
    )
    print("Lambda environment variable updated")


if __name__ == "__main__":
    app.run(host="localhost", port=3000, debug=True)