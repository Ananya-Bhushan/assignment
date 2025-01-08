import time
from datetime import datetime

user_data = {}
topic_store = {}
message_queue = []

def create_user(name, privilege):
    if name in user_data:
        print(f"User {name} already exists.")
        return
    if privilege not in ["ADMIN", "USER"]:
        print("Invalid role. Role must be ADMIN or USER.")
        return
    user_data[name] = {"privilege": privilege, "subscribed_topics": []}
    print(f"User {name} added successfully.")

def register_topic(topic, admin):
    if admin not in user_data or user_data[admin]["privilege"] != "ADMIN":
        print("Only an ADMIN can register topics.")
        return
    if topic in topic_store:
        print(f"Topic {topic} is already registered.")
        return
    topic_store[topic] = {"followers": []}
    print(f"Topic {topic} registered successfully.")

def follow_topic(topic, user):
    if user not in user_data:
        print(f"User {user} does not exist.")
        return
    if topic not in topic_store:
        print(f"Topic {topic} does not exist.")
        return
    if user in topic_store[topic]["followers"]:
        print(f"User {user} is already following {topic}.")
        return
    topic_store[topic]["followers"].append(user)
    user_data[user]["subscribed_topics"].append(topic)
    print(f"User {user} is now following {topic}.")

def send_message(payload):
    try:
        message_id = payload["id"]
        target_topic = payload["topic"]
        content = payload["message"]
        scheduled_time = payload.get("timestamp")

        if target_topic not in topic_store:
            print(f"Target topic {target_topic} not found.")
            return

        for msg in message_queue:
            if msg["id"] == message_id:
                print("Duplicate message ID detected. Message not sent.")
                return

        message_queue.append({
            "id": message_id,
            "topic": target_topic,
            "message": content,
            "timestamp": scheduled_time,
        })
        print("Message sent successfully.")
    except KeyError:
        print("Invalid message format. Ensure all fields are present.")

def handle_messages():
    print("Processing queued messages...")
    for msg in message_queue[:]:  
        if msg.get("timestamp") and datetime.now() < datetime.fromtimestamp(msg["timestamp"]):
            continue

        topic = msg["topic"]
        message_content = msg["message"]
        if topic in topic_store:
            for follower in topic_store[topic]["followers"]:
                print({
                    "topic": topic,
                    "message": message_content,
                    "sent_to": follower,
                })
        message_queue.remove(msg)
    print("Message queue processed.")

def show_followed_topics(user):
    if user not in user_data:
        print(f"User {user} not found.")
        return
    print(f"{user} follows: {user_data[user]['subscribed_topics']}")

def notification_service():
    print("Welcome to the Notification System!")
    while True:
        command = input("Enter command: ").strip()
        if command.startswith("createUser"):
            _, name, privilege = command.split()
            create_user(name, privilege)
        elif command.startswith("registerTopic"):
            _, topic, admin = command.split()
            register_topic(topic, admin)
        elif command.startswith("followTopic"):
            _, topic, user = command.split()
            follow_topic(topic, user)
        elif command.startswith("sendMessage"):
            _, message_payload = command.split(" ", 1)
            import json
            payload = json.loads(message_payload)
            send_message(payload)
        elif command.startswith("processQueue"):
            handle_messages()
        elif command.startswith("showTopics"):
            _, user = command.split()
            show_followed_topics(user)
        elif command == "quit":
            print("Exiting the Notification System.")
            break
        else:
            print("Unknown command. Try again.")

if __name__ == "__main__":
    notification_service()
