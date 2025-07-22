from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import deque
import pytz

# Mongo setup
client = MongoClient("mongodb+srv://root:root@cluster0.jt307.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["school"]
collection = db["apikeys"]

IST = pytz.timezone("Asia/Kolkata")  # You can change this timezone as needed


class APIKey:
    def __init__(self, key, model, rpm, rpd, request_times=None, daily_count=0, last_reset_day=None):
        self.key = key
        self.model = model
        self.rpm = rpm
        self.rpd = rpd
        self.window = deque()
        self.daily_count = daily_count
        self.last_reset_day = last_reset_day or datetime.now(IST).date()

        # Restore request timestamps within 1 minute
        now = datetime.now(IST)
        if request_times:
            for ts in request_times:
                dt = datetime.fromisoformat(ts)
                if (now - dt) <= timedelta(minutes=1):
                    self.window.append(dt)

    def reset_if_needed(self):
        today = datetime.now(IST).date()
        if self.last_reset_day != today:
            self.daily_count = 0
            self.window.clear()
            self.last_reset_day = today

    def cleanup_window(self):
        now = datetime.now(IST)
        while self.window and (now - self.window[0]) > timedelta(minutes=1):
            self.window.popleft()

    def is_available(self):
        self.reset_if_needed()
        self.cleanup_window()
        if self.daily_count >= self.rpd:
            return "rpd_exceeded"
        elif len(self.window) >= self.rpm:
            return "rpm_exceeded"
        return "available"

    def record_request(self):
        now = datetime.now(IST)
        self.window.append(now)
        self.daily_count += 1
        self.save_to_db()

    def save_to_db(self):
        collection.update_one(
            {"key": self.key},
            {
                "$set": {
                    "key": self.key,
                    "model": self.model,
                    "rpm": self.rpm,
                    "rpd": self.rpd,
                    "request_times": [t.isoformat() for t in self.window],
                    "daily_count": self.daily_count,
                    "last_reset_day": self.last_reset_day.isoformat()
                }
            },
            upsert=True
        )


class APIKeyManager:
    def __init__(self, key_data):
        """
        key_data: list of tuples (apikey, model, rpm, rpd)
        """
        self.keys = []

        for key, model, rpm, rpd in key_data:
            doc = collection.find_one({"key": key})
            if doc:
                api_key = APIKey(
                    key=key,
                    model=model,
                    rpm=int(rpm),
                    rpd=int(rpd),
                    request_times=doc.get("request_times", []),
                    daily_count=doc.get("daily_count", 0),
                    last_reset_day=datetime.fromisoformat(doc.get("last_reset_day")).date()
                )
            else:
                api_key = APIKey(key, model, int(rpm), int(rpd))

            self.keys.append(api_key)

    def get_available_key(self):
        for key in self.keys:
            status = key.is_available()
            if status == "available":
                key.record_request()
                return (key.key, key.model)

        if all(k.is_available() == "rpd_exceeded" for k in self.keys):
            return ('', "Try again tomorrow")
        elif all(k.is_available() != "available" and k.is_available() != "rpd_exceeded" for k in self.keys):
            return ('', "Please wait 1 minute")

# # 🧪 Example usage
# if __name__ == "__main__":
#     # Simulated input: "apikey, rpm, rpd"
#     input_data = """
#     KEY1, 30, 50
#     KEY2, 5, 20
#     KEY3, 15, 100
#     """.strip().splitlines()

#     key_data = [tuple(item.strip() for item in line.split(",")) for line in input_data]
#     print("Parsed key data:", key_data)
#     manager = APIKeyManager(key_data)

#     # Simulate incoming requests
#     import time
#     for i in range(40):
#         result = manager.get_available_key()
#         print(f"Request {i+1}: Assigned to ->", result)
#         time.sleep(0.3)
