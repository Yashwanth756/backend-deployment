from collections import deque
from datetime import datetime, timedelta
# from flask import Flask, jsonify, request

class APIKey:
    def __init__(self, key,model, rpm, rpd):
        self.key = key
        self.model = model
        self.rpm = rpm
        self.rpd = rpd
        self.window = deque()
        self.daily_count = 0
        self.last_reset_day = datetime.now().date()

    def reset_if_needed(self):
        today = datetime.now().date()
        if self.last_reset_day != today:
            self.daily_count = 0
            self.window.clear()
            self.last_reset_day = today

    def cleanup_window(self):
        now = datetime.now()
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
        self.window.append(datetime.now())
        self.daily_count += 1


class APIKeyManager:
    def __init__(self, key_data):
        """
        key_data: list of tuples (apikey, rpm, rpd)
        """
        self.keys = [APIKey(key, model, int(rpm), int(rpd)) for key, model, rpm, rpd in key_data]

    def get_available_key(self):
        for key in self.keys:
            status = key.is_available()
            if status == "available":
                key.record_request()
                # print(key.model)
                return (key.key, key.model)  # return the actual API key

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
