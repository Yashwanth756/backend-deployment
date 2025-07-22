from datetime import datetime, timedelta
from pymongo import MongoClient
from collections import deque


class APIKeyManager:
    def __init__(self, mongo_url="mongodb+srv://root:root@cluster0.jt307.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", db_name="school", collection_name="apikeys"):
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def reset_if_needed(self, doc):
        today = datetime.now().date()
        if doc.get("last_reset_day") != str(today):
            doc["daily_count"] = 0
            doc["window"] = []
            doc["last_reset_day"] = str(today)
        return doc

    def cleanup_window(self, doc):
        now = datetime.now()
        window = deque(datetime.fromisoformat(ts) for ts in doc.get("window", []))
        while window and (now - window[0]) > timedelta(minutes=1):
            window.popleft()
        return window

    def is_available(self, doc):
        doc = self.reset_if_needed(doc)
        window = self.cleanup_window(doc)

        if doc["daily_count"] >= doc["rpd"]:
            return "rpd_exceeded", doc, window
        elif len(window) >= doc["rpm"]:
            return "rpm_exceeded", doc, window
        return "available", doc, window

    def record_request(self, doc, window):
        now = datetime.now()
        window.append(now)
        doc["daily_count"] += 1
        doc["window"] = [dt.isoformat() for dt in window]
        doc["last_reset_day"] = str(datetime.now().date())
        self.collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "daily_count": doc["daily_count"],
                    "window": doc["window"],
                    "last_reset_day": doc["last_reset_day"]
                }
            }
        )

    def get_available_key(self):
        all_docs = list(self.collection.find({}))
        for doc in all_docs:
            status, updated_doc, cleaned_window = self.is_available(doc)
            if status == "available":
                self.record_request(updated_doc, cleaned_window)
                return {
                    "apiKey": updated_doc["key"],
                    "model": updated_doc["model"]
                }

        if all(self.is_available(doc)[0] == "rpd_exceeded" for doc in all_docs):
            return {"apiKey": "", "message": "Try again tomorrow"}
        elif all(self.is_available(doc)[0] != "available" and self.is_available(doc)[0] != "rpd_exceeded" for doc in all_docs):
            return {"apiKey": "", "message": "Please wait 1 minute"}

        return {"apiKey": "", "message": "Unknown error"}
