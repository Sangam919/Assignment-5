import schedule
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from data_extractor import DataExtractor

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, extractor):
        self.extractor = extractor
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('company.db'):
            print(f"📥 Database modified: {event.src_path}")
            self.extractor.extract_all_tables()

def run_scheduled_extraction():
    extractor = DataExtractor()
    print("⏰ Starting scheduled extraction...")
    extractor.extract_all_tables()

def main():
    extractor = DataExtractor()
    
    # Schedule-based trigger (every 5 minutes)
    schedule.every(5).minutes.do(run_scheduled_extraction)
    
    # Event-based trigger (database file changes)
    observer = Observer()
    event_handler = FileEventHandler(extractor)
    observer.schedule(event_handler, path='.', recursive=False)
    observer.start()
    
    print("🕒 Scheduler running (Ctrl+C to stop)...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
