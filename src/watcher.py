import time
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from analyze_costs import CloudBillHunter

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [WATCHER] - %(message)s')

LANDING_ZONE = "data/landing_zone"

class BillingFileHandler(FileSystemEventHandler):
    """
    The 'Trigger': Reacts whenever a file is created in the folder.
    """
    def on_created(self, event):
        if event.is_directory:
            return
        
        filename = event.src_path
        if filename.endswith(".csv"):
            logging.info(f"⚡ Detected new file drop: {filename}")
            
            # --- TRIGGER THE PIPELINE ---
            engine = None
            try:
                # Initialize the Engine
                engine = CloudBillHunter()
                
                # 1. Medallion: Bronze (Ingest)
                engine.ingest_data(filename)
                
                # 2. Medallion: Silver & Gold (Transform)
                engine.run_pipeline()
                
                logging.info(f"✅ Pipeline complete for {filename}")
                
            except Exception as e:
                logging.error(f"❌ Pipeline failed: {str(e)}")
            finally:
                # CRITICAL: Manually close the connection now
                if engine:
                    engine.close()

def start_watcher():
    # Ensure the folder exists
    if not os.path.exists(LANDING_ZONE):
        os.makedirs(LANDING_ZONE)

    event_handler = BillingFileHandler()
    observer = Observer()
    observer.schedule(event_handler, LANDING_ZONE, recursive=False)
    
    logging.info(f"👀 Watching directory: {LANDING_ZONE} for new bills...")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()