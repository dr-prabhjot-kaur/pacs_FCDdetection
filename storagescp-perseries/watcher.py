
import argparse
import subprocess
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pydicom

def clean_text(string):
  # clean and standardize text descriptions, which makes searching files easier
  forbidden_symbols = ["*", ".", ",", "\"", "\\", "/", "|", "[", "]", ":", ";", " "]
  for symbol in forbidden_symbols:
    string = string.replace(symbol, "_") # replace symbols with underscore
  return string.lower()


class NewFileHandler(FileSystemEventHandler):

  def __init__(self, conv_dir): 
        self.conv_dir = conv_dir

  def on_any_event(self, event):
    if event.event_type == "closed":
      print(f"file closed event - {event}")
      if not event.is_directory:
        print(f"File closed for writing: {event.src_path}")
        # Add your custom logic here to process the new file
        dataset = pydicom.dcmread(event.src_path)
        # (0008,0016) SOP Class UID UI: Enhanced MR Image Storage
        print(dataset.SOPClassUID)
        if dataset.SOPClassUID == "1.2.840.10008.5.1.4.1.1.4.1":
          print("Found Enhanced DICOM file.")
          print(f"Convert file into {self.conv_dir}.")
          subprocess.run(["echo", "emf2sf", "--out-dir",
              self.conv_dir, event.src_path])
        else:
          print("Found regular DICOM file.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert multiframe DICOM files.')
    parser.add_argument("incomingDir")
    parser.add_argument("conversionDir")
    parser.add_argument("outgoingDir")
    args = parser.parse_args()

    # The actual directory to watch:
    path_to_watch = args.incomingDir
    if not os.path.isdir(path_to_watch):
        print(f"Directory is missing {incomingDir}")

    os.makedirs(args.conversionDir, exist_ok=True)
    os.makedirs(args.outgoingDir, exist_ok=True)

    event_handler = NewFileHandler(args.conversionDir)
    observer = Observer()
    # Set recursive=True to watch subdirectories
    observer.schedule(event_handler, path_to_watch, recursive=True) 
    observer.start()

    print(f"Watching for new files in {path_to_watch}...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
            observer.stop()
    observer.join()

