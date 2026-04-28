
The SCP receives DICOMs from PACS over the network. As each file arrives (handle_store), it saves it to in/<seriesUID>/<sopUID>. When the network association finishes (handle_released/handle_aborted), the SCP reads MRN/DOS/StudyInstanceUID from the received series and moves them into study_pending/<MRN>/<DOS>/<StudyUID>/, recording the first-arrival timestamp.
A separate watcher thread, running every 60s, then checks for studies that have been quiet for the timeout window and promotes them to staging/, writing the queue/<study_key>.ready marker — that's the signal that triggers the submitter.




