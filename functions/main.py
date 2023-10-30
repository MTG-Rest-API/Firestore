# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`


from firebase_functions import firestore_fn, https_fn, options, scheduler_fn
from firebase_admin import initialize_app, firestore
import google.cloud.firestore
from datetime import datetime

app = initialize_app()
options.set_global_options(max_instances=10)

@scheduler_fn.on_schedule(schedule="*/5 * * * *")
def scheduled(event: scheduler_fn.ScheduledEvent):
    hour, minute, second = str((datetime.now().time())).split(":")
    firestore_client = firestore.client()
    firestore_client.collection("dates").add({
        "hour": hour, "minute": minute, "second": second.split(".")[0]
    }) 
