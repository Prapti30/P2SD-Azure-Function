import azure.functions as func
import datetime
import json
import logging
import os
import requests
from dotenv import load_dotenv
import time
import threading
from azure.eventhub import EventHubConsumerClient
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from databricks_llm import DatabricksLLM

# ---------------- Load .env ----------------
load_dotenv()
# ---------------- Environment Variables ----------------
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "sql/1.0/warehouses/3dd12f4b21432cb2")
DATABRICKS_QUERY = "SELECT * FROM databricks_dev.default.anamolydetection"  # Replace with your table/query

app = func.FunctionApp()

# ---------------- Databricks HTTP Function ----------------
@app.route(route="GetDatabricksData", auth_level=func.AuthLevel.ANONYMOUS)
def GetDatabricksData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Databricks HTTP trigger function processed a request.")

    try:
        if not all([DATABRICKS_TOKEN, DATABRICKS_HOST, "3dd12f4b21432cb2"]):
            raise Exception("Missing Databricks credentials in environment variables.")

        # Ensure host starts with https
        host = DATABRICKS_HOST.strip().rstrip('/')
        if not host.startswith("http"):
            host = f"https://{host}"

        url = f"{host}/api/2.0/sql/statements"
        payload = {
            "statement": DATABRICKS_QUERY,
            "warehouse_id": "3dd12f4b21432cb2"
        }

        logging.info(f"Databricks request URL: {url}")
        logging.info(f"Databricks payload: {json.dumps(payload)}")
        logging.info(f"Databricks token (first 10 chars): {DATABRICKS_TOKEN[:10]}...")

        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=30
        )

        # Log response for debugging
        logging.info(f"Databricks response status: {response.status_code}")
        logging.info(f"Databricks response body: {response.text}")

        response.raise_for_status()
        data = response.json()

        return func.HttpResponse(json.dumps(data), mimetype="application/json")

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error fetching Databricks data: {e}")
        return func.HttpResponse(f"Error fetching Databricks data: {e}", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse(f"Unexpected error: {e}", status_code=500)

# ---------------- ADX: Event Hub -> HTTP Pull (test) ----------------
@app.route(route="GetADXData", auth_level=func.AuthLevel.ANONYMOUS)
def GetADXData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ADX HTTP pull trigger processed a request.")

    # Read Event Hub connection string from env
    eh_conn = os.getenv("EH_CONN_STR")
    if not eh_conn:
        return func.HttpResponse("EH_CONN_STR not configured in environment.", status_code=500)

    # Parameters
    max_events = int(os.getenv("EH_MAX_EVENTS", "50"))
    listen_seconds = int(os.getenv("EH_LISTEN_SECONDS", "5"))

    events = []

    def on_event(partition_context, event):
        try:
            body = None
            try:
                body = event.body_as_str(encoding='UTF-8')
            except Exception:
                body = str(event.body)

            events.append({
                "partition": partition_context.partition_id,
                "offset": event.offset,
                "sequence_number": event.sequence_number,
                "enqueued_time": event.enqueued_time.isoformat() if hasattr(event, 'enqueued_time') else None,
                "body": body
            })
            # stop early if we've collected enough
            if len(events) >= max_events:
                try:
                    client.close()
                except Exception:
                    pass
        except Exception as e:
            logging.exception("Error processing event: %s", e)

    # Use a consumer client to pull events for a short period
    client = EventHubConsumerClient.from_connection_string(eh_conn, consumer_group="$Default")

    def _run_receive():
        try:
            with client:
                # starting_position "-1" reads from the beginning; use "@latest" for newest
                client.receive(on_event=on_event, starting_position="@latest")
        except Exception:
            # receive will exit when client.close() is called or on error
            pass

    thread = threading.Thread(target=_run_receive, daemon=True)
    thread.start()

    # wait a short time to collect events
    time.sleep(listen_seconds)
    try:
        client.close()
    except Exception:
        pass
    thread.join(2)

    return func.HttpResponse(json.dumps({"events": events}), mimetype="application/json")

# ---------------- Chatbot Placeholder Function ----------------
@app.route(route="ChatbotADXFunction", auth_level=func.AuthLevel.ANONYMOUS)
def chat(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Chatbot request received.")

    try:
        body = req.get_json()
        prompt = body.get("prompt")
    except:
        prompt = None

    if not prompt:
        return func.HttpResponse(
            json.dumps({"error": "No prompt provided."}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        llm = DatabricksLLM()
        result = llm.generate_response(prompt)

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Chatbot error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

    

# ---------------- Email Alert Function ----------------

# ---------------- Email Alert Function ----------------
@app.route(route="send-email", methods=["POST"])
def send_email(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing email request...")

    # ----------------------------
    # 1) TRY JSON FIRST
    # ----------------------------
    try:
        req_body = req.get_json()
    except:
        req_body = None

    # ----------------------------
    # 2) IF NOT JSON, TRY FORM DATA
    # ----------------------------
    if not req_body:
        form = req.form
        req_body = {
            "to": form.get("to"),
            "subject": form.get("subject"),
            "body": form.get("body")
        }

    # ----------------------------
    # 3) Extract fields
    # ----------------------------
    to_email = req_body.get("to")
    subject = req_body.get("subject")
    body_text = req_body.get("body")

    if not all([to_email, subject, body_text]):
        return func.HttpResponse(
            json.dumps({"success": False, "error": "Missing fields (to, subject, body)"}),
            status_code=400,
            mimetype="application/json"
        )

    # Gmail configuration
    sender_email = "studyversebydivyansh@gmail.com"
    app_password = "lzza zxll mbvc weti"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body_text, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, to_email, msg.as_string())

        return func.HttpResponse(
            json.dumps({"success": True, "message": "Email sent successfully"}),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Email send error: {e}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
