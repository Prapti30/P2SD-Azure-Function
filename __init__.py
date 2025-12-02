import azure.functions as func
import json
import logging
from .function_app import fetch_databricks_data

app = func.FunctionApp()

@app.route(route="GetDatabricksData", auth_level=func.AuthLevel.ANONYMOUS)
def GetDatabricksData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Azure Function Trigger Hit (__init__.py)")

    result = fetch_databricks_data()

    return func.HttpResponse(
        json.dumps(result, indent=4),
        mimetype="application/json",
        status_code=200
    )
