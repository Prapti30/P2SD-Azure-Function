import datetime
import os
import json
import logging
import azure.functions as func
import requests
from databricks.vector_search.client import VectorSearchClient
from databricks.vector_search.reranker import DatabricksReranker
import mlflow
from dotenv import load_dotenv

load_dotenv()

mlflow.set_tracking_uri("databricks")


os.environ["WORKSPACE_URL"] = os.getenv("WORKSPACE_URL")
os.environ["DATABRICKS_TOKEN"] = os.getenv("DATABRICKS_TOKEN")
os.environ["DATABRICKS_HOST"] = os.getenv("DATABRICKS_HOST")

utcnow = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
 

class DatabricksLLM:
    def __init__(self):
        self.workspace_url =os.getenv("WORKSPACE_URL")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.endpoint_url = os.getenv("endpoint_url")
    def generate_response(self, query_text, max_tokens=5000) -> str:
        vsc = VectorSearchClient(workspace_url=self.workspace_url, personal_access_token=self.databricks_token)
        try:
            index = vsc.get_index(endpoint_name="vectordb", index_name="databricks_dev.default.anamolydetectionindex")
        except Exception as e:
            print("Error getting index:", e)
            return
        
        search_results = index.similarity_search(
            num_results=3,
            columns=["timestamp", "AssetId"],
            query_text=query_text,
            query_type="HYBRID",
            reranker=DatabricksReranker(columns_to_rerank=["timestamp"])
        )
        
        if 'result' in search_results and 'data_array' in search_results['result']:
            data = search_results['result']['data_array']
            columns = [col['name'] for col in search_results['manifest']['columns']]
            context = "Valve status data:\n"
            for row in data:
                context += ", ".join(f"{col}: {val}" for col, val in zip(columns, row)) + "\n"
        else:
            print("No relevant context found from vector search.")
            context = "You are an information extraction and reporting assistant whose task is to read the provided text, identify key data points, and respond with a single clear paragraph. Your goal is to extract core metrics such as maximum or minimum values (for example pressure or temperature), timestamps or dates, and any sensor or device identifiers, and present these elements in a natural, human-readable paragraph. The output must be exactly one paragraph in plain text that states the key metric and its value, states when it occurred if a timestamp or date is available, and states which sensor or identifier it is associated with if available, all within 1–3 concise sentences. Do not use bullet points, numbered lists, code blocks, or headings, do not repeat the user’s question, and do not include any meta-comments or explanatory phrases. Use clear, neutral language as if summarizing a log entry for an engineer or operator, and do not invent or assume any values that are not explicitly present in the input text"
            
        headers = {
        "Authorization": f"Bearer {self.databricks_token}",
        "Content-Type": "application/json"
        }
        payload = {
            "messages": [
                {"role": "system", "content": context},
                {"role": "user", "content": query_text}
            ],
            "max_tokens": max_tokens
        }
        resp = requests.post(self.endpoint_url, headers=headers, data=json.dumps(payload))
        print(resp.text)
        try:
            result = resp.json()
            return result
        except Exception:
            print("Error:", resp.text)
 
        