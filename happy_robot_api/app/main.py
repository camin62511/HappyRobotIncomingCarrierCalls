from fastapi import FastAPI, Header, HTTPException, Request
import json
from pathlib import Path
import os
import boto3
import logging

app = FastAPI()

# AWS CloudWatch
region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
cloudwatch = boto3.client("cloudwatch", region_name=region)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "load_data" / "loads.json"

API_KEY = os.getenv("LOADS_API_KEY", "dev-secret-key")


@app.get("/loads")
async def get_loads(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    with open(DATA_FILE, "r") as f:
        data = json.load(f)  # data is a list

    return {
        "count": len(data),
        "data": data
    }

# Helper for POST call
def parse_money(value, default=0.0) -> float:
    if value is None:
        return default
    s = str(value).strip()
    if s == "":
        return default
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return default

@app.post("/call-data")
async def post_call_data(request: Request, x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    try:
        event = await request.json()
        logger.info("FULL EVENT RECEIVED:")
        logger.info(json.dumps(event))

        data = event

        # Parse fields for CloudWatch
        try:
            starting_price = parse_money(data.get("starting_price"), default=0.0)
            final_price = parse_money(data.get("final_price"), default=0.0)


            negotiation_rounds = int(data.get("negotiation_rounds", 0))

            sentiment = str(data.get("sentiment", "Neutral"))
            if sentiment not in ["Non-confrontational", "Hostile", "Confused", "Frustrated"]:
                sentiment = "Neutral"

            invalid_mc = not bool(data.get("is_valid_mc", True))

        except Exception as e:
            logger.error(f"Error parsing payload: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid payload format")

        # Prepare metrics
        metrics = [
            {"MetricName": "CallCount", "Value": 1, "Unit": "Count"},
            {"MetricName": "NegotiationRounds", "Value": negotiation_rounds, "Unit": "Count"},
            {"MetricName": "StartingPrice", "Value": starting_price, "Unit": "None"},
            {"MetricName": "FinalPrice", "Value": final_price, "Unit": "None"},
            {"MetricName": "InvalidMCCount", "Value": 1 if invalid_mc else 0, "Unit": "Count"},
            {
                "MetricName": "Sentiment",
                "Dimensions": [{"Name": "Type", "Value": sentiment}],
                "Value": 1,
                "Unit": "Count"
            }
        ]

        # Push metrics to CloudWatch
        try:
            cloudwatch.put_metric_data(
                Namespace="InboundCarrierCalls",
                MetricData=metrics
            )
        except Exception as e:
            logger.error(f"Failed to populate metrics in CloudWatch: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to populate metrics")

        return {"message": "Metrics populated successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception("Unexpected error")
        raise HTTPException(status_code=500, detail="Internal server error")
    

