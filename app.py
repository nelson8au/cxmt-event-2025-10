# enhanced_prize_lambda.py
import os
import json
import logging
import uuid
from decimal import Decimal
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Any, Dict

import boto3
from boto3.dynamodb.conditions import Key
from redis import Redis
import requests
from requests.adapters import HTTPAdapter, Retry

# --- Configuration (use env vars in Lambda) ---
REDIS_HOST = os.getenv("REDIS_HOST", "cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")
REDIS_SSL = os.getenv("REDIS_SSL", "True").lower() in ("1", "true", "yes")
DDB_REGION = os.getenv("DDB_REGION", "ap-northeast-1")
EVENT_RECORD_TABLE = os.getenv("EVENT_RECORD_TABLE", "cxmt-event-prize-draw-records")
LEDGER_TABLE = os.getenv("LEDGER_TABLE", "cxmt-lps-ledger")

# External endpoints & token should come from environment
CXM_BASE = os.getenv("CXM_BASE", "https://secure.cxmtrading.com")
CXM_TOKEN = os.getenv("CXM_TOKEN", "eyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps%2Fs%2B")  # token only via env var

# Business constants
RATE = Decimal(os.getenv("USD_RATE", "7.1"))
shanghai_tz = timezone(timedelta(hours=8))
# Logging
# logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# logger = logging.getLogger(__name__)

# --- AWS clients / resources ---
dynamodb_resource = boto3.resource("dynamodb", region_name=DDB_REGION)
record_table = dynamodb_resource.Table(EVENT_RECORD_TABLE)
ledger_table = dynamodb_resource.Table(LEDGER_TABLE)

# --- Redis connection (lazy) ---
redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    decode_responses=True,
    ssl=REDIS_SSL,
)

# --- HTTP session with retries ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({"Content-Type": "application/json"})

# --- User-facing messages (use dictionary access) ---
RETURN_MSG = {
    "msg1": "恭喜您，抢到**奬品, 我们会尽快安排发送奬品！",
    "msg2": "您已经抽过奬品啦！",
    "msg3": "抽奬尚未开启，请耐心等待",
    "msg4": "您未满足抽奬的设置条件，不可以抽哦",
    "msg5": "请输入手机号码、电子邮箱及收货地址，我们将根据奖品类型发送至您提供的地址或手机号码。",
    "msg6": "今天所有獎品已經發放完畢，下次請及早參與！"
}

# --- Helpers ---
def today_str() -> str:
    return date.today().isoformat()

def safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except ValueError:
        print(f"Response has no JSON: {resp.text}" )
        return None

def update_table(
    email: str,
    rp_amount: Decimal,
    lp_amount: Decimal,
    trans_amt: Decimal,
    us_rate: Decimal,
    account: str,
    to_sid: str,
    rp_status: str,
    lp_status: str,
    prize_status: str,
    event_name: str,
    prize: str,
) -> Dict[str, Any]:
    """
    Update event record for a given user/email and today's date.
    """
    item_date = today_str()
    try:
        resp = record_table.update_item(
            Key={"email": email, "date": item_date},
            UpdateExpression=(
                "SET redpocket = :rp, loyaltypoint = :lp, trans_amt = :ta, "
                "us_rate = :r, account = :l, toSid = :sid, date_created = :dt, "
                "rp_status = :rp_status, lp_status = :lp_status, prize_status = :prize_status, "
                "event = :event, prize = :prize"
            ),
            ExpressionAttributeValues={
                ":rp": Decimal(str(rp_amount)),
                ":lp": Decimal(str(lp_amount)),
                ":ta": Decimal(str(trans_amt)),
                ":r": Decimal(str(us_rate)),
                ":l": account or "",
                ":sid": to_sid or "",
                ":rp_status": rp_status or "",
                ":lp_status": lp_status or "",
                ":prize_status": prize_status or "",
                ":dt": datetime.now(shanghai_tz).isoformat(),
                ":event": event_name or "",
                ":prize": prize or "",
            },
            ReturnValues="ALL_NEW",
        )
        print(f"Updated record_table: {resp}")
        return resp
    except Exception as exc:
        print(f"Failed to update record table for {email}" )
        raise

def update_ledger(lp_amount: Decimal, user_id: str) -> Dict[str, Any]:
    """
    Add a ledger entry and increment lp_wallets for a user ledger type Normal.
    """
    entry_id = f"{date.today().strftime('%Y%m%d')}{uuid.uuid4().hex}"
    items = {
        "id": entry_id,
        "date": today_str(),
        "description": "Autumn Serial Event Prize 秋季活动奖励",
        "credit": str(lp_amount),
        "debit": str(0),
    }
    try:
        resp = ledger_table.update_item(
            Key={"user_id": user_id, "ledger_type": "Normal"},
            UpdateExpression="SET lp_ledger.#entry_id = :vals, lp_wallets = lp_wallets + :lp, last_lp_update = :dt",
            ExpressionAttributeNames={"#entry_id": entry_id},
            ExpressionAttributeValues={
                ":vals": items,
                ":lp": Decimal(str(lp_amount)),
                ":dt": datetime.now(shanghai_tz).isoformat()
            },
            ReturnValues="ALL_NEW",
        )
        print(f"Updated ledger: {resp}" )
        return resp
    except Exception:
        print(f"Failed to update ledger for user_id={user_id}" )
        raise

def query_ledger(email: str) -> Optional[str]:
    """Return user_id for email via ledger table email-index. Returns None if not found."""
    try:
        resp = ledger_table.query(IndexName="email-index", KeyConditionExpression=Key("email").eq(email))
        items = resp.get("Items", [])
        if not items:
            print(f"No ledger entry found for email {email})")
            return None
        user_id = items[0].get("user_id")
        print(f"Found user_id {user_id} for email {email}")
        return user_id
    except ledger_table.meta.client.exceptions.ResourceNotFoundException:
        print("Ledger table or index not found")
        return None
    except Exception as e:
        print(f"Error querying ledger for {email}: {e}")
        return None

def query_event_rec(email: str, day: str) -> bool:
    """Return True if event record exists."""
    try:
        resp = record_table.get_item(Key={"email": email, "date": day})
        exists = "Item" in resp
        print(f"Event record exists for {email} on day {exists}")
        return exists
    except Exception:
        print(f"Failed to query event record for {email}")
        return False

# --- CXM / external helper calls ---
def fetch_deposit(date_range: str, user_id: str) -> Optional[Decimal]:
    """Fetch deposit summary amountInUsd from CXM manager endpoint."""
    if not CXM_TOKEN:
        print(f"CXM_TOKEN not provided")
        return None
    url = f"{CXM_BASE}/api.manager.trans.getRange?token={CXM_TOKEN}&skip=0&take=10"
    payload = {"conditions": {"displayType": "deposit", "displayStatus": "success", "createdAt": date_range}, "isLd": False, "userId": user_id, "IsExcludeTransfers": True}
    try:
        resp = session.post(url, json=payload, timeout=10)
        data = safe_json(resp)
        # Expect shape {"summary": {"amountInUsd": 3200}, ...}
        if data and isinstance(data, dict) and "summary" in data:
            amt = data["summary"].get("amountInUsd")
            return Decimal(str(amt)) if amt is not None else None
        print(f"No summary in fetch_deposit response: {data}" )
        return None
    except Exception:
        print(f"Error fetching deposit for user_id={user_id}" )
        return None

def fetch_wallet(user_id: int) -> Optional[str]:
    """Return first wallet id from CXM wallet endpoint."""
    if not CXM_TOKEN:
        
        return None
    url = f"{CXM_BASE}/api.lps.user.wallet.getWallets?token={CXM_TOKEN}"

    payload = {"userId": user_id}

    try:
        resp = session.post(url, json=payload, timeout=10)
        if resp.status_code == 401:
            print(f'resp', resp)
            print(f"Unauthorized access for user {user_id}")
            return None
        data = safe_json(resp)
        
        if isinstance(data, list) and data:
            wallet_id = data[0].get("id")
            print(f"fetch_wallet -> {wallet_id}")
            return wallet_id
        print(f"fetch_wallet returned empty: {data}")
        return None
    except Exception:
        print(f"Error fetching wallet for {user_id}")
        return None

def transfer_meta_credit(account: str, amount: Decimal) -> Optional[requests.Response]:
    if not CXM_TOKEN:
        print(f"CXM_TOKEN not provided")
        return None
    url = f"{CXM_BASE}/api.lps.user.wallet.addMetaCredit?token={CXM_TOKEN}"
    payload = {"metaAccountId": account, "amount": float(amount), "comment": "Middle Autumn Lucky Draw", "expiration": "2025-10-31T23:14:01.1622132Z"}
    try:
        resp = session.post(url, json=payload, timeout=10)
        print(f"transfer_meta_credit status={resp.status_code}")
        return resp
    except Exception:
        print(f"transfer_meta_credit failed")
        return None

def transfer_money_wallet(wallet: str, amount: Decimal) -> Optional[requests.Response]:
    if not CXM_TOKEN:
        print(f"CXM_TOKEN not provided")
        return None
    url = f"{CXM_BASE}/api.lps.user.wallet.walletBalanceCorrection?token={CXM_TOKEN}"
    payload = {"lpsWalletId": wallet, "amount": float(amount), "comment": "Middle Autumn Lucky Draw"}
    try:
        resp = session.post(url, json=payload, timeout=10)
        print(f"transfer_money_wallet status={resp.status_code}")
        return resp
    except Exception:
        print(f"transfer_money_wallet failed")
        return None

def fetch_trade_data(user_id: str, daterange: str) -> bool:
    """Return True if user has at least one USD trade in given date range."""
    if not CXM_TOKEN:
        print(f"CXM_TOKEN not provided")
        return False
    url = f"{CXM_BASE}/api.manager.report.loyalty.meta.deal.getRange?token={CXM_TOKEN}&skip=0&take=10"
    payload = {"conditions": {"closeTime": daterange, "userID": f"{user_id};{user_id}"}}

    try:
        resp = session.post(url, json=payload, timeout=10)
        data = safe_json(resp) or {}
        items = data.get("items", [])
        # count items with currency == USD
 
        for item in items:
            if item.get("currency") == "USD":
                return True
        return False
    except Exception:
        print(f"Error fetching trade data for {user_id}")
        return False

# --- Prize processing (generic) ---
def pop_prize(prize_key: str) -> Optional[str]:
    try:
        prize = redis.rpop(prize_key)
        print(f"popped prize from {prize_key} -> {prize}" )
        return prize
    except Exception:
        print(f"Failed to pop prize key={prize_key}" )
        return None

def process_red_envelope(email: str, prize: str, event_name: str, prize_key: str) -> Dict[str, Any]:
    """
    Handles cases where prize is a numeric red envelope amount (value can be parsed to float).
    """
    try:
        rp_amount = Decimal(str(float(prize)))
    except Exception:
        print(f"Invalid red envelope amount: {prize}")
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "Invalid prize amount"})}

    trans_amt = (rp_amount / RATE).quantize(Decimal("0.01"))
    user_id = query_ledger(email)
    if not user_id:
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "User not found"})}

    wallet = fetch_wallet(user_id)
    if not wallet:
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "User wallet not found"})}

    txresp = transfer_money_wallet(wallet, trans_amt)
    rp_status = "success" if txresp is not None and getattr(txresp, "status_code", 0) == 200 else "failed"

    update_table(email, rp_amount, Decimal("0"), trans_amt, RATE, wallet, "0", rp_status, "", "", event_name, prize)

    return {
        "statusCode": 200,
        "body": json.dumps({"Prize": "Red Envelope", "Amount": float(rp_amount), "TransactionStatus": rp_status}),
    }

def process_email_prize(email: str, prize: str, event_name: str) -> Dict[str, Any]:
    """When prize is to be fulfilled by email (e.g. voucher)."""
    update_table(email, Decimal("0"), Decimal("0"), Decimal("0"), RATE, "", "", "", "", "email", event_name, prize)
    return {"statusCode": 200, "body": json.dumps({"Prize": prize, "TransactionStatus": "email"})}
   
def process_lp_or_coupon(email: str, prize: str, event_name: str, lp_amount: Decimal = Decimal("18")) -> Dict[str, Any]:
    """Handle awarding loyalty points (lp) or coupons. Default lp_amount 18 (as in original)."""
   
    user_id = query_ledger(email)
    if not user_id:
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "User not found"})}
    # Update ledger (will raise/log internally on failure)
    ledger_resp = update_ledger(lp_amount, user_id)
    status = "success" if ledger_resp.ResponseMetadata.get("HTTPStatusCode") == 200 else "failure"
    update_table(email, Decimal("0"), lp_amount, Decimal(0), RATE, "", "", "", status, "", event_name, prize)
    return {"statusCode": 200, "body": json.dumps({"Prize": prize, "TransactionStatus": "ledger_updated"})}
  
# --- Event-specific high-level wrappers (these call above helpers) ---
def handle_event_generic(email: str, prize_key: str, event_name: str, eligibility_fn=None, prize_type_hint: Optional[str] = None):
    """
    Generic handler:
      - Check already claimed
      - Optionally run eligibility_fn(user_id) -> bool
      - Pop prize from redis prize_key
      - Process based on prize_type_hint or prize value
    """
    if not email:
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "Missing email"})}
    day = today_str()
    if query_event_rec(email, day):
        return {"statusCode": 200, "body": json.dumps({"Status": "1", "Message": RETURN_MSG["msg2"]})}

    user_id = query_ledger(email)
    if not user_id:
        return {"statusCode": 400, "body": json.dumps({"Status": "error", "Message": "User ledger not found"})}

    # eligibility check
    if eligibility_fn:
        try:
            ok = eligibility_fn(user_id)
        except Exception:
            print(f"Eligibility check failed for {user_id}")
            return {"statusCode": 500, "body": json.dumps({"Status": "error", "Message": "Unable to verify eligibility"})}
        if not ok:
            return {"statusCode": 200, "body": json.dumps({"Status": "2", "Message": RETURN_MSG["msg4"]})}

    prize = pop_prize(prize_key)
    if not prize:
        return {"statusCode": 200, "body": json.dumps({"Status": "3", "Message": RETURN_MSG["msg6"]})}

    # Decide handling: email voucher, red envelope number, JD card etc.
    # Heuristics: if prize looks numeric -> red envelope; if contains "京东" or "E卡" -> email; else if prize looks like LP -> treat as lp
    try:
        prize_clean = prize.strip()
        print('prize_clean', prize_clean)
        # numeric -> red envelope
        try:
            float(prize_clean)
            return process_red_envelope(email, prize_clean, event_name, prize_key)
        except Exception:
            # not numeric
            if "京东" in prize_clean or "京东" in prize_clean or "E卡" in prize_clean:
                print("京东京东")
                return process_email_prize(email, prize_clean, event_name)
            if "商城" in prize_clean or "商城" in prize_clean or "积分" in prize_clean:
                return process_lp_or_coupon(email, prize_clean, event_name)
            # fallback: email
            return process_email_prize(email, prize_clean, event_name)
    except Exception:
        print(f"Error processing prize for {email} prize={prize}")
        return {"statusCode": 500, "body": json.dumps({"Status": "error", "Message": "Internal error while processing prize"})}

# --- Eligibility helper factory examples ---
def deposit_eligibility_factory(date_range: str, required: Decimal):
    def fn(user_id: str) -> bool:
        amt = fetch_deposit(date_range, user_id)
        if amt is None:
            return False
        return amt >= required
    return fn

def trade_eligibility_factory(date_range: str):
    def fn(user_id: str) -> bool:
        return fetch_trade_data(user_id, date_range)
    return fn

# --- Lambda handler ---
def lambda_handler(event, context):
    """
    Expects POST body with JSON { "event_name": "...", "email": "..." }
    Query string parameter 'funcname' determines which event to run (mapping below).
    """
    try:
        payload = json.loads(event.get("body", "{}"))
    except Exception:
        payload = {}

    event_name = payload.get("event_name")
    email = payload.get("email")

    params = event.get("queryStringParameters") or {}
    funcname = params.get("funcname")

    # Map funcname to prize_key, event name and eligibility
    mapping = {
        # "e20251001": {"prize_key": "2025mid:1001", "event": "2025-10-01", "eligibility": None},
        # "e20251002": {"prize_key": "2025mid:1002", "event": "2025-10-02", "eligibility": deposit_eligibility_factory("2025-09-01;2025-09-30", Decimal("2000"))},
        # "e20251003": {"prize_key": "2025mid:1003", "event": "2025-10-03", "eligibility": trade_eligibility_factory("2025-10-01;2025-10-02")},
        # "e20251004": {"prize_key": "2025mid:1004", "event": "2025-10-04", "eligibility": trade_eligibility_factory("2025-10-03;2025-10-03")},
        # "e20251005": {"prize_key": "2025mid:1005", "event": "2025-10-05", "eligibility": deposit_eligibility_factory("2025-10-01;2025-10-04", Decimal("500"))},
        # "e20251006": {"prize_key": "2025mid:1006", "event": "2025-10-06", "eligibility": None},
        # "e20251007": {"prize_key": "2025mid:1007", "event": "2025-10-07", "eligibility": trade_eligibility_factory("2025-10-06;2025-10-06")},
        # "e20251008": {"prize_key": "2025mid:1008", "event": "2025-10-08", "eligibility": trade_eligibility_factory("2025-10-07;2025-10-07")},
        "e20251001": {"prize_key": "2025mid:1001", "event": "2025-10-01", "eligibility": None},
        "e20251002": {"prize_key": "2025mid:1002", "event": "2025-10-02", "eligibility": deposit_eligibility_factory("2025-09-01;2025-09-30", Decimal("2000"))},
        "e20251003": {"prize_key": "2025mid:1003", "event": "2025-10-03", "eligibility": trade_eligibility_factory("2025-09-01;2025-09-30")},
        "e20251004": {"prize_key": "2025mid:1004", "event": "2025-10-04", "eligibility": trade_eligibility_factory("2025-09-01;2025-09-30")},
        "e20251005": {"prize_key": "2025mid:1005", "event": "2025-10-05", "eligibility": deposit_eligibility_factory("2025-09-01;2025-09-30", Decimal("500"))},
        "e20251006": {"prize_key": "2025mid:1006", "event": "2025-10-06", "eligibility": None},
        "e20251007": {"prize_key": "2025mid:1007", "event": "2025-10-07", "eligibility": trade_eligibility_factory("2025-09-01;2025-09-30")},
        "e20251008": {"prize_key": "2025mid:1008", "event": "2025-10-08", "eligibility": trade_eligibility_factory("2025-09-01;2025-09-30")},
    }

    if funcname is None:
        return {"statusCode": 400, "body": json.dumps({"message": "Missing funcname parameter"})}

    # admin actions
    if funcname == "reset":
        from topup import manage_event  # lazy import in case not available in some contexts
        res=manage_event("reset")
        return {"statusCode": 200, "body": json.dumps({"message": "reset requested", "data": res})}
    if funcname == "topupall":
        from topup import manage_event  # lazy import in case not available in some contexts
        res=manage_event("topupall")
        return {"statusCode": 200, "body": json.dumps({"message": "reset requested"})}
    if funcname == "getBalance":
        from topup import manage_event
        res=manage_event("check")
        return {"statusCode": 200, "body": json.dumps({"message": "balance check requested", "data":res})}


    if funcname == "topup_event":
        eventdate = params.get("eventdate")
        from topup import manage_event
        manage_event(eventdate)
        return {"statusCode": 200, "body": json.dumps({"message": f"topup requested for {eventdate}"})}

    config = mapping.get(funcname)
    if not config:
        return {"statusCode": 400, "body": json.dumps({"message": "Unknown funcname"})}

    # Run generic handler
    result = handle_event_generic(email=email, prize_key=config["prize_key"], event_name=config["event"], eligibility_fn=config["eligibility"])
    return result

# If running locally for testing:
# if __name__ == "__main__":
#     # quick local test harness (adjust body and queryStringParameters)
#     fake_event = {"body": json.dumps({"event_name": "autumn", "email": "test@example.com"}), "queryStringParameters": {"funcname": "e20251001"}}
#     print(lambda_handler(fake_event, None))
