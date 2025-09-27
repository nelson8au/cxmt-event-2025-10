import json
from redis import Redis

# --- Redis connection ---
REDIS_HOST = "cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_USERNAME = "default"
REDIS_SSL = True

redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    decode_responses=True,
    ssl=REDIS_SSL,
)

# --- Prize initialization ---
def init_all_prizes():
    """
    Define all prizes for events 2025-10-01 to 2025-10-08
    """
    return {
        "20251001": [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                     28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                     88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28],
        "20251002": ["京东E卡50"] * 40 + ["京东E卡1000"],
        "20251003": ["商城积分18积分"] * 49 + ["京东E卡100"],
        "20251004": [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                     28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                     88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28],
        "20251005": ["京东E卡50"] * 20 + ["京东E卡200"],
        "20251006": [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                     28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                     88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28],
        "20251007": ["商城积分18积分"] * 49 + ["京东E卡100"],
        "20251008": ["商城积分18积分"] * 49 + ["京东E卡100"]
    }

# --- Redis helpers ---
def reset_event(key: str, prizes: list) -> str:
    """Reset a single event Redis list."""
    redis.delete(key)
    if prizes:
        redis.rpush(key, *prizes)
    return f"Event {key} prizes have been recreated."

def check_all_event_lengths() -> dict:
    """Return current lengths of all Redis prize lists."""
    lengths = {}
    for i in range(1, 9):
        key = f"2005mid:100{i}"
        lengths[key] = redis.llen(key)
    return lengths

# --- Main Lambda-compatible handler ---
def manage_event(eventname: str):
    prizes_dict = init_all_prizes()
    try:
        if eventname == "all":
            # Reset all events
            for idx, prizes in enumerate(prizes_dict.values(), start=1):
                redis_key = f"2005mid:100{idx}"
                reset_event(redis_key, prizes)
            return "All events are updated"

        elif eventname == "check":
            # Check all Redis list lengths
            return check_all_event_lengths()

        elif eventname in prizes_dict:
            # Reset a single event
            idx = list(prizes_dict.keys()).index(eventname) + 1
            return reset_event(f"2005mid:100{idx}", prizes_dict[eventname])

        else:
            return "The event name you provided is not in the list. Please check the event name."

    except Exception as e:
        return f"Error: {str(e)}"

# --- Optional Lambda integration ---
# def lambda_handler(event, context):
#     eventname = None
#     if event.get("queryStringParameters"):
#         eventname = event["queryStringParameters"].get("eventname")

#     if not eventname:
#         return {
#             "statusCode": 400,
#             "body": json.dumps({"message": "Missing eventname query parameter"})
#         }

#     res = manage_event(eventname)
#     return {
#         "statusCode": 200,
#         "body": json.dumps({"message": res})
#     }
