import json
from redis import Redis

# --- Redis connection ---
# REDIS_HOST = "cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com"
# REDIS_PORT = 6379
# REDIS_USERNAME = "default"
# REDIS_SSL = True

# redis = Redis(
#     host=REDIS_HOST,
#     port=REDIS_PORT,
#     username=REDIS_USERNAME,
#     decode_responses=True,
#     ssl=REDIS_SSL,
# )
redis=Redis(host='cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com', port=6379, decode_responses=True, ssl=True, username='default')
print(redis)
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
def topup_event(key: str, prizes: list) -> str:
    """Delete + repopulate prizes for a single event key."""
    redis.delete(key)
    redis.lpush(key, *prizes)
    return f"[TOPUP] {key} recreated with {len(prizes)} prizes."

def reset_event(key: str) -> str:
    """Delete a single event key only."""
    redis.delete(key)
    return f"[RESET] {key} deleted. Exists now? {bool(redis.exists(key))}"

def check_all_event_lengths() -> dict:
    """Check lengths of all event keys."""
    lengths = {}
    for i in range(1, 9):
        key = f"2025mid:100{i}"
        lengths[key] = redis.llen(key)
    return lengths

# --- Main manager ---
def manage_event(eventname: str):
    prizes_dict = init_all_prizes()
    try:
        if eventname == "reset":
            results = []
            for idx in range(1, 9):
                key = f"2025mid:100{idx}"
                results.append(reset_event(key))
            return results

        elif eventname == "topupall":
            results = []
            for idx, prizes in enumerate(prizes_dict.values(), start=1):
                key = f"2025mid:100{idx}"
                results.append(topup_event(key, prizes))
            return results

        elif eventname == "check":
            return check_all_event_lengths()

        elif eventname in prizes_dict:
            idx = list(prizes_dict.keys()).index(eventname) + 1
            key = f"2025mid:100{idx}"
            return reset_event(key)

        else:
            return f"Unknown event: {eventname}"

    except Exception as e:
        return {"error": str(e)}