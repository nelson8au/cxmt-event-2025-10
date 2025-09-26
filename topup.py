
import json
from redis import Redis

# Connect to Redis
redis = Redis(
    host='cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com',
    port=6379,
    decode_responses=True,
    ssl=True,
    username='default'
)

# Helper function to reset a Redis list for an event
def reset_event(key, prizes):
    redis.delete(key)
    redis.rpush(key, *prizes)
    return f'Event {key} prizes have been recreated.'

# Initialize all prizes
def init_all_prizes():
    # Event 1
    prize1001 = [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                 28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                 88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28]

    # Event 2
    prize1002 = ["京东E卡50"] * 40 + ["京东E卡1000"]

    # Event 3
    prize1003 = ["商城积分18积分"] * 49 + ["京东E卡100"]

    # Event 4
    prize1004 = [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                 28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                 88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28]

    # Event 5
    prize1005 = ["京东E卡50"] * 20 + ["京东E卡200"]

    # Event 6
    prize1006 = [28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58,
                 28, 88, 18, 28, 58, 18, 88, 28, 58, 8, 18, 58, 88, 28, 58,
                 88, 8, 28, 8, 88, 18, 28, 58, 18, 8, 58, 28, 18, 8, 88, 8, 58, 18, 8, 28]

    # Event 7
    prize1007 = ["商城积分18积分"] * 49 + ["京东E卡100"]

    # Event 8
    prize1008 = ["商城积分18积分"] * 49 + ["京东E卡100"]

    return {
        "20251001": prize1001,
        "20251002": prize1002,
        "20251003": prize1003,
        "20251004": prize1004,
        "20251005": prize1005,
        "20251006": prize1006,
        "20251007": prize1007,
        "20251008": prize1008
    }

# New function: check all event Redis list lengths
def check_all_event_lengths():
    lengths = {}
    for i in range(1, 9):
        key = f"2005mid:100{i}"
        lengths[key] = redis.llen(key)
    return lengths

# Lambda handler
def manage_event(eventname):
    prizes_dict = init_all_prizes()
    try:
        if eventname == "all":
            # Reset all events
            for idx, (key, prizes) in enumerate(prizes_dict.items(), start=1):
                reset_event(f"2005mid:100{idx}", prizes)
            res = "All events are updated"

        elif eventname == "check":
            # Check all Redis list lengths
            res = check_all_event_lengths()

        elif eventname in prizes_dict:
            # Reset a single event
            idx = list(prizes_dict.keys()).index(eventname) + 1
            res = reset_event(f"2005mid:100{idx}", prizes_dict[eventname])
        else:
            res = "The event name you provided is not in the list. Please check the event name."

    except Exception as e:
        res = f"Error: {str(e)}"

    return res
# import json
# from redis import Redis
# redis=Redis(host='cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com', port=6379, decode_responses=True, ssl=True, username='default')
# # import requests
# def init_prize(prize1001, prize1002, prize1003, prize1004, prize1005, prize1006, prize1007, prize1008):
#     redis.delete('2005mid:1001')
#     redis.delete('2005mid:1002')
#     redis.delete('2005mid:1003')
#     redis.delete('2005mid:1004')
#     redis.delete('2005mid:1005')
#     redis.delete('2005mid:1006')
#     redis.delete('2005mid:1007')
#     redis.delete('2005mid:1008')
#     for i in prize1001:
#         res=redis.rpush('2005mid:1001', i)
#     for i in prize1002:
#         res=redis.rpush('2005mid:1002', i)
#     for i in prize1003:
#         res=redis.rpush('2005mid:1003', i)
#     for i in prize1004:
#         res=redis.rpush('2005mid:1004', i)
#     for i in prize1005:
#         res=redis.rpush('2005mid:1005', i)
#     for i in prize1006:
#         res=redis.rpush('2005mid:1006', i)
#     for i in prize1007:
#         res=redis.rpush('2005mid:1007', i)
#     for i in prize1008:    
#         res=redis.rpush('2005mid:1008', i)
#     return 'All event prizes have been recreated.'

# def event1001(prize1001):
#     redis.delete('2005mid:1001')
#     for i in prize1001:
#         res=redis.rpush('2005mid:1001', i)
#     return 'Event 2025-10-01 prizes have been recreated.'

# def event1002(prize1002):
#     redis.delete('2005mid:1002')
#     for i in prize1002:
#         res=redis.rpush('2005mid:1002', i)
#     return 'Event 2025-10-02 prizes have been recreated.'

# def event1003(prize1003):
#     redis.delete('2005mid:1003')
#     for i in prize1003:
#         res=redis.rpush('2005mid:1003', i)
#     return 'Event 2025-10-03 prizes have been recreated.'

# def event1004(prize1004):
#     redis.delete('2005mid:1004')
#     for i in prize1004:
#         res=redis.rpush('2005mid:1004', i)
#     return 'Event 2025-10-04 prizes have been recreated.'

# def event1005(prize1005):
#     redis.delete('2005mid:1005')
#     for i in prize1005:
#         res=redis.rpush('2005mid:1005', i)
#     return 'Event 2025-10-05 prizes have been recreated.'

# def event1006(prize1006):
#     redis.delete('2005mid:1006')
#     for i in prize1006:
#         res=redis.rpush('2005mid:1006', i)
#     return 'Event 2025-10-06 prizes have been recreated.'

# def event1007(prize1007):
#     redis.delete('2005mid:1007')
#     for i in prize1007:
#         res=redis.rpush('2005mid:1007', i)
#     return 'Event 2025-10-07 prizes have been recreated.'

# def event1008(prize1008):
#     redis.delete('2005mid:1008')
#     for i in prize1008:
#         res=redis.rpush('2005mid:1008', i)
#     return 'Event 2025-10-08 prizes have been recreated.'

# def lambda_handler(event, context):
# # 有以下几点说明：
# # 活动需要在9月 15日发布
# # 每天的福袋设置如下： 10月1日：无门槛参与，福袋总额¥1888，设置金额如下：
# # ¥8 20个
# # ¥18 10个
# # ¥28 10个
# # ¥58 10个
# # ¥88 8个 
# # 10月2日：共设置41份奖项， 京东E卡1000 元1份， 京E卡50元 40份 其中1000元最高福袋放在第41个。 
# # 10月3日：共设置50个奖项， 京东E卡100元1份， 积分商城积分18积分共49份 其中100元最高福袋放在第50个。 
# # 10月4日：无门槛参与，福袋总额¥1888，设置金额如下：
# # ¥8 20个
# # ¥18 10个
# # ¥28 10个
# # ¥58 10个
# # ¥88 8个 
# # 10月5日：共设置21份奖项， 京东E卡200 元1份， 京E卡50元 20份 其中200元最高福袋放在第21个。 
# # 10月6日：无门槛参与，福袋总额¥1888，设置金额如下：
# # ¥8 20个
# # ¥18 10个
# # ¥28 10个
# # ¥58 10个
# # ¥88 8个 
# # 10月7日：共设置50个奖项， 京东E卡100元1份， 积分商城积分18积分共49份 其中100元最高福袋放在第50个。 
# # 10月8日：共设置50个奖项， 京东E卡100元1份， 积分商城积分18积分共49份 其中100元最高福袋放在第50个。  
# # 每个客户仅能参与一次
# # 活动时间10月1日——10月8日
#     prize1001=[28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58, 28, 88, 18, 28, 58,
#  18, 88, 28, 58, 8, 18, 58, 88, 28, 58, 88, 8, 28, 8, 88, 18, 28, 58, 18, 8,
#  58, 28, 18, 8, 88, 8, 58, 18, 8, 28]
    
#     prize1002 = [
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡1000"
# ]
#     prize1003 = [
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","京东E卡100"]

#     prize1004=[28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58, 28, 88, 18, 28, 58,
#  18, 88, 28, 58, 8, 18, 58, 88, 28, 58, 88, 8, 28, 8, 88, 18, 28, 58, 18, 8,
#  58, 28, 18, 8, 88, 8, 58, 18, 8, 28]
    
#     prize1005=  ["京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡50","京东E卡50","京东E卡50","京东E卡50","京东E卡50",
#     "京东E卡200"]
    
#     prize1006=[28, 18, 88, 8, 58, 18, 88, 28, 58, 8, 88, 28, 18, 8, 58, 28, 88, 18, 28, 58,
#  18, 88, 28, 58, 8, 18, 58, 88, 28, 58, 88, 8, 28, 8, 88, 18, 28, 58, 18, 8,
#  58, 28, 18, 8, 88, 8, 58, 18, 8, 28]

#     prize1007 = [
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","京东E卡100"]

#     prize1008 = [
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分",
#     "商城积分18积分","商城积分18积分","商城积分18积分","商城积分18积分","京东E卡100"]

#     #payload=json.loads(event["body"])
#     #eventname=payload.get("eventname")
#     if event["queryStringParameters"] is None:
#         return {
#             "statusCode": 400,
#             "body": json.dumps({
#                 "message": "Request query string is empty, please provide the event name",
#                 # "location": ip.text.replace("\n", "")
#             }),
#         }
#     else:
#         eventname=event["queryStringParameters"]["eventname"]
    
#     match eventname:
#         case '20251001':
#             res=1001(prize1001)
#         case '20251002':
#             res=1002(prize1002)
#         case '20251003':
#             res=1003(prize1003)
#         case '20251004':
#             res=1004(prize1004)
#         case '20251005':
#             res=1005(prize1005)
#         case '20251006':
#             res=1006(prize1006)
#         case '20251007':
#             res=1007(prize1007)
#         case '20251008':
#             res=1008(prize1008)
#         case 'all':
#             try:
#                 init_prize(prize1001, prize1002, prize1003, prize1004, prize1005, prize1006, prize1007, prize1008)
#                 res="All events are updated"
                
#             except Exception as e:
#                 res="Error"
                
#         case _:
#             res="The event name you provided is not in the list. Please check the event name."
            
    
#     #res=redis.json().set('test', "$", [prize1])
#     #res=redis.json().get('test', "$")
#     return {
#         "statusCode": 200,
#         "body": json.dumps({
#             "message": res,
#             # "location": ip.text.replace("\n", "")
#         }),
#     }
