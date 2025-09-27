import json
from redis import Redis
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, date
import requests
import uuid
from topup import manage_event
today=str(date.today())
redis=Redis(host='cxmt-cache-eqwznd.serverless.apne1.cache.amazonaws.com', port=6379, decode_responses=True, ssl=True, username='default')
dynamodb_resource = boto3.resource("dynamodb")
record = dynamodb_resource.Table("cxmt-event-prize-draw-records")
#trade_table = dynamodb_resource.Table("cxmt_event_trade-2024")
#wallet_table = dynamodb_resource.Table("cxmt-event-wallet-2024")
ledger = dynamodb_resource.Table("cxmt-lps-ledger")
#lp= dynamodb_resource.Table("cxmt-event-lp-2024")
rate=7.1
# import requests
returnMsg = {
    'msg1': "恭喜您，抢到**奬品, 我们会尽快安排发送奬品！",
    "msg2": "您已经抽过奬品啦！",
    "msg3": "抽奬尚未开启，请耐心等待",
    "msg4": "您未满足抽奬的设置条件，不可以抽哦",
    "msg5": "请输入手机号码、电子邮箱及收货地址，我们将根据奖品类型发送至您提供的地址或手机号码。",
    "mag6": "今天所有獎品已經發放完畢，下次請及早參與！"
}
def update_table(email, rp_amount, lp_amount, trans_amt, us_rate, account, toSid, rp_status, lp_status, prize_status, event, prize):
    
    result = record.update_item(
        Key={
        'email': email, 
        'date': today
    },
    UpdateExpression='SET redpocket = :rp, loyaltypoint=:lp, trans_amt = :ta, us_rate = :r, account = :l, toSid = :sid, date_created = :datetime, rp_status = :rp_status, lp_status = :lp_status, prize_status = :prize_status, event = :event, prize=:prize',
    ExpressionAttributeValues={
        ':event': event,
        ':lp': Decimal(str(lp_amount)),
        ':rp': Decimal(str(rp_amount)),
        ':ta': Decimal(str(trans_amt)),
        ':r' : Decimal(str(us_rate)),
        ':l' : account,
        ':sid' : toSid,
        ':rp_status' : rp_status,
        ':lp_status' : lp_status,
        ':prize_status' : prize_status,
        ':datetime' : str(datetime.now()),
        ':prize': prize

        }
    
    )
    return result

def update_ledger(lp_amount, user_id):
    entrydate = date.today().strftime('%Y%m%d')
    entry_id = entrydate + str(int(uuid.uuid4()))
    
    items = {
            "id": entry_id,
            "date":str(today),
            "description": "Autumn Serial Event Prize 秋季活动奖励",
            "credit": str(lp_amount),
            "debit": str(0),
            }
    result = ledger.update_item(
        Key={
        'user_id': user_id, 
        'ledger_type': "Normal"
    },
    UpdateExpression='SET lp_ledger.#entry_id = :vals, lp_wallets = lp_wallets + :lp,  last_lp_update = :datetime',
    ExpressionAttributeNames={"#entry_id": entry_id},
    ExpressionAttributeValues={
        ':vals': items,
        ':lp': Decimal(str(lp_amount)),
        ':datetime' : str(datetime.now())
        }
    
    )
    return result

def fetch_deposit(date_range, userId):
    url = "https://secure.cxmtrading.com/api.manager.trans.getRange?token=eeyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+&skip=0&take=10"
    headers={"Content-Type":"application/json"}
    data ={"conditions":{"displayType":"deposit","displayStatus":"success","createdAt":date_range},"isLd":False,"userId": userId,"IsExcludeTransfers":True}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if (res.summary):
        print('fetch_deposit', res.summary.amountInUsd)
        return res.summary.amountInUsd
    else:
        print('empty')
    return False
def fetch_wallet(userId):
    url = "https://secure.cxmtrading.com/api.lps.user.wallet.getWallets?token=eeyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+"
    headers={"Content-Type":"application/json"}
    data ={"userID": userId}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if (res[0].id):
        print('fetch_wallet',res[0].id)
        return res[0].id
    else:
        print('empty')
    return False

# {
#   "summary": {
#     "amountInUsd": 3200,
#   },
# eyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+
# {"conditions":{"closeTime":"2025-09-25;2025-09-25","userID":"1047170;1047170"}}
def transfer_credit_metaaccount(account, amount):
    url="https://secure.cxmtrading.com//api.lps.user.wallet.addMetaCredit?token=eyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+"
    headers={"Content-Type":"application/json"}
    data = {
        "metaAccountId": account,
        "amount": amount,
        "comment": "Middle Autumn Lucky Draw",
        "expiration": "2025-10-31T23:14:01.1622132Z"
        }
    
    res = requests.post(url, data=json.dumps(data), headers=headers)
    
    print('credit transfer', res)
    return res

def transfer_meoney_wallet(wallet, amount):
    url="https://secure.cxmtrading.com/api.lps.user.wallet.walletBalanceCorrection?token=eyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+"
    headers={"Content-Type":"application/json"}
    data = {
        "lpsWalletId": wallet,
        "amount": amount,
        "comment": "Middle Autumn Lucky Draw",
        }
    
    res = requests.post(url, data=json.dumps(data), headers=headers)
    
    print('credit transfer', res)
    return res

def fetch_trade_data(userId, daterange):
    # userId= "1047170"
    # date_start='2025-09-25'
    # date_end='2025-09-25'
    
    data = {
        "conditions":{
            "closeTime": daterange,
            "userID":userId + ';' + userId
            }
    }
    url ='https://secure.cxmtrading.com/api.manager.report.loyalty.meta.deal.getRange?token=eyJpZCI6MTcsInVzZXJJZCI6MTAwMDAxMCwidHlwZSI6InN0YXRpYyIsImV4cGlyZWRBdCI6IjIwNTAtMDQtMTRUMDA6MDA6MDBaIiwicmVhZE9ubHkiOmZhbHNlLCJpc0FkbWluU2lnbmVkQXNVc2VyIjp0cnVlfS5exvzV6DaIzsRwADxBrqOPSJeU3eYSkPHQVt0Ps/s+&skip=0&take=10'
    response = requests.post(url, json=data, headers={'Content-Type': 'application/json'})
    resdata=response.json()
    print(len(resdata))
    tradelist= []
    if (resdata):
        for i in range(len(resdata)):
            if resdata[i]['currency'] == 'USD':   
                tradelist.append(resdata[i]['currency'])
        tradelist = list(set(tradelist))
    print(len(tradelist))
    if len(tradelist)>=1:
        return True
    else:
        return False

def query_ledger(email):
    response={}
    response=ledger.query(IndexName='email-index', KeyConditionExpression=Key('email').eq(email))
    print('response', response)
    if 'Items' not in response:
        return 0
    else:
        return response["Items"][0]["user_id"]
    

def query_event_rec(email, today):
    response={}
    response=record.get_item(Key={'email': email, 'date': today})
    if 'Item' not in response:
        return 0
    else:
        return 1
    
# 'msg1': "恭喜您，抢到**奬品, 我们会尽快安排发送奬品！",
#     "msg2": "您已经抽过奬品啦！",
#     "msg3": "抽奬尚未开启，请耐心等待",
#     "msg4": "您未满足抽奬的设置条件，不可以抽哦",
#     "msg5": "请输入手机号码、电子邮箱及收货地址，我们将根据奖品类型发送至您提供的地址或手机号码。",
# "mag6":"今天所有獎品已經發放完畢，下次請及早參與！"

def e20251001(email):
            # 1. Already claimed?
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 3. Pop prize from Redis
    
    prize_key = "2005mid:1001"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:
        rp_amount = float(prize)
        trans_amt = round(rp_amount / rate, 2)
        userId = query_ledger(email)
        wallet = fetch_wallet(userId)
        txres=transfer_meoney_wallet(wallet, trans_amt)
        rp_status = "success" if txres.status_code == 200 else "failed"

        update_table(email, rp_amount, 0, trans_amt, rate, wallet, "0", rp_status, "", "", "20251001","")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": "Red Envelope",
                "Amount": rp_amount,
                "TransactionStatus": rp_status
            }),
        }

    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }
def e20251002(email):
    # 1. Already claimed?
    userId = query_ledger(email)
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 2. Eligibility check (deposit condition)
    try:
        deposit_amount = fetch_deposit("2025-09-01;2025-09-30", userId)
        required_deposit = 2000  # condition threshold, adjust as needed

        if not deposit_amount or deposit_amount < required_deposit:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }

    # 3. Pop prize from Redis

    prize_key = "2005mid:1002"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:
        
        update_table(email, 0, 0, 0, rate, "", "", "", "", "email", "20251002", prize)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": prize,
                "TransactionStatus": "email"
            }),
        }

    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }        
def e20251003(email):
    # 1. Already claimed?
    userId = query_ledger(email)
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 2. Eligibility check (deposit condition)
    try:

        if fetch_trade_data(userId, "2025-10-01;2025-10-02") == False:    
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {userId}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }

    # 3. Pop prize from Redis

    prize_key = "2005mid:1003"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:

        
        if prize != "京东E卡100":
            res =update_ledger(18, userId) #if updated return success or failed
            update_table(email, 0, 0, 18, rate, "", "", "", res, "", "20251003", prize)
            return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": prize,
                "TransactionStatus": res
            }),
        }
        else:
            update_table(email, 0, 0, 0, rate, "", "", "", "", "email", "20251003", prize)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Prize": prize,
                    "TransactionStatus": "email"
                }),
            }
        
    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }        
def e20251004(email):
    # 1. Already claimed?
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }
    # 2. Eligibility check (deposit condition)
    try:

        if fetch_trade_data(userId, "2025-10-03;2025-10-03") == False:    
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {userId}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }
    # 3. Pop prize from Redis
    
    prize_key = "2005mid:1004"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:
        rp_amount = float(prize)
        trans_amt = round(rp_amount / rate, 2)
        userId = query_ledger(email)
        wallet = fetch_wallet(userId)
        txres=transfer_meoney_wallet(wallet, trans_amt)
        rp_status = "success" if txres.status_code == 200 else "failed"

        update_table(email, rp_amount, 0, trans_amt, rate, wallet, "0", rp_status, "", "", "20251004","")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": "Red Envelope",
                "Amount": rp_amount,
                "TransactionStatus": rp_status
            }),
        }

    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }
def e20251005(email):
    # 1. Already claimed?
    userId = query_ledger(email)
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 2. Eligibility check (deposit condition)
    try:
        deposit_amount = fetch_deposit("2025-10-01;2025-10-04", userId)
        required_deposit = 500  # condition threshold, adjust as needed

        if not deposit_amount or deposit_amount < required_deposit:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }

    # 3. Pop prize from Redis
    
    prize_key = "2005mid:1005"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:
        
        update_table(email, 0, 0, 0, rate, "", "", "", "", "email", "20251005", prize)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": prize,
                "TransactionStatus": "email"
            }),
        }

    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }
def e20251006(email):
            # 1. Already claimed?
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 3. Pop prize from Redis
    
    prize_key = "2005mid:1006"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:
        rp_amount = float(prize)
        trans_amt = round(rp_amount / rate, 2)
        userId = query_ledger(email)
        wallet = fetch_wallet(userId)
        txres=transfer_meoney_wallet(wallet, trans_amt)
        rp_status = "success" if txres.status_code == 200 else "failed"

        update_table(email, rp_amount, 0, trans_amt, rate, wallet, "0", rp_status, "", "", "20251006","")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": "Red Envelope",
                "Amount": rp_amount,
                "TransactionStatus": rp_status
            }),
        }

    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }
def e20251007(email):
    # 1. Already claimed?
    userId = query_ledger(email)
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 2. Eligibility check (deposit condition)
    try:

        if fetch_trade_data(userId, "2025-10-06;2025-10-06") == False:    
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {userId}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }

    # 3. Pop prize from Redis

    prize_key = "2005mid:1007"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:

        
        if prize != "京东E卡100":
            res =update_ledger(18, userId) #if updated return success or failed
            update_table(email, 0, 0, 18, rate, "", "", "", res, "", "20251007", prize)
            return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": prize,
                "TransactionStatus": res
            }),
        }
        else:
            update_table(email, 0, 0, 0, rate, "", "", "", "", "email", "20251007", prize)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Prize": prize,
                    "TransactionStatus": "email"
                }),
            }
        
    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }
def e20251008(email):
    # 1. Already claimed?
    userId = query_ledger(email)
    if query_event_rec(email, today) != 0:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "1",  # already claimed
                "Message": returnMsg.msg2
            }),
        }

    # 2. Eligibility check (deposit condition)
    try:

        if fetch_trade_data(userId, "2025-10-07;2025-10-07") == False:    
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Status": "2",  # not eligible
                    "Message": returnMsg.msg4
                }),
            }
    except Exception as e:
        print(f"Error checking deposits for {userId}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Unable to verify eligibility"
            }),
        }

    # 3. Pop prize from Redis

    prize_key = "2005mid:1008"
    prize = redis.rpop(prize_key)

    if not prize:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "Status": "3",
                "Message": returnMsg.msg6
            }),
        }

    # 4. Process prize
    try:

        
        if prize != "京东E卡100":
            res =update_ledger(18, userId) #if updated return success or failed
            update_table(email, 0, 0, 18, rate, "", "", "", res, "", "20251008", prize)
            return {
            "statusCode": 200,
            "body": json.dumps({
                "Prize": prize,
                "TransactionStatus": res
            }),
        }
        else:
            update_table(email, 0, 0, 0, rate, "", "", "", "", "email", "20251008", prize)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "Prize": prize,
                    "TransactionStatus": "email"
                }),
            }
        
    except Exception as e:
        print(f"Error processing prize for {email}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "Status": "error",
                "Message": "Internal error while processing prize"
            }),
        }                         
def lambda_handler(event, context):
    
    payload=json.loads(event["body"])
    eventname = payload.get('event_name')
    email = payload.get('email')
    if event["queryStringParameters"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Request query string is empty",
                # "location": ip.text.replace("\n", "")
            }),
        }
    else:
        params = event.get("queryStringParameters") or {}
        funcname= params.get("funcname")
        eventdate= params.get("eventdate")    
    match funcname:
        case "reset":
            manage_event('all')
        case "getBalance":
            manage_event('check')
        case "topup_event":
            manage_event(eventdate)
        case "e20251001":
            e20251001(email)
        case "e20251002":
            e20251002(email)
        case "e20251003":
            e20251003(email)
        case "e20251004":
            e20251004(email)
        case "e20251005":
            e20251005(email)
        case "e20251006":
            e20251006(email)
        case "e20251007":
            e20251007(email)
        case "e20251008":
            e20251008(email)
    

