from powerbot_client import Configuration, ApiClient, ContractApi

import os

API_KEY=os.environ["POWERBOT_API_KEY"]
CONTRACT_ID="NX_597024"
DELIVERY_AREA=os.environ["POWERBOT_DELIVERY_AREA"]
HOST_URL="https://<tenant>.powerbot.volue.com/<tenant>"
client = ApiClient(Configuration(api_key={"api_key_security": API_KEY}, host=HOST_URL))

result = ContractApi(client).get_orders(contract_id=CONTRACT_ID,
                                        delivery_area=DELIVERY_AREA,
                                        exchange="nordpool")

print(result)
