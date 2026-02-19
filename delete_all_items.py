import os
import requests

# API settings
API_URL = os.environ.get("HOOD_API_URL", "https://www.hood.de/api.htm").strip()
USERNAME = os.environ.get("HOOD_API_USER", "").strip()
PASSWORD = os.environ.get("HOOD_API_PASSWORD", "").strip()  # usually MD5 password hash if required

if not USERNAME or not PASSWORD:
    raise ValueError("HOOD_API_USER and HOOD_API_PASSWORD must be set")

start_id = 129262524
end_id = 129262678

items_xml = "".join([f"<item><itemID>{i}</itemID></item>" for i in range(start_id, end_id + 1)])

xml_payload = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<api type=\"public\" version=\"2.0.1\" user=\"{USERNAME}\" password=\"{PASSWORD}\">
    <function>itemDelete</function>
    <accountName>{USERNAME}</accountName>
    <accountPass>{PASSWORD}</accountPass>
    <items>
        {items_xml}
    </items>
</api>"""

headers = {"Content-Type": "application/xml"}
response = requests.post(API_URL, data=xml_payload.encode("utf-8"), headers=headers)

print(response.status_code)
print(response.text)
