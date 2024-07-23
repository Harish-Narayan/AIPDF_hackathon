import os
import json
import google.auth

service_account_key_path = 'ai-drive-psg-2024-local-sa.json'
if not os.path.exists(service_account_key_path):
    raise FileNotFoundError(f"Service account key file not found: {service_account_key_path}")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path

# Print the current credentials info
credentials, project_id = google.auth.default()
print(f"Credentials: {credentials}")
print(f"Project ID: {project_id}")

# Print the content of the service account key file (optional for debugging)
with open(service_account_key_path, 'r') as f:
    key_content = json.load(f)
    print(json.dumps(key_content, indent=2))
