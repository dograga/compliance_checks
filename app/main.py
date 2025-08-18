from fastapi import FastAPI, HTTPException
from google.cloud import storage, bigquery, pubsub_v1
from googleapiclient import discovery
import google.auth
import structlog

logger = structlog.get_logger()
app = FastAPI()

credentials, _ = google.auth.default()

PUBLIC_IDENTITIES = {"allUsers", "allAuthenticatedUsers"}


def is_public_iam(policy):
    """Helper: check if IAM policy has public identities"""
    for binding in policy.get("bindings", []):
        for member in binding.get("members", []):
            if member in PUBLIC_IDENTITIES:
                return True
    return False


@app.get("/check/{project_id}")
async def check_public_access(project_id: str):
    result = {"ProjectID": project_id, "Services": []}

    try:
        # --- Cloud Storage ---
        logger.info("Starting Cloud Storage checks")
        storage_client = storage.Client(project=project_id, credentials=credentials)
        service_results = {"Service": "Cloud Storage", "Checks": []}
        for bucket in storage_client.list_buckets():
            policy = bucket.get_iam_policy(requested_policy_version=3)
            iam_public = is_public_iam(dict(policy))
            acl_public = any(
                entry["entity"] in ["allUsers", "allAuthenticatedUsers"]
                for entry in bucket.acl
            )

            service_results["Checks"].append({
                "Check": f"Bucket {bucket.name} IAM public",
                "Result": "Fail" if iam_public else "Pass"
            })
            service_results["Checks"].append({
                "Check": f"Bucket {bucket.name} ACL public",
                "Result": "Fail" if acl_public else "Pass"
            })
        result["Services"].append(service_results)
        logger.info("Cloud Storage checks completed")
        # --- Cloud SQL ---
        sqladmin = discovery.build("sqladmin", "v1", credentials=credentials)
        sql_resp = sqladmin.instances().list(project=project_id).execute()
        service_results = {"Service": "Cloud SQL", "Checks": []}
        for inst in sql_resp.get("items", []):
            public_ip = any(ip.get("type") == "PRIMARY" and not ip["ipAddress"].startswith("10.")
                            for ip in inst.get("ipAddresses", []))
            open_network = any(auth["value"] == "0.0.0.0/0"
                               for auth in inst.get("settings", {}).get("ipConfiguration", {}).get("authorizedNetworks", []))

            service_results["Checks"].append({
                "Check": f"Instance {inst['name']} has public IP",
                "Result": "Fail" if public_ip else "Pass"
            })
            service_results["Checks"].append({
                "Check": f"Instance {inst['name']} open to 0.0.0.0/0",
                "Result": "Fail" if open_network else "Pass"
            })
        result["Services"].append(service_results)
        logger.info("Cloud SQL checks completed")
        # --- Datastore / Firestore ---
        crm_service = discovery.build("cloudresourcemanager", "v3", credentials=credentials)
        policy = crm_service.projects().getIamPolicy(resource=f"projects/{project_id}", body={}).execute()

        datastore_public = is_public_iam(policy)
        result["Services"].append({
            "Service": "Datastore/Firestore",
            "Checks": [
                {"Check": "Project IAM public", "Result": "Fail" if datastore_public else "Pass"}
            ]
        })
        logger.info("Datastore/Firestore checks completed")
        # --- BigQuery ---
        bq_client = bigquery.Client(project=project_id, credentials=credentials)
        service_results = {"Service": "BigQuery", "Checks": []}
        for ds in bq_client.list_datasets():
            dataset = bq_client.get_dataset(ds.dataset_id)
            entries = dataset.access_entries
            public = any(entry.entity_id in PUBLIC_IDENTITIES for entry in entries)
            service_results["Checks"].append({
                "Check": f"Dataset {ds.dataset_id} IAM public",
                "Result": "Fail" if public else "Pass"
            })
        result["Services"].append(service_results)
        logger.info("BigQuery checks completed")
        # --- Pub/Sub ---
        pubsub_client = pubsub_v1.PublisherClient(credentials=credentials)
        service_results = {"Service": "Pub/Sub", "Checks": []}
        for topic in pubsub_client.list_topics(request={"project": f"projects/{project_id}"}):
            iam_policy = pubsub_client.get_iam_policy(request={"resource": topic.name})
            public = any(
                member in PUBLIC_IDENTITIES
                for binding in iam_policy.bindings
                for member in binding.members
            )
            service_results["Checks"].append({
                "Check": f"Topic {topic.name} IAM public",
                "Result": "Fail" if public else "Pass"
            })
        result["Services"].append(service_results)
        logger.info("Pub/Sub checks completed")
    except Exception as e:
        logger.error("Error occurred during compliance checks", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e))

    return result