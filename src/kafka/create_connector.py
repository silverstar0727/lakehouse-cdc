#!/usr/bin/env python3
"""
Hard-coded PostgreSQL Debezium Connector creation script
"""

import requests
import json
import sys

import os

EXTERNAL_IP = os.getenv("EXTERNAL_IP")

# All configuration settings hard-coded
CONNECT_URL = f"http://kafka-connect.{EXTERNAL_IP}.nip.io"  # Kafka Connect URL
CONNECTOR_CONFIG = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "backend-postgres.default.svc.cluster.local",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "12341234",
        "database.dbname": "postgres",
        "database.server.name": "backend-postgres-server",
        "topic.prefix": "postgres-connector",
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "filtered",
        "slot.name": "debezium_slot",
        "table.include.list": "public.items",
        "snapshot.mode": "initial",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite"
    }
}

# Connector creation function
def create_connector():
    headers = {"Content-Type": "application/json"}
    
    try:
        # Check existing connectors (to prevent name conflicts)
        try:
            response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
            if response.status_code == 200:
                print(f"Connector '{CONNECTOR_CONFIG['name']}' already exists. Deleting and recreating.")
                # Delete existing connector
                requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
        except:
            pass  # Ignore errors during verification and continue
        
        # Request connector creation
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG)
        )
        
        # Check response
        if response.status_code == 201 or response.status_code == 200:
            print(f"Connector '{CONNECTOR_CONFIG['name']}' successfully created.")
            print(json.dumps(response.json(), indent=2))
            return 0
        else:
            print(f"Connector creation failed: HTTP {response.status_code}")
            print(response.text)
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
        return 1

if __name__ == "__main__":
    # Execute script
    exit_code = create_connector()
    sys.exit(exit_code)