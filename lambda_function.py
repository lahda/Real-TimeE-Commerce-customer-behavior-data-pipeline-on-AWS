import json
import boto3
import random
import uuid
from datetime import datetime

# ============================================================
#  Générateur d'événements Clickstream — E-commerce Pipeline
#  AWS Lambda Function
#  Publishes synthetic customer behavior events to Kinesis
# ============================================================

STREAM_NAME = "ecomm-pipeline-stream"
REGION      = "eu-west-1"          # ← adaptez à votre région

kinesis = boto3.client("kinesis", region_name=REGION)

# --- Données de référence -----------------------------------
PRODUCTS = [
    {"id": "prod_101", "name": "Laptop Pro 15",      "category": "Electronics", "base_price": 1299.99},
    {"id": "prod_102", "name": "Smartphone X12",     "category": "Electronics", "base_price": 799.00},
    {"id": "prod_103", "name": "Casque Bluetooth",   "category": "Electronics", "base_price": 129.99},
    {"id": "prod_104", "name": "Montre Connectée",   "category": "Electronics", "base_price": 249.00},
    {"id": "prod_201", "name": "Running Shoes Pro",  "category": "Sports",      "base_price": 89.99},
    {"id": "prod_202", "name": "Vélo de Route",      "category": "Sports",      "base_price": 599.00},
    {"id": "prod_203", "name": "Tapis de Yoga",      "category": "Sports",      "base_price": 34.99},
    {"id": "prod_301", "name": "T-Shirt Premium",    "category": "Fashion",     "base_price": 29.99},
    {"id": "prod_302", "name": "Jean Slim Fit",      "category": "Fashion",     "base_price": 59.99},
    {"id": "prod_303", "name": "Veste Cuir",         "category": "Fashion",     "base_price": 199.00},
    {"id": "prod_401", "name": "Lampe de Bureau",    "category": "Home",        "base_price": 49.99},
    {"id": "prod_402", "name": "Robot Cuiseur",      "category": "Home",        "base_price": 149.00},
]

COUNTRIES   = ["FR", "US", "DE", "GB", "ES", "IT", "BE", "CA", "AU", "JP"]
DEVICES     = ["mobile", "desktop", "tablet"]
BROWSERS    = ["Chrome", "Safari", "Firefox", "Edge"]
EVENT_TYPES = ["view", "click", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]

# Probabilités réalistes par type d'événement
EVENT_WEIGHTS = [0.40, 0.25, 0.18, 0.05, 0.08, 0.04]


def generate_event(user_id: str, session_id: str) -> dict:
    """Génère un événement clickstream réaliste."""
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)
    price    = round(product["base_price"] * random.uniform(0.85, 1.15), 2)

    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    return {
        "event_id":       str(uuid.uuid4()),
        "user_id":        user_id,
        "session_id":     session_id,
        "event_type":     event_type,
        "product_id":     product["id"],
        "product_name":   product["name"],
        "category":       product["category"],
        "price":          price,
        "quantity":       quantity,
        "total_amount":   round(price * quantity, 2),
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "page_url":       f"https://shop.example.com/product/{product['id']}",
        "referrer":       random.choice(["google", "direct", "email", "social", "affiliate"]),
        "device":         random.choice(DEVICES),
        "browser":        random.choice(BROWSERS),
        "country":        random.choice(COUNTRIES),
        "is_new_user":    random.random() < 0.30,     # 30 % nouveaux visiteurs
        "schema_version": "1.0",
    }


def publish_to_kinesis(events: list) -> dict:
    """
    Publie une liste d'événements dans Kinesis via put_records (batch).
    put_records envoie jusqu'à 500 records en un seul appel API → plus efficace.
    """
    records = [
        {
            "Data":         json.dumps(evt),
            "PartitionKey": evt["user_id"],   # même user → même shard → ordre garanti
        }
        for evt in events
    ]

    response = kinesis.put_records(
        Records=records,
        StreamName=STREAM_NAME,
    )

    failed = response.get("FailedRecordCount", 0)
    return {
        "total_sent":   len(records),
        "failed":       failed,
        "succeeded":    len(records) - failed,
    }


def lambda_handler(event, context):
    """
    Point d'entrée Lambda.

    Paramètres optionnels dans l'event JSON :
      - num_events  (int)  : nombre d'événements à générer (défaut : 10)
      - num_users   (int)  : nombre d'utilisateurs simulés  (défaut : 5)
    """
    num_events = int(event.get("num_events", 10))
    num_users  = int(event.get("num_users",  5))

    # Génère des utilisateurs et sessions simulés
    users    = [f"user_{random.randint(1000, 9999)}" for _ in range(num_users)]
    sessions = {u: str(uuid.uuid4()) for u in users}

    events_list = [
        generate_event(
            user_id    = random.choice(users),
            session_id = sessions[random.choice(users)],
        )
        for _ in range(num_events)
    ]

    result = publish_to_kinesis(events_list)

    print(
        f"[INFO] Kinesis publish result — "
        f"sent={result['total_sent']}, "
        f"ok={result['succeeded']}, "
        f"failed={result['failed']}"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":    f"{result['succeeded']} events published to Kinesis stream '{STREAM_NAME}'",
            "stream":     STREAM_NAME,
            "region":     REGION,
            "stats":      result,
            "sample_event": events_list[0],   # aperçu du premier événement généré
        }, indent=2),
    }
