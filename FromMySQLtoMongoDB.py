import pymongo
from pymongo import MongoClient, ReplaceOne
import mysql.connector

# ✅ Connect to MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Hotchoco2005",
    database="test"
)
mysql_cursor = mysql_conn.cursor(dictionary=True)

# ✅ Connect to MongoDB
mongo_conn = MongoClient('localhost', 27017)
mongo_db = mongo_conn["sale"]
mongo_orders = mongo_db["orders"]

# ✅ Fetch all orders
mysql_cursor.execute("SELECT * FROM orders  WHERE order_id > 999999 AND order_id < 15000000")
orders = mysql_cursor.fetchall()
print("✅ Orders fetched")

# ✅ Fetch all consumers at once
mysql_cursor.execute("SELECT * FROM consumers")
consumers = {str(c["consumer_id"]): c for c in mysql_cursor.fetchall()}
print("✅ Consumers fetched")

# ✅ Fetch all order details at once
mysql_cursor.execute("""
    SELECT od.order_id, od.product_id, od.quantity, od.subtotal, 
           p.name, p.category, p.priceEach 
    FROM orderdetails od 
    JOIN products p ON od.product_id = p.product_id
""")

# Group order details by order_id
order_details_map = {}
for detail in mysql_cursor.fetchall():
    order_id = str(detail["order_id"])
    if order_id not in order_details_map:
        order_details_map[order_id] = []

    order_details_map[order_id].append({
        "product_id": str(detail["product_id"]),
        "name": detail["name"],
        "category": detail["category"],
        "price_each": float(detail["priceEach"]),
        "quantity": detail["quantity"],
        "subtotal": float(detail["subtotal"])
    })
print("✅ Order details fetched and grouped")

# ✅ Prepare bulk operations
bulk_operations = []

for order in orders:
    order_id = str(order["order_id"])
    consumer_id = str(order["consumer_id"])

    # ✅ Ensure consumer exists
    consumer = consumers.get(consumer_id)
    if not consumer:
        print(f"⚠️ Skipping Order {order_id}: Consumer {consumer_id} not found.")
        continue

    # ✅ Create the order document
    order_doc = {
        "_id": order_id,
        "consumer_info": {
            "consumer_id": consumer_id,
            "name": consumer["name"],
            "phone": consumer["phone"],
            "address": consumer["address"]
        },
        "order_details": order_details_map.get(order_id, []),
        "order_date": str(order["order_date"]),
        "status": order["status"],
        "payment_method": order["payment_method"],
        "totalPayment": float(order["totalPayment"])
    }

    # ✅ Use ReplaceOne to handle duplicates
    bulk_operations.append(ReplaceOne({"_id": order_id}, order_doc, upsert=True))

# ✅ Execute bulk write in batches
if bulk_operations:
    batch_size = 1000  # Adjust batch size for large data
    for i in range(0, len(bulk_operations), batch_size):
        mongo_orders.bulk_write(bulk_operations[i:i + batch_size])
        print(f"✅ Inserted {i + batch_size} records into MongoDB.")

# ✅ Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_conn.close()

print("🎉 Data export completed successfully!")
