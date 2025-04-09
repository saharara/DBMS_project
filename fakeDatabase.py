from datetime import date

import mysql.connector
from faker import Faker
import random

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Hotchoco2005",
    database="test"
)
mycursor = db.cursor()
f = Faker(["vi_VN"])

#close db
def close_db():
    mycursor.close()
    db.close()


def generate_consumers(numbersOfRecord):
    # Generate all data first
    data = []
    sql = "INSERT INTO consumers(name, address, phone) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name), address=VALUES(address)"
    for i in range(numbersOfRecord):
        name1 = f.last_name() + ' ' + f.first_name_male()
        name2 = f.last_name() + ' ' + f.first_name_female()
        names = [name1, name2]
        name = random.choice(names)
        addr = f.administrative_unit()
        phone = "0" + str(random.randint(100000000, 999999999))

        data.append((name, addr, phone))

    # Batch insert (smaller chunks to avoid lost connection)
    batch_size = 100000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        mycursor.executemany(sql, batch)
        db.commit()
        print(f"Inserted {i + len(batch)} rows so far...")

    print("Total inserted:", mycursor.rowcount)

    close_db()


def generate_orders(numbersOfRecord):
    data = []
    sql_consumerId = "SELECT consumer_id FROM consumers ORDER BY RAND() LIMIT %s;"

    sql_findID = ("SELECT consumer_id FROM consumers "
                  "WHERE NOT EXISTS ( SELECT 1 FROM orders WHERE orders.consumer_id = consumers.consumer_id) "
                  "ORDER BY RAND() LIMIT %s")

    status = ['In process','Shipped', 'Cancelled']
    payment_method = ['Cash', 'Credit card', 'Debit card']

    consumerId = ""
    st = ""
    pm = ""
    od = date.today()

    sql = "INSERT INTO orders(consumer_id, order_date, status, payment_method) VALUES (%s, %s, %s, %s)"

    mycursor.execute(sql_consumerId, (numbersOfRecord,))
    consumer_ids = mycursor.fetchall()  # List of tuples [(id1,), (id2,), ...]

    for consumer in consumer_ids:
        consumerId = consumer[0]  # Extract the consumer_id
        st = random.choice(status)
        pm = random.choice(payment_method)
        od = f.date_between(start_date="-5y", end_date="+2m")

        data.append((consumerId, od, st, pm))

    batch_size = 10000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        mycursor.executemany(sql, batch)
        db.commit()
        print(f"Inserted {i + len(batch)} rows so far...")

    #close_db()


import random


def generate_orderdetails():
    print("check1")
    data = []

    #sql_orderId = "SELECT order_id FROM orders WHERE order_id > '1510192' ORDER BY order_id ASC;"
    sql_productId = "SELECT product_id FROM products;"

    # Fetch order IDs
    mycursor.execute(sql_productId)
    orders_id = mycursor.fetchall()
    print("Orders ID fetched:", orders_id[:5])  # Debugging check

    # Fetch all product IDs once and store them in memory
    mycursor.execute(sql_productId)
    products_id = [row[0] for row in mycursor.fetchall()]
    print("Products ID fetched:", products_id[:5])  # Debugging check

    if not products_id:
        print("No products found in the database.")
        return

    # SQL insert query
    sql = """INSERT INTO orderdetails (order_id, product_id, quantity)
             VALUES (%s, %s, %s)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity);"""

    print("check3")

    for (orderId,) in orders_id:  # Unpacking to avoid tuple issue
        num_products = random.randint(1, 10)  # Number of products per order
        selected_products = random.sample(products_id, min(num_products, len(products_id)))  # Avoid duplicates
        print(f"Processing order ID: {orderId}, assigning {num_products} products")

        for productId in selected_products:
            quantity = random.randint(1, 10)
            data.append((orderId, productId, quantity))

    # Debugging: Print sample data before insert
    print("Sample data to be inserted:", data[:5])

    # Batch insert for performance
    batch_size = 10000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]

        if batch:  # Ensure batch is not empty
            mycursor.executemany(sql, batch)
            db.commit()
            print(f"Inserted {i + len(batch)} rows so far...")

    #close_db()



#generate_consumers(1000000)
#generate_orders()
#generate_orderdetails()
