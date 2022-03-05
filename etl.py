import pymysql
import os
from dotenv import load_dotenv
import csv



def connect_to_db():

    load_dotenv()
    host = os.environ.get("mysql_host")
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    database = os.environ.get("mysql_db")

    connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

    return connection



def extract():


    connection = connect_to_db()
    cursor = connection.cursor()

    try:

        list_of_sales = []

        with open ('sales_data.csv', 'r', newline='' ) as file_content:
            data = csv.DictReader(file_content)
            for row in data:
                if '' not in row.values():
                    list_of_sales.append(row)

           
    except FileNotFoundError as err:
        print('Can\'t open the file')

    cursor.execute(''' CREATE TABLE if not exists Customers (
        customer_id int not null,
        purchase_date date,
        purchase_amount decimal (5, 2),
        product_id varchar (10)
        );
   
    ''')

    cursor.execute(''' truncate table Customers;   ''')
    
    connection.commit()

    for row in list_of_sales:
        cursor.execute(
                    ''' INSERT INTO Sales.Customers
                    (customer_id,purchase_date,purchase_amount, product_id)
                         VALUES ( %s,%s,%s, %s)''', (row.get('customer_id'), row.get('purchase_date'),
                         row.get('purchase_amount') , row.get('product_id')))
        connection.commit()

    os.system('cls')
    print('CSV has been transfered to Database.')
    cursor.close()
    connection.close()
    
       

def transform():


    connection1 = connect_to_db()
    cursor1 = connection1.cursor()

    
    cursor1.execute(''' CREATE TABLE IF NOT EXISTS Spending (
        customer_id int not null,
         total_spending decimal (5,2),
          average_spending decimal (5,2)
           );'''
           )

    cursor1.execute(''' truncate table Spending ;''')

    connection1.commit()


    query_1 = '''INSERT into Spending
    SELECT customer_id, sum(purchase_amount), avg(purchase_amount) 
    FROM Customers
    where purchase_date between '2020-12-01' and '2020-12-05'
    GROUP BY customer_id'''

    cursor1.execute(query_1)
    connection1.commit()
    cursor1.close()
    connection1.close()



    
    connection2 = connect_to_db()
    cursor2 = connection2.cursor()

    cursor2.execute(''' CREATE TABLE IF NOT EXISTS Customers_Products (
        customer_id int not null,
         product_id varchar (10),
          quantity int
           );'''
           )
    
    connection2.commit()

    cursor2.execute(' Truncate table Customers_Products;')
    connection2.commit()


    query_2 = '''INSERT into Customers_Products
    SELECT customer_id, product_id, count(product_id) 
    FROM Customers
    where purchase_date between '2020-12-01' and '2020-12-05'
    GROUP BY customer_id, product_id
    order by customer_id'''
    
    cursor2.execute(query_2)
    connection2.commit()
    
    connection2.close()
    cursor2.close()
  
   

def main():
    extract()
    transform()


main()
