import pandas as pd

from sqlalchemy import create_engine



df = pd.read_excel('Online Retail.xlsx')

print(df.head(), "\n")


  # handle missing values

df = df.dropna()

  # handle duplicate value

df.drop_duplicates(subset=[

  'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate',

  'UnitPrice', 'CustomerID', 'Country'

 ], keep="first", inplace=True)



df.to_excel("retail_cleaned.xlsx", index=False)

print("Cleaned file saved as retail_cleaned.xlsx")

db_user = "root"

db_password = "StrongPass123!"

db_host = "localhost"

db_port = 3306

db_name = "retail_uk"

table_name = "retail"

df_clean = pd.read_excel('retail_cleaned.xlsx')

print(df_clean.head())


FILE_PATH = 'retail_cleaned.xlsx'


connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(connection_string)


 # Extract rows from mysql database

df_check = pd.read_sql("SELECT * FROM retail LIMIT 10;", con=engine)

print(df_check)



def read_retail_data(FILE_PATH="retail_cleaned.xlsx"):

  try:

    
    df = pd.read_excel(FILE_PATH)

    return df

  except Exception as e:

    print(f"Error reading Excel file: {e}")

    return None   




 
try:

  df.columns = df.columns.str.strip().str.lower()

  df = df.rename(columns={

    'invoiceno': 'Invoice_No',

    'stockcode': 'Stock_Code',

    'description': 'Description',

    'quantity': 'Quantity',

    'invoicedate': 'Invoice_Date',

    'unitprice': 'Unit_Price',

    'customerid': 'Customer_ID',

    'country': 'Country',
  })

  print(df.columns)


  # handle datatypes

  df = df.convert_dtypes()

  print(df.dtypes)

  # Data from retail_clean.xlsx insert  into mysql database
      
  df.to_sql('retail', con=engine, if_exists='replace', index=False)

  print(f"Data from '{FILE_PATH}' inserted into MYSQL table '{table_name}'successfully")

except Exception as e:
   
  print(f"An error occured during inserting : {e}")



  # it close database connection
  
finally:

  if 'engine' in locals():

    engine.dispose()

    print('database connection closed.')



if __name__ == "__main__": 

  df = read_retail_data()

  print(" Testing read_excel.py directly")


