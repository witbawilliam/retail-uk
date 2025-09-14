import pandas as pd

from sqlalchemy import create_engine


df = pd.read_excel('Online Retail.xlsx')

print(df.head(), "\n")

#  automaticaly handle missing value

for col in df.columns:

    if df[col].dtype == "object":

      df[col] = df[col].fillna("Unknown")

    else:

        df[col] = df[col].fillna(0)


 # handle duplicate rows

df = df.drop_duplicates()


#  automaticaly handle data type

for col in df.columns:
   
   df[col] = pd.to_numeric(df[col], errors="coerce")

   if "date" in col.lower():
      
      df[col] = pd.to_datetime(df[col], errors="coerce")


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

df.to_excel("retail_cleaned.xlsx", index=False)

print("Cleaned file saved as retail_cleaned.xlsx")

username = "root"

password = "StrongPass123!"

host = "localhost"

database = "retail_uk"

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}/{database}")

df.to_sql("retail", con=engine, if_exists="append", index=False)

print("Data inserted into MYSQL table successfully")