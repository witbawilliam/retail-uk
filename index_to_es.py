from elasticsearch import Elasticsearch, helpers

from elasticsearch.helpers import bulk

from read_excel import read_retail_data



# Elasticsearch connection

es = Elasticsearch("http://localhost:9200")

index_name = "your_invoice_index"

if not es.ping():

 raise Exception("Clould not connect to Elasticsearch")

print(" Sucessfully connect to elasticsearch ")


# Read data from excel

df = read_retail_data()


# convert dataframe rows to dics

records = df.to_dict(orient="records")


# prepare action for bulk indexing

actions = [

    {

    "_index": "your_invoice_index",
     
     "_source": record

    }

    for record in records
]

# bulk index

try:
 
 success, errors = helpers.bulk(es, 
                                
actions, raise_on_error=False )
 
 print("Inserted:", success)

 if errors:
  
  print("Errors:")

  for err in errors:
   
   print(err)

except Exception as e:

    print("bulk insert failed:", str(e))

   

