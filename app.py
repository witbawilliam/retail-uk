from flask import Flask, request, jsonify

from elasticsearch import Elasticsearch


app = Flask(__name__)

es = Elasticsearch("http://localhost:9200")

index_name = "your_invioce_index"

description_field = "Description"

@app.route('/search', methods=['GET'])

def search_endpoint():



    query_text = request.args.get('q', type=str)

    n_results = request.args.get('n', default=5, type=int)

    if not query_text:

        return jsonify({'error': 'query sring "q" is required'}), 400
    

    try:

        search_body = {

            "size": n_results,

            "query":{

                "match":{

                    description_field:query_text


                }
            }
        }

        res = es.search(index='your_invoice_index', body=search_body)

        results = [hit['_source'] for hit in res['hits']['hits']]

        return jsonify(results)
    
    except Exception as e:

        return jsonify({'error': str(e)}), 500
    

if __name__=='__main__':

    app.run(debug=True)



