

from elasticsearch import Elasticsearch

# Create an Elasticsearch client instance
host = '10.242.71.159'
port = 9200
scheme = 'http'

# Create an Elasticsearch client instance
es = Elasticsearch(hosts=[{'host': host, 'port': port, 'scheme': scheme}])

# Start the initial search request with scroll
scroll_size = 100
response = es.search(
    index='random_commissions_fixes_v2_20230501',
    body={
        "query": {
            "match_all": {}
        },
        "size": scroll_size,
        "scroll": "1m"
    }
)

# Retrieve the scroll ID and initial search results
scroll_id = response['_scroll_id']
total_hits = response['hits']['total']['value']
scroll_documents = response['hits']['hits']

# Process the initial batch of documents
for doc in scroll_documents:
    # Do something with the document
    print(doc['_source'])

# Scroll through the remaining documents
while len(scroll_documents) > 0:
    response = es.scroll(
        scroll_id=scroll_id,
        scroll='1m'
    )
    scroll_documents = response['hits']['hits']
    scroll_id = response['_scroll_id']

    # Process the batch of documents
    for doc in scroll_documents:
        # Do something with the document
        print(doc['_source'])

# Clear the scroll context
es.clear_scroll(scroll_id=scroll_id)


