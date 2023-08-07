
#!/bin/bash

# Elasticsearch configuration
ELASTICSEARCH_HOST="10.242.71.159"
ELASTICSEARCH_PORT="9200"
ELASTICSEARCH_INDEX="random_commissions_fixes_v2_20230501"
ELASTICSEARCH_SCROLL_SIZE=100
ELASTICSEARCH_SCROLL_DURATION="1m"

# Elasticsearch search query
SEARCH_QUERY='
{
  "size": '$ELASTICSEARCH_SCROLL_SIZE',
  "query": {
    "match_all": {}
  }
}'

# Perform initial search request
response=$(curl -X POST -H "Content-Type: application/json" -d "$SEARCH_QUERY" \
  "http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/$ELASTICSEARCH_INDEX/_search?scroll=$ELASTICSEARCH_SCROLL_DURATION")

# Extract the scroll ID and initial search results
scroll_id=$(echo "$response" | jq -r '._scroll_id')
total_hits=$(echo "$response" | jq -r '.hits.total.value')
scroll_documents=$(echo "$response" | jq -r '.hits.hits[]')

# Process the initial batch of documents
for doc in $scroll_documents; do
  # Do something with the document
  echo "$doc"
done

# Scroll through the remaining documents
while [[ $total_hits -gt 0 ]]; do
  response=$(curl -X POST -H "Content-Type: application/json" -d "{ \"scroll\": \"$ELASTICSEARCH_SCROLL_DURATION\", \"scroll_id\": \"$scroll_id\" }" \
    "http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_search/scroll")

  scroll_id=$(echo "$response" | jq -r '._scroll_id')
  scroll_documents=$(echo "$response" | jq -r '.hits.hits[]')

  # Process the batch of documents
  for doc in $scroll_documents; do
    # Do something with the document
    echo "$doc"
  done

  total_hits=$(echo "$response" | jq -r '.hits.total.value')
done

# Clear the scroll context
curl -X DELETE "http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_search/scroll" -d "{ \"scroll_id\": [\"$scroll_id\"] }"

