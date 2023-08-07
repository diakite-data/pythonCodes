

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.Scroll;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.ScrollHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class ElasticsearchScrollExample {

    public static void main(String[] args) throws IOException {
        // Specify the Elasticsearch host, port, and scheme
        String host = "10.242.71.159";
        int port = 9200;
        String scheme = "http";

        // Create the Elasticsearch client instance
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, scheme)));

        // Define the scroll size and duration
        int scrollSize = 100;
        TimeValue scrollDuration = TimeValue.timeValueMinutes(1);

        // Create the initial search request with scroll
        SearchRequest searchRequest = new SearchRequest("random_commissions_fixes_v2_20230501");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(scrollSize);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(scrollDuration);

        // Execute the initial search request
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        ScrollHits scrollHits = searchResponse.getHits();

        // Process the initial batch of documents
        processDocuments(scrollHits);

        // Scroll through the remaining documents
        while (scrollHits != null && scrollHits.getHits().length > 0) {
            Scroll scroll = new Scroll(scrollDuration);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            scrollHits = searchResponse.getHits();
            processDocuments(scrollHits);
        }

        // Clear the scroll context
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        // Close the Elasticsearch client
        client.close();
    }

    private static void processDocuments(ScrollHits scrollHits) {
        // Process the documents in the scroll hits
        // ...
    }
}

