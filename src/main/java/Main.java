import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration("localhost", 9200, "")) {

            // 데이터 생성 (Index)
            String index = "indexindex";
            String id = "1";
            Map<String, Object> data = new HashMap<>();
            data.put("name", "John Doe");
            data.put("age", 30);

            IndexRequest indexRequest = new IndexRequest(index).id(id)
                    .source(data, XContentType.JSON);

            IndexResponse indexResponse = elasticConfiguration.getElasticClient().index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("Index created: " + indexResponse.getId());

            // 데이터 조회 (Get)
            GetResponse getResponse = elasticConfiguration.getElasticClient().get(new GetRequest(index, id), RequestOptions.DEFAULT);
            Map<String, Object> retrievedData = getResponse.getSourceAsMap();
            System.out.println("Retrieved data: " + retrievedData);

            // 데이터 수정 (Update)
            // (Update operation can be performed using the Update API)

            // 데이터 삭제 (Delete)
            // (Delete operation can be performed using the Delete API)

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
