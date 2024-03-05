import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration("localhost", 9200, "")) {

            String index = "bulkindex";
            String filePath = "C:\\Users\\mayfarm\\Desktop\\es\\chatbot_data.json";

            List<Map<String, Object>> jsonData = readJsonFile(filePath);

            // 데이터 벌크 삽입
            bulkInsert(elasticConfiguration, index, jsonData);

            // 예제로 몇 개의 문서를 가져와서 출력
            retrieveAndPrintDocuments(elasticConfiguration, index, 5);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> readJsonFile(String filePath) throws IOException {
        // JSON 파일을 읽어와서 List<Map<String, Object>> 형태로 반환
        String jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(jsonContent, List.class);
    }

    private static void bulkInsert(ElasticConfiguration elasticConfiguration, String index, List<Map<String, Object>> jsonData) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        for (Map<String, Object> data : jsonData) {
            String id = data.get("id").toString();
            IndexRequest indexRequest = new IndexRequest(index).id(id)
                    .source(data, XContentType.JSON);

            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
            System.out.println("Bulk insert failed: " + bulkResponse.buildFailureMessage());
        }
    }

    private static void retrieveAndPrintDocuments(ElasticConfiguration elasticConfiguration, String index, int numDocuments) throws IOException {
        // 예제로 몇 개의 문서를 가져와서 출력
        for (int i = 1; i <= numDocuments; i++) {
            GetResponse getResponse = elasticConfiguration.getElasticClient().get(new GetRequest(index, String.valueOf(i)), RequestOptions.DEFAULT);
            Map<String, Object> retrievedData = getResponse.getSourceAsMap();
            System.out.println("Retrieved data for document " + i + ": " + retrievedData);
        }
    }
}
