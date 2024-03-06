import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Search {
    public static void main(String[] args) {
        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration("localhost", 9200, "")) {
            RestHighLevelClient client = elasticConfiguration.getElasticClient();

            // 검색 요청을 만들기
            SearchRequest searchRequest = new SearchRequest("news_articles");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.source(sourceBuilder);

            // 검색 실행 및 결과 얻기
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<NewsArticle> newsArticles = new ArrayList<>();

            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsArticle newsArticle = new NewsArticle(sourceAsMap);
                newsArticles.add(newsArticle);
            }

            StringBuilder result = new StringBuilder();

            for (NewsArticle newsArticle : newsArticles) {
                result.append(newsArticle.title).append("/")
                        .append(newsArticle.writer).append("/")
                        .append(newsArticle.writeDate).append("/")
                        .append(newsArticle.categoryOneDepth).append("/")
                        .append(newsArticle.categoryTwoDepth).append("/")
                        .append(newsArticle.url).append("/")
                        .append(newsArticle.thumbnailUrl).append("/")
                        .append(newsArticle.contents).append("/")
                        .append("\n\n");
            }
            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class NewsArticle implements Comparable<NewsArticle> {

    public String title;
    public String writer;
    public String writeDate;
    public String categoryOneDepth;
    public String categoryTwoDepth;
    public String url;
    public String thumbnailUrl;
    public String contents;

    public NewsArticle(Map<String, Object> sourceAsMap) {
        this.title = String.valueOf(sourceAsMap.get("title"));
        this.writer = String.valueOf(sourceAsMap.get("writer"));
        this.writeDate = String.valueOf(sourceAsMap.get("write_date"));
        this.categoryOneDepth = String.valueOf(sourceAsMap.get("category_one_depth"));
        this.categoryTwoDepth = String.valueOf(sourceAsMap.get("category_two_depth"));
        this.url = String.valueOf(sourceAsMap.get("url"));
        this.thumbnailUrl = String.valueOf(sourceAsMap.get("thumbnail_url"));
        this.contents = String.valueOf(sourceAsMap.get("contents"));
    }

    @Override
    public int compareTo(NewsArticle article) {

        if (this.writeDate.equals(article.writeDate)) {
            return this.title.compareTo(article.title);
        }
        return this.writeDate.compareTo(article.writeDate);
    }
}
