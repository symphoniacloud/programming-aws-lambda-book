package book.api;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class WeatherQueryLambda {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbClient dynamoDB = DynamoDbClient.create();
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    private static final String DEFAULT_LIMIT = "50";

    public APIGatewayProxyResponseEvent handler(APIGatewayProxyRequestEvent request) throws IOException {
        final String limitParam = request.getQueryStringParameters() == null
                ? DEFAULT_LIMIT
                : request.getQueryStringParameters().getOrDefault("limit", DEFAULT_LIMIT);
        final int limit = Integer.parseInt(limitParam);

        final ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(limit)
                .build();
        final ScanResponse scanResponse = dynamoDB.scan(scanRequest);

        final List<WeatherEvent> events = scanResponse.items().stream()
                .map(item -> new WeatherEvent(
                        item.get("locationName").s(),
                        Double.parseDouble(item.get("temperature").n()),
                        Long.parseLong(item.get("timestamp").n()),
                        Double.parseDouble(item.get("longitude").n()),
                        Double.parseDouble(item.get("latitude").n())
                ))
                .collect(Collectors.toList());

        final String json = objectMapper.writeValueAsString(events);

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(json);
    }
}
