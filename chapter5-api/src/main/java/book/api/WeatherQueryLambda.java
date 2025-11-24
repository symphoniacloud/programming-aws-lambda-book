package book.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WeatherQueryLambda {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbClient dynamoDB = DynamoDbClient.create();
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    private static final String DEFAULT_LIMIT = "50";

    public ApiGatewayResponse handler(ApiGatewayRequest request) throws IOException {
        final var limitParam = request.queryStringParameters == null
                ? DEFAULT_LIMIT
                : request.queryStringParameters.getOrDefault("limit", DEFAULT_LIMIT);
        final var limit = Integer.parseInt(limitParam);

        final var scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(limit)
                .build();
        final var scanResponse = dynamoDB.scan(scanRequest);

        final var events = scanResponse.items().stream()
                .map(item -> new WeatherEvent(
                        item.get("locationName").s(),
                        Double.parseDouble(item.get("temperature").n()),
                        Long.parseLong(item.get("timestamp").n()),
                        Double.parseDouble(item.get("longitude").n()),
                        Double.parseDouble(item.get("latitude").n())
                ))
                .collect(Collectors.toList());

        final var json = objectMapper.writeValueAsString(events);

        return new ApiGatewayResponse(200, json);
    }
}
