package book.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WeatherEventLambda {
    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final DynamoDbClient dynamoDB = DynamoDbClient.create();
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    public ApiGatewayResponse handler(ApiGatewayRequest request) throws IOException {
        var weatherEvent = objectMapper.readValue(request.body, WeatherEvent.class);

        var item = new HashMap<String, AttributeValue>();
        item.put("locationName", AttributeValue.builder().s(weatherEvent.locationName).build());
        item.put("temperature", AttributeValue.builder().n(String.valueOf(weatherEvent.temperature)).build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(weatherEvent.timestamp)).build());
        item.put("longitude", AttributeValue.builder().n(String.valueOf(weatherEvent.longitude)).build());
        item.put("latitude", AttributeValue.builder().n(String.valueOf(weatherEvent.latitude)).build());

        var putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        dynamoDB.putItem(putItemRequest);

        return new ApiGatewayResponse(200, weatherEvent.locationName);
    }
}
