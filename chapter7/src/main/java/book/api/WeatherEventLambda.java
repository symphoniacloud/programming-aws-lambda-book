package book.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ObjectMessage;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WeatherEventLambda {

    private static Logger logger = LogManager.getLogger();

    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final DynamoDbClient dynamoDB = DynamoDbClient.create();
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    public APIGatewayProxyResponseEvent handler(APIGatewayProxyRequestEvent request, Context context) throws IOException {

        var weatherEvent = objectMapper.readValue(request.getBody(), WeatherEvent.class);

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

        var message = new HashMap<Object, Object>();
        message.put("action", "record");
        message.put("locationName", weatherEvent.locationName);
        message.put("temperature", weatherEvent.temperature);
        message.put("timestamp", weatherEvent.timestamp);

        logger.info(new ObjectMessage(message));

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(weatherEvent.locationName);
    }
}
