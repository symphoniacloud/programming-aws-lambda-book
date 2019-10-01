package book.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ObjectMessage;

import java.io.IOException;
import java.util.HashMap;
//import java.io.PrintWriter;
//import java.io.StringWriter;

public class WeatherEventLambda {

    private static Logger logger = LogManager.getLogger();

    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    public APIGatewayProxyResponseEvent handler(APIGatewayProxyRequestEvent request, Context context) throws IOException {

//        StringWriter stringWriter = new StringWriter();
//        Exception e = new Exception("Test exception");
//        e.printStackTrace(new PrintWriter(stringWriter));
//
//        System.err.println(String.format("System.err: %s", stringWriter.toString()));
//        context.getLogger().log(String.format("LambdaLogger: %s", stringWriter.toString()));
//
//        logger.info("Log4J logger");
//        logger.error("Log4J logger", e);

        final WeatherEvent weatherEvent = objectMapper.readValue(request.getBody(), WeatherEvent.class);

        final Table table = dynamoDB.getTable(tableName);
        final Item item = new Item()
                .withPrimaryKey("locationName", weatherEvent.locationName)
                .withDouble("temperature", weatherEvent.temperature)
                .withLong("timestamp", weatherEvent.timestamp)
                .withDouble("longitude", weatherEvent.longitude)
                .withDouble("latitude", weatherEvent.latitude);
        table.putItem(item);

        HashMap<Object, Object> message = new HashMap<>();
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