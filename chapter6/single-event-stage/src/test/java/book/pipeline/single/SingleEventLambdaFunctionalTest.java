package book.pipeline.single;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.stream.SystemOut;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class SingleEventLambdaFunctionalTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JodaModule())
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);

    @SystemStub
    private SystemOut systemOut;

    @Test
    public void testHandler() throws IOException {

        // Fixture SNS event
        SNSEvent snsEvent = objectMapper.readValue(getClass().getResourceAsStream("/sns_event.json"), SNSEvent.class);

        // Construct Lambda function class, and invoke handler
        SingleEventLambda lambda = new SingleEventLambda();
        lambda.handler(snsEvent);

        assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}\n"
                , systemOut.getText());
    }

    @Test
    public void testHandlerNoJackson() {

        // Fixture SNS content, record, and event
        SNSEvent.SNS snsContent = new SNSEvent.SNS().withMessage("{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}");
        SNSEvent.SNSRecord snsRecord = new SNSEvent.SNSRecord().withSns(snsContent);
        SNSEvent snsEvent = new SNSEvent().withRecords(Collections.singletonList(snsRecord));

        // Construct Lambda function class, and invoke handler
        SingleEventLambda lambda = new SingleEventLambda();
        lambda.handler(snsEvent);

        assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}\n"
                , systemOut.getText());
    }

    @Test
    public void testBadData() throws IOException {

        // Fixture SNS event
        SNSEvent snsEvent = objectMapper.readValue(getClass().getResourceAsStream("/sns_event_bad_data.json"), SNSEvent.class);

        // Construct Lambda function class
        SingleEventLambda lambda = new SingleEventLambda();

        // Assert exception
        RuntimeException exception = assertThrows(RuntimeException.class, () -> lambda.handler(snsEvent));
        assertInstanceOf(InvalidFormatException.class, exception.getCause());
        assertTrue(exception.getMessage().contains("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\""));
    }

}
