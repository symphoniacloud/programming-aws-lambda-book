package book.pipeline.single;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class SingleEventLambdaFunctionalTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JodaModule())
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);

    @Rule
    public SystemOutRule systemOutRule = new SystemOutRule().enableLog().muteForSuccessfulTests();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testHandler() throws IOException {

        // Fixture SNS event
        SNSEvent snsEvent = objectMapper.readValue(getClass().getResourceAsStream("/sns_event.json"), SNSEvent.class);

        // Construct Lambda function class, and invoke handler
        SingleEventLambda lambda = new SingleEventLambda();
        lambda.handler(snsEvent);

        Assert.assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}\n"
                , systemOutRule.getLog());
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

        Assert.assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}\n"
                , systemOutRule.getLog());
    }

    @Test
    public void testBadData() throws IOException {

        // Fixture SNS event
        SNSEvent snsEvent = objectMapper.readValue(getClass().getResourceAsStream("/sns_event_bad_data.json"), SNSEvent.class);

        // Expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.instanceOf(InvalidFormatException.class));
        thrown.expectMessage("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\": not a valid Long value");

        // Construct Lambda function class, and invoke handler
        SingleEventLambda lambda = new SingleEventLambda();
        lambda.handler(snsEvent);
    }

}
