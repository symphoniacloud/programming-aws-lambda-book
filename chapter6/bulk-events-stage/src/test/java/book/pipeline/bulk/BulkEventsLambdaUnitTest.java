package book.pipeline.bulk;

import book.pipeline.common.WeatherEvent;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class BulkEventsLambdaUnitTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public EnvironmentVariables environment = new EnvironmentVariables();

    @Before
    public void before() {
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, "test-topic");
    }

    @Test
    public void testReadWeatherEvents() {

        // Fixture data
        InputStream inputStream = getClass().getResourceAsStream("/bulk_data.json");

        // Construct Lambda function class, and invoke
        BulkEventsLambda lambda = new BulkEventsLambda(null, null);
        List<WeatherEvent> weatherEvents = lambda.readWeatherEvents(inputStream);

        // Assert
        Assert.assertEquals(3, weatherEvents.size());

        Assert.assertEquals("Brooklyn, NY", weatherEvents.get(0).locationName);
        Assert.assertEquals(91.0, weatherEvents.get(0).temperature, 0.0);
        Assert.assertEquals(1564428897L, weatherEvents.get(0).timestamp, 0);
        Assert.assertEquals(40.7, weatherEvents.get(0).latitude, 0.0);
        Assert.assertEquals(-73.99, weatherEvents.get(0).longitude, 0.0);

        Assert.assertEquals("Oxford, UK", weatherEvents.get(1).locationName);
        Assert.assertEquals(64.0, weatherEvents.get(1).temperature, 0.0);
        Assert.assertEquals(1564428897L, weatherEvents.get(1).timestamp, 0);
        Assert.assertEquals(51.75, weatherEvents.get(1).latitude, 0.0);
        Assert.assertEquals(-1.25, weatherEvents.get(1).longitude, 0.0);

        Assert.assertEquals("Charlottesville, VA", weatherEvents.get(2).locationName);
        Assert.assertEquals(87.0, weatherEvents.get(2).temperature, 0.0);
        Assert.assertEquals(1564428897L, weatherEvents.get(2).timestamp, 0);
        Assert.assertEquals(38.02, weatherEvents.get(2).latitude, 0.0);
        Assert.assertEquals(-78.47, weatherEvents.get(2).longitude, 0.0);
    }

    @Test
    public void testReadWeatherEventsBadData() {

        // Fixture data
        InputStream inputStream = getClass().getResourceAsStream("/bad_data.json");

        // Expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.instanceOf(InvalidFormatException.class));
        thrown.expectMessage("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\": not a valid Long value");

        // Invoke
        BulkEventsLambda lambda = new BulkEventsLambda(null, null);
        lambda.readWeatherEvents(inputStream);
    }

    @Test
    public void testWeatherEventToSnsMessage() {
        WeatherEvent weatherEvent = new WeatherEvent();
        weatherEvent.locationName = "Foo, Bar";
        weatherEvent.latitude = 100.0;
        weatherEvent.longitude = -100.0;
        weatherEvent.temperature = 32.0;
        weatherEvent.timestamp = 0L;

        BulkEventsLambda lambda = new BulkEventsLambda(null, null);
        String message = lambda.weatherEventToSnsMessage(weatherEvent);

        Assert.assertEquals(
                "{\"locationName\":\"Foo, Bar\",\"temperature\":32.0,\"timestamp\":0,\"longitude\":-100.0,\"latitude\":100.0}"
                , message);
    }

}
