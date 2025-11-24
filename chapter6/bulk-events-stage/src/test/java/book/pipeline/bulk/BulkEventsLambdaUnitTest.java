package book.pipeline.bulk;

import book.pipeline.common.WeatherEvent;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class BulkEventsLambdaUnitTest {

    @SystemStub
    private EnvironmentVariables environment;

    @BeforeEach
    public void before() {
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, "test-topic");
    }

    @Test
    public void testReadWeatherEvents() {

        // Fixture data
        var inputStream = getClass().getResourceAsStream("/bulk_data.json");

        // Construct Lambda function class, and invoke
        var lambda = new BulkEventsLambda(null, null);
        var weatherEvents = lambda.readWeatherEvents(inputStream);

        // Assert
        assertEquals(3, weatherEvents.size());

        assertEquals("Brooklyn, NY", weatherEvents.get(0).locationName);
        assertEquals(91.0, weatherEvents.get(0).temperature, 0.0);
        assertEquals(1564428897L, weatherEvents.get(0).timestamp, 0);
        assertEquals(40.7, weatherEvents.get(0).latitude, 0.0);
        assertEquals(-73.99, weatherEvents.get(0).longitude, 0.0);

        assertEquals("Oxford, UK", weatherEvents.get(1).locationName);
        assertEquals(64.0, weatherEvents.get(1).temperature, 0.0);
        assertEquals(1564428897L, weatherEvents.get(1).timestamp, 0);
        assertEquals(51.75, weatherEvents.get(1).latitude, 0.0);
        assertEquals(-1.25, weatherEvents.get(1).longitude, 0.0);

        assertEquals("Charlottesville, VA", weatherEvents.get(2).locationName);
        assertEquals(87.0, weatherEvents.get(2).temperature, 0.0);
        assertEquals(1564428897L, weatherEvents.get(2).timestamp, 0);
        assertEquals(38.02, weatherEvents.get(2).latitude, 0.0);
        assertEquals(-78.47, weatherEvents.get(2).longitude, 0.0);
    }

    @Test
    public void testReadWeatherEventsBadData() {

        // Fixture data
        var inputStream = getClass().getResourceAsStream("/bad_data.json");

        // Construct Lambda function class
        var lambda = new BulkEventsLambda(null, null);

        // Assert exception
        var exception = assertThrows(RuntimeException.class, () -> lambda.readWeatherEvents(inputStream));
        assertInstanceOf(InvalidFormatException.class, exception.getCause());
        assertTrue(exception.getMessage().contains("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\""));
    }

    @Test
    public void testWeatherEventToSnsMessage() {
        var weatherEvent = new WeatherEvent();
        weatherEvent.locationName = "Foo, Bar";
        weatherEvent.latitude = 100.0;
        weatherEvent.longitude = -100.0;
        weatherEvent.temperature = 32.0;
        weatherEvent.timestamp = 0L;

        var lambda = new BulkEventsLambda(null, null);
        var message = lambda.weatherEventToSnsMessage(weatherEvent);

        assertEquals(
                "{\"locationName\":\"Foo, Bar\",\"temperature\":32.0,\"timestamp\":0,\"longitude\":-100.0,\"latitude\":100.0}"
                , message);
    }

}
