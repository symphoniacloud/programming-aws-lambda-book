package book.pipeline.single;

import book.pipeline.common.WeatherEvent;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.stream.SystemOut;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class SingleEventLambdaUnitTest {

    @SystemStub
    private SystemOut systemOut;

    @Test
    public void testReadWeatherEvent() {
        String message = "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}";

        SingleEventLambda lambda = new SingleEventLambda();
        WeatherEvent weatherEvent = lambda.readWeatherEvent(message);

        assertEquals("Brooklyn, NY", weatherEvent.locationName);
        assertEquals(91.0, weatherEvent.temperature, 0.0);
        assertEquals(1564428897L, weatherEvent.timestamp, 0);
        assertEquals(40.7, weatherEvent.latitude, 0.0);
        assertEquals(-73.99, weatherEvent.longitude, 0.0);
    }

    @Test
    public void testReadWeatherEventBadData() {
        String message = "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":\"Wrong data type\",\"longitude\":-73.99,\"latitude\":40.7}";

        // Construct Lambda function class
        SingleEventLambda lambda = new SingleEventLambda();

        // Assert exception
        RuntimeException exception = assertThrows(RuntimeException.class, () -> lambda.readWeatherEvent(message));
        assertInstanceOf(InvalidFormatException.class, exception.getCause());
        assertTrue(exception.getMessage().contains("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\""));
    }

    @Test
    public void testLogWeatherEvent() {
        WeatherEvent weatherEvent = new WeatherEvent();
        weatherEvent.locationName = "Foo, Bar";
        weatherEvent.latitude = 100.0;
        weatherEvent.longitude = -100.0;
        weatherEvent.temperature = 32.0;
        weatherEvent.timestamp = 0L;

        SingleEventLambda lambda = new SingleEventLambda();
        lambda.logWeatherEvent(weatherEvent);

        assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Foo, Bar', temperature=32.0, timestamp=0, longitude=-100.0, latitude=100.0}\n"
                , systemOut.getText());
    }

}
