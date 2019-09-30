package book.pipeline.single;

import book.pipeline.common.WeatherEvent;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

public class SingleEventLambdaUnitTest {

    @Rule
    public SystemOutRule systemOutRule = new SystemOutRule().enableLog().muteForSuccessfulTests();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testReadWeatherEvent() {
        String message = "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}";

        SingleEventLambda lambda = new SingleEventLambda();
        WeatherEvent weatherEvent = lambda.readWeatherEvent(message);

        Assert.assertEquals("Brooklyn, NY", weatherEvent.locationName);
        Assert.assertEquals(91.0, weatherEvent.temperature, 0.0);
        Assert.assertEquals(1564428897L, weatherEvent.timestamp, 0);
        Assert.assertEquals(40.7, weatherEvent.latitude, 0.0);
        Assert.assertEquals(-73.99, weatherEvent.longitude, 0.0);
    }

    @Test
    public void testReadWeatherEventBadData() {
        String message = "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":\"Wrong data type\",\"longitude\":-73.99,\"latitude\":40.7}";

        // Expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.instanceOf(InvalidFormatException.class));
        thrown.expectMessage("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\": not a valid Long value");

        // Invoke
        SingleEventLambda lambda = new SingleEventLambda();
        lambda.readWeatherEvent(message);
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

        Assert.assertEquals(
                "Received weather event:\nWeatherEvent{locationName='Foo, Bar', temperature=32.0, timestamp=0, longitude=-100.0, latitude=100.0}\n"
                , systemOutRule.getLog());
    }

}
