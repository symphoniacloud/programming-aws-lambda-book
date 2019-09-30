package book.pipeline.single;

import book.pipeline.common.WeatherEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SingleEventLambda {
    private final ObjectMapper objectMapper =
            new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public void handler(SNSEvent event) {
        event.getRecords().stream()
                .map(record -> record.getSNS().getMessage())
                .map(this::readWeatherEvent)
                .forEach(this::logWeatherEvent);
    }

    WeatherEvent readWeatherEvent(String message) {
        try {
            return objectMapper.readValue(message, WeatherEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void logWeatherEvent(WeatherEvent weatherEvent) {
        System.out.println("Received weather event:");
        System.out.println(weatherEvent);
    }
}
