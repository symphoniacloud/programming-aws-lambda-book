package book.pipeline.bulk;

import book.pipeline.common.WeatherEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class BulkEventsLambda {
    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                            false);
    private final SnsClient sns = SnsClient.create();
    private final S3Client s3 = S3Client.create();
    private final String snsTopic = System.getenv("FAN_OUT_TOPIC");

    public void handler(S3Event event) {
        event.getRecords().forEach(this::processS3EventRecord);
    }

    private void processS3EventRecord(S3EventNotification.S3EventNotificationRecord record) {
        final List<WeatherEvent> weatherEvents = readWeatherEventsFromS3(
                record.getS3().getBucket().getName(),
                record.getS3().getObject().getKey());

        weatherEvents.stream()
                .map(this::weatherEventToSnsMessage)
                .forEach(message -> sns.publish(PublishRequest.builder()
                        .topicArn(snsTopic)
                        .message(message)
                        .build()));

        System.out.println("Published " + weatherEvents.size() + " weather events to SNS");
    }

    private List<WeatherEvent> readWeatherEventsFromS3(String bucket, String key) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            final InputStream s3is = s3.getObject(getObjectRequest);
            final WeatherEvent[] weatherEvents =
                    objectMapper.readValue(s3is, WeatherEvent[].class);
            s3is.close();
            return Arrays.asList(weatherEvents);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String weatherEventToSnsMessage(WeatherEvent weatherEvent) {
        try {
            return objectMapper.writeValueAsString(weatherEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
