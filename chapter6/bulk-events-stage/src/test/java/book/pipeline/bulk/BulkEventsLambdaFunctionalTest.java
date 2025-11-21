package book.pipeline.bulk;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class BulkEventsLambdaFunctionalTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);

    @SystemStub
    private EnvironmentVariables environment;

    @Test
    public void testHandler() throws IOException {

        // Set up mock AWS SDK clients
        SnsClient mockSNS = Mockito.mock(SnsClient.class);
        S3Client mockS3 = Mockito.mock(S3Client.class);

        // Fixture S3 event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event.json"), S3Event.class);
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        // Fixture S3 return value
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                getClass().getResourceAsStream(String.format("/%s", key))
        );
        Mockito.when(mockS3.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(responseInputStream);

        // Mock SNS publish
        Mockito.when(mockSNS.publish(Mockito.any(PublishRequest.class)))
                .thenReturn(PublishResponse.builder().build());

        // Fixture environment
        String topic = "test-topic";
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, topic);

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSNS, mockS3);
        lambda.handler(s3Event);

        // Capture outbound SNS messages
        ArgumentCaptor<PublishRequest> publishRequests = ArgumentCaptor.forClass(PublishRequest.class);
        Mockito.verify(mockSNS, Mockito.times(3)).publish(publishRequests.capture());

        // Assert
        assertEquals(3, publishRequests.getAllValues().size());
        assertEquals(topic, publishRequests.getAllValues().get(0).topicArn());
        assertEquals(topic, publishRequests.getAllValues().get(1).topicArn());
        assertEquals(topic, publishRequests.getAllValues().get(2).topicArn());
        assertArrayEquals(new String[]{
                "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}",
                "{\"locationName\":\"Oxford, UK\",\"temperature\":64.0,\"timestamp\":1564428898,\"longitude\":-1.25,\"latitude\":51.75}",
                "{\"locationName\":\"Charlottesville, VA\",\"temperature\":87.0,\"timestamp\":1564428899,\"longitude\":-78.47,\"latitude\":38.02}"
        }, publishRequests.getAllValues().stream().map(PublishRequest::message).toArray());
    }

    @Test
    public void testBadData() throws IOException {

        // Set up mock AWS SDK clients
        SnsClient mockSNS = Mockito.mock(SnsClient.class);
        S3Client mockS3 = Mockito.mock(S3Client.class);

        // Fixture S3 event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event_bad_data.json"), S3Event.class);
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        // Fixture S3 return value
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                getClass().getResourceAsStream(String.format("/%s", key))
        );
        Mockito.when(mockS3.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(responseInputStream);

        // Fixture environment
        String topic = "test-topic";
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, topic);

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSNS, mockS3);

        // Assert exception
        RuntimeException exception = assertThrows(RuntimeException.class, () -> lambda.handler(s3Event));
        assertInstanceOf(InvalidFormatException.class, exception.getCause());
        assertTrue(exception.getMessage().contains("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\""));
    }

    @Test
    public void testBadEnvironment() throws IOException {

        // Set up mock AWS SDK clients
        SnsClient mockSNS = Mockito.mock(SnsClient.class);
        S3Client mockS3 = Mockito.mock(S3Client.class);

        // Do *not* fixture environment

        // Assert exception
        RuntimeException exception = assertThrows(RuntimeException.class, () -> new BulkEventsLambda(mockSNS, mockS3));
        assertTrue(exception.getMessage().contains("FAN_OUT_TOPIC must be set"));
    }
}
