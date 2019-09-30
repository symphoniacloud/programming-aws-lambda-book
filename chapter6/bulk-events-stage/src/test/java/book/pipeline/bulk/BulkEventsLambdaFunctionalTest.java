package book.pipeline.bulk;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;

public class BulkEventsLambdaFunctionalTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Rule
    public EnvironmentVariables environment = new EnvironmentVariables();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testHandler() throws IOException {

        // Set up mock AWS SDK clients
        AmazonSNS mockSNS = Mockito.mock(AmazonSNS.class);
        AmazonS3 mockS3 = Mockito.mock(AmazonS3.class);

        // Fixture S3 event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event.json"), S3Event.class);
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        // Fixture S3 return value
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(getClass().getResourceAsStream(String.format("/%s", key)));
        Mockito.when(mockS3.getObject(bucket, key)).thenReturn(s3Object);

        // Fixture environment
        String topic = "test-topic";
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, topic);

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSNS, mockS3);
        lambda.handler(s3Event);

        // Capture outbound SNS messages
        ArgumentCaptor<String> topics = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messages = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockSNS, Mockito.times(3)).publish(topics.capture(), messages.capture());

        // Assert
        Assert.assertArrayEquals(new String[]{topic, topic, topic}, topics.getAllValues().toArray());
        Assert.assertArrayEquals(new String[]{
                "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}",
                "{\"locationName\":\"Oxford, UK\",\"temperature\":64.0,\"timestamp\":1564428898,\"longitude\":-1.25,\"latitude\":51.75}",
                "{\"locationName\":\"Charlottesville, VA\",\"temperature\":87.0,\"timestamp\":1564428899,\"longitude\":-78.47,\"latitude\":38.02}"
        }, messages.getAllValues().toArray());
    }

    @Test
    public void testBadData() throws IOException {

        // Set up mock AWS SDK clients
        AmazonSNS mockSNS = Mockito.mock(AmazonSNS.class);
        AmazonS3 mockS3 = Mockito.mock(AmazonS3.class);

        // Fixture S3 event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event_bad_data.json"), S3Event.class);
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        // Fixture S3 return value
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(getClass().getResourceAsStream(String.format("/%s", key)));
        Mockito.when(mockS3.getObject(bucket, key)).thenReturn(s3Object);

        // Fixture environment
        String topic = "test-topic";
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, topic);

        // Expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.instanceOf(InvalidFormatException.class));
        thrown.expectMessage("Cannot deserialize value of type `java.lang.Long` from String \"Wrong data type\": not a valid Long value");

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSNS, mockS3);
        lambda.handler(s3Event);
    }

    @Test
    public void testBadEnvironment() throws IOException {

        // Set up mock AWS SDK clients
        AmazonSNS mockSNS = Mockito.mock(AmazonSNS.class);
        AmazonS3 mockS3 = Mockito.mock(AmazonS3.class);

        // Fixture S3 event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event.json"), S3Event.class);

        // Do *not* fixture environment

        // Expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("FAN_OUT_TOPIC must be set");

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSNS, mockS3);
        lambda.handler(s3Event);
    }
}
