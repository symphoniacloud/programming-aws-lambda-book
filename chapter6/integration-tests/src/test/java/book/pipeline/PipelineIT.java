package book.pipeline;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackResourceRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackResourceResponse;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class PipelineIT {

    private final String stackName;
    private final CloudFormationClient cfn = CloudFormationClient.create();
    private final S3Client s3 = S3Client.create();
    private final CloudWatchLogsClient logs = CloudWatchLogsClient.create();

    public PipelineIT() {
        this.stackName = System.getProperty("stackName");
        if (stackName == null) {
            throw new RuntimeException("stackName property must be set");
        }
    }

    @Test
    public void endToEndTest() throws InterruptedException {
        String bucketName = resolvePhysicalId("PipelineStartBucket");
        String key = UUID.randomUUID().toString();
        File file = new File(getClass().getResource("/bulk_data.json").getFile());

        // 1. Upload bulk_data file to S3
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), Paths.get(file.toURI()));

        // 2. Check for executions of SingleEventLambda
        Thread.sleep(30000);
        String singleEventLambda = resolvePhysicalId("SingleEventLambda");
        Set<String> logMessages = getLogMessages(singleEventLambda);
        assertThat(logMessages, hasItems(
                "WeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}",
                "WeatherEvent{locationName='Oxford, UK', temperature=64.0, timestamp=1564428898, longitude=-1.25, latitude=51.75}",
                "WeatherEvent{locationName='Charlottesville, VA', temperature=87.0, timestamp=1564428899, longitude=-78.47, latitude=38.02}"
        ));

        // 3. Delete object from S3 bucket (to allow a clean CloudFormation teardown)
        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());

        // 4. Delete Lambda log groups
        logs.deleteLogGroup(DeleteLogGroupRequest.builder()
                .logGroupName(getLogGroup(singleEventLambda))
                .build());
        String bulkEventsLambda = resolvePhysicalId("BulkEventsLambda");
        logs.deleteLogGroup(DeleteLogGroupRequest.builder()
                .logGroupName(getLogGroup(bulkEventsLambda))
                .build());
    }

    private String resolvePhysicalId(String logicalId) {
        DescribeStackResourceRequest request = DescribeStackResourceRequest.builder()
                .stackName(stackName)
                .logicalResourceId(logicalId)
                .build();
        DescribeStackResourceResponse response = cfn.describeStackResource(request);
        return response.stackResourceDetail().physicalResourceId();
    }

    private Set<String> getLogMessages(String lambdaName) {
        String logGroup = getLogGroup(lambdaName);

        return logs.describeLogStreams(DescribeLogStreamsRequest.builder()
                        .logGroupName(logGroup)
                        .build())
                .logStreams().stream()
                .map(LogStream::logStreamName)
                .flatMap(logStream -> logs.getLogEvents(GetLogEventsRequest.builder()
                                .logGroupName(logGroup)
                                .logStreamName(logStream)
                                .build())
                        .events().stream())
                .map(OutputLogEvent::message)
                .filter(message -> message.contains("WeatherEvent"))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    private String getLogGroup(String lambdaName) {
        return String.format("/aws/lambda/%s", lambdaName);
    }

}
