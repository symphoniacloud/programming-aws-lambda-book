package book.pipeline;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.DescribeStackResourceRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackResourceResult;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class PipelineIT {

    private final String stackName;
    private final AmazonCloudFormation cfn = AmazonCloudFormationClientBuilder.defaultClient();
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final AWSLogs logs = AWSLogsClientBuilder.defaultClient();

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
        s3.putObject(bucketName, key, file);

        // 2. Check for executions of SingleEventLambda
        Thread.sleep(30000);
        String singleEventLambda = resolvePhysicalId("SingleEventLambda");
        Set<String> logMessages = getLogMessages(singleEventLambda);
        Assert.assertThat(logMessages, CoreMatchers.hasItems(
                "WeatherEvent{locationName='Brooklyn, NY', temperature=91.0, timestamp=1564428897, longitude=-73.99, latitude=40.7}",
                "WeatherEvent{locationName='Oxford, UK', temperature=64.0, timestamp=1564428898, longitude=-1.25, latitude=51.75}",
                "WeatherEvent{locationName='Charlottesville, VA', temperature=87.0, timestamp=1564428899, longitude=-78.47, latitude=38.02}"
        ));

        // 3. Delete object from S3 bucket (to allow a clean CloudFormation teardown)
        s3.deleteObject(bucketName, key);

        // 4. Delete Lambda log groups
        logs.deleteLogGroup(new DeleteLogGroupRequest(getLogGroup(singleEventLambda)));
        String bulkEventsLambda = resolvePhysicalId("BulkEventsLambda");
        logs.deleteLogGroup(new DeleteLogGroupRequest(getLogGroup(bulkEventsLambda)));
    }

    private String resolvePhysicalId(String logicalId) {
        DescribeStackResourceRequest request = new DescribeStackResourceRequest()
                .withStackName(stackName)
                .withLogicalResourceId(logicalId);
        DescribeStackResourceResult result = cfn.describeStackResource(request);
        return result.getStackResourceDetail().getPhysicalResourceId();
    }

    private Set<String> getLogMessages(String lambdaName) {
        String logGroup = getLogGroup(lambdaName);

        return logs.describeLogStreams(new DescribeLogStreamsRequest(logGroup))
                .getLogStreams().stream()
                .map(LogStream::getLogStreamName)
                .flatMap(logStream -> logs.getLogEvents(new GetLogEventsRequest(logGroup, logStream))
                        .getEvents().stream())
                .map(OutputLogEvent::getMessage)
                .filter(message -> message.contains("WeatherEvent"))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    private String getLogGroup(String lambdaName) {
        return String.format("/aws/lambda/%s", lambdaName);
    }

}
