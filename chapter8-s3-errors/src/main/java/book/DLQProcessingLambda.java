package book;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;

public class DLQProcessingLambda {
    public void handler(SNSEvent event) {
        event.getRecords().forEach(snsRecord ->
                System.out.println("Received DLQ event: " + snsRecord.toString())
        );
    }
}
