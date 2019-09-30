package book;

import com.amazonaws.services.lambda.runtime.events.S3Event;

public class S3ErroringLambda {
    public void handler(S3Event event) {
        System.out.println("Received new S3 event");
        throw new RuntimeException("This function unable to process S3 Events");
    }
}
