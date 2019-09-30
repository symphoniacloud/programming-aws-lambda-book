package book;

import com.amazonaws.services.lambda.runtime.Context;

public class TimeoutLambda {
    public void handler(Object input, Context context) throws InterruptedException {
        while (true) {
            Thread.sleep(100);
            System.out.println("Context.getRemainingTimeInMillis() : " +
                    context.getRemainingTimeInMillis());
        }
    }
}
