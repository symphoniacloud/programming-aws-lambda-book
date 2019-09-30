package book;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

public class HelloWorldAPI {
    public APIGatewayProxyResponseEvent handler(APIGatewayProxyRequestEvent event, Context c) {
        System.out.println(event);
        return new APIGatewayProxyResponseEvent().withStatusCode(200).withBody("HelloAPIWorld");
    }
}
