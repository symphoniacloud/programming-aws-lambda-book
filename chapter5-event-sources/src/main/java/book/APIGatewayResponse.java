package book;

public class APIGatewayResponse {
    public final int statusCode;
    public final String body;

    public APIGatewayResponse(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }
}
