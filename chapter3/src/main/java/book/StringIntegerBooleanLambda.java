package book;

public class StringIntegerBooleanLambda {
    public void handlerString(String s) {
        System.out.println("Hello, " + s);
    }

    public boolean handlerBoolean(boolean input) {
        return !input;
    }

    public boolean handlerInt(int input) {
        return input > 100;
    }
}
