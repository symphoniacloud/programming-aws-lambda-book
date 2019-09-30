package book;

public class PojoLambda {
    public PojoResponse handlerPojo(PojoInput input) {
        return new PojoResponse("Input was " + input.getA());
    }

    public static class PojoInput {
        private String a;

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }
    }

    public static class PojoResponse {
        private final String b;

        PojoResponse(String b) {
            this.b = b;
        }

        public String getB() {
            return b;
        }
    }
}
