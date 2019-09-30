package book;

public class EnvVarLambda {
    public void handler(Object event) {
        String databaseUrl = System.getenv("DATABASE_URL");
        if (databaseUrl == null || databaseUrl.isEmpty())
            System.out.println("DATABASE_URL is not set");
        else
            System.out.println("DATABASE_URL is set to: " + databaseUrl);
    }
}
