package book.api;

public class WeatherEvent {
    public String locationName;
    public Double temperature;
    public Long timestamp;
    public Double longitude;
    public Double latitude;

    public WeatherEvent() {
    }

    public WeatherEvent(String locationName, Double temperature, Long timestamp, Double longitude, Double latitude) {
        this.locationName = locationName;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}
