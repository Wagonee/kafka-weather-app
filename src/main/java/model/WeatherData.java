package model;

import java.time.LocalDateTime;

public record WeatherData(String city, double temp, String condition, LocalDateTime time) {
    @Override
    public String toString() {
        return "WeatherData{" +
                "city='" + city + '\'' +
                ", temp=" + temp +
                ", condition='" + condition + '\'' +
                ", time=" + time +
                '}';
    }
}
