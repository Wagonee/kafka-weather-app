package consumer;

import model.WeatherData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import serdes.WeatherDataDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeatherConsumer {

    private static final String TOPIC = "weather";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Map<String, List<WeatherData>> weatherDataByCity = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", WeatherDataDeserializer.class.getName());
        props.setProperty("group.id", "weather-group");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(WeatherConsumer::printAnalytics, 15, 10, TimeUnit.SECONDS);

        try (KafkaConsumer<String, WeatherData> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            while (true) {
                ConsumerRecords<String, WeatherData> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, WeatherData> record : records) {
                    weatherDataByCity.computeIfAbsent(record.value().city(), k -> new ArrayList<>()).add(record.value());
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            scheduler.shutdown();
        }
    }

    private static void printAnalytics() {
        System.out.println("\n===== АНАЛИТИКА =====");

        String cityMostRain = null;
        long maxRainyDays = 0;
        for (var entry : weatherDataByCity.entrySet()) {
            long rainyDays = entry.getValue().stream()
                    .filter(d -> "дождь" .equalsIgnoreCase(d.condition()))
                    .count();
            if (rainyDays > maxRainyDays) {
                maxRainyDays = rainyDays;
                cityMostRain = entry.getKey();
            }
        }
        if (cityMostRain != null) {
            System.out.printf("Больше всего дождливых дней %d в городе %s%n", maxRainyDays, cityMostRain);
        } else {
            System.out.println("Нет данных о дождливых днях.");
        }

        String cityMaxTemp = null;
        double maxTemp = Double.MIN_VALUE;
        WeatherData maxTempData = null;
        for (var entry : weatherDataByCity.entrySet()) {
            for (WeatherData data : entry.getValue()) {
                if (data.temp() > maxTemp) {
                    maxTemp = data.temp();
                    cityMaxTemp = entry.getKey();
                    maxTempData = data;
                }
            }
        }
        if (maxTempData != null) {
            System.out.printf("Самая высокая температура %.1f°C была %s в городе %s%n",
                    maxTempData.temp(), maxTempData.time(), cityMaxTemp);
        } else {
            System.out.println("Нет данных о температуре.");
        }

        String cityMinAvg = null;
        double minAvgTemp = Double.MAX_VALUE;
        for (var entry : weatherDataByCity.entrySet()) {
            double avgTemp = entry.getValue().stream()
                    .mapToDouble(WeatherData::temp)
                    .average().orElse(Double.MAX_VALUE);
            if (avgTemp < minAvgTemp) {
                minAvgTemp = avgTemp;
                cityMinAvg = entry.getKey();
            }
        }
        if (cityMinAvg != null) {
            System.out.printf("Самая низкая средняя температура %.1f°C в городе %s%n", minAvgTemp, cityMinAvg);
        } else {
            System.out.println("Нет данных о средней температуре.");
        }
        System.out.println("=======================\n");
    }
}
