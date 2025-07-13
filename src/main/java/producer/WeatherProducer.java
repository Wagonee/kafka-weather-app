package producer;

import model.WeatherData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import serdes.WeatherDataSerializer;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class WeatherProducer {
    private static final Random rnd = new Random(42);
    private static final String TOPIC = "weather";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";


    static List<String> cities = List.of("Москва", "Рим", "Новокосино", "Новосибирск", "Чертаново", "Выхино");
    static List<String> conditions = List.of("солнечно", "облачно", "дождь");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", WeatherDataSerializer.class.getName());

        try (KafkaProducer<String, WeatherData> producer = new KafkaProducer<>(props)) {
            while (true) {
                String city = cities.get(rnd.nextInt(cities.size()));
                double temperature = Math.round((rnd.nextDouble() * 35.0) * 10.0) / 10.0;
                String condition = conditions.get(rnd.nextInt(conditions.size()));
                LocalDateTime time = LocalDateTime.now();
                WeatherData weatherData = new WeatherData(city, temperature, condition, time);

                ProducerRecord<String, WeatherData> record = new ProducerRecord<>(TOPIC, weatherData.city(), weatherData);
                producer.send(record);

                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Error in producer " + e);
        }
    }
}
