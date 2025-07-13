package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import model.WeatherData;
import org.apache.kafka.common.serialization.Serializer;

public class WeatherDataSerializer implements Serializer<WeatherData> {
    private final ObjectMapper mapper;

    public WeatherDataSerializer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(String topic, WeatherData data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error in serializing WeatherData", e);
        }
    }
}
