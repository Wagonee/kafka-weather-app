package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import model.WeatherData;
import org.apache.kafka.common.serialization.Deserializer;

public class WeatherDataDeserializer implements Deserializer<WeatherData> {
    private final ObjectMapper mapper;

    public WeatherDataDeserializer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public WeatherData deserialize(String topic, byte[] bytes) {
        try {
            return mapper.readValue(bytes, WeatherData.class);
        } catch (Exception e) {
            throw new RuntimeException("Error in deserializing WeatherData", e);
        }
    }
}
