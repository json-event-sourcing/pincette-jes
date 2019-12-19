package net.pincette.jes;

import java.util.function.BiFunction;
import javax.json.JsonObject;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * A type for functions that create extra streams inside an aggregate.
 *
 * @since 1.0
 * @author Werner Donn\u00e9
 */
@FunctionalInterface
public interface StreamProcessor
    extends BiFunction<KStream<String, JsonObject>, StreamsBuilder, KStream<String, JsonObject>> {}
