package net.pincette.jes;

import static java.time.Instant.now;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.util.Event.isEvent;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.iterate;
import static net.pincette.util.Util.tryToGetRethrow;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Often reacting to events consists of converting them to commands, which are then sent to some
 * aggregate. This class makes that a little easier. You provide it with the source and destination
 * aggregate types, a function to fetch the identifiers of the destinations and a function to create
 * a command from an event. Optionally you can also provide an event filter function.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
public class Reactor {
  private StreamsBuilder builder;
  private String destinationType;
  private GetDestinations destinations;
  private String environment = "dev";
  private EventToCommand eventToCommand;
  private Predicate<JsonObject> filter;
  private String sourceType;

  public StreamsBuilder build() {
    final KStream<String, JsonObject> stream =
        builder.stream(sourceType + "-event-full-" + environment);

    stream
        .filter(this::filterEvent)
        .flatMap(this::createCommands)
        .to(destinationType + "-command-" + environment);

    return builder;
  }

  private JsonObject completeCommand(
      final String id, final JsonObject event, final JsonObject command) {
    return createObjectBuilder(command)
        .add(ID, id)
        .add(TYPE, destinationType)
        .add(CORR, event.getString(CORR))
        .add(JWT, Optional.ofNullable(event.getJsonObject(JWT)).orElseGet(JsonUtil::emptyObject))
        .add(TIMESTAMP, now().toEpochMilli())
        .build();
  }

  private Iterable<KeyValue<String, JsonObject>> createCommands(
      final String id, final JsonObject event) {
    return iterate(
        tryToGetRethrow(() -> eventToCommand.apply(event).toCompletableFuture().get())
            .filter(command -> command.containsKey(COMMAND))
            .map(
                command ->
                    with(destinations.apply(event))
                        .map(dest -> dest.getString(ID, null))
                        .filter(Objects::nonNull)
                        .map(dest -> new KeyValue<>(dest, completeCommand(dest, event, command)))
                        .get())
            .orElseGet(net.pincette.rs.Util::empty));
  }

  private boolean filterEvent(final String id, final JsonObject event) {
    return isEvent(event) && event.containsKey(CORR) && (filter == null || filter.test(event));
  }

  /**
   * Sets the Kafka Streams builder that will be used to create the topology.
   *
   * @param builder the given builder.
   * @return The reactor object itself.
   * @since 1.1
   */
  public Reactor withBuilder(final StreamsBuilder builder) {
    this.builder = builder;

    return this;
  }

  /**
   * Sets the full aggregate type for the commands.
   *
   * @param type the full agregate type for the commands.
   * @return The reactor object itself;
   * @since 1.1
   */
  public Reactor withDestinationType(final String type) {
    this.destinationType = type;

    return this;
  }

  /**
   * Sets the function with which the command destinations are obtained.
   *
   * @param destinations the function that returns the identifiers of the destinations.
   * @return The reactor object itself.
   * @since 1.1
   */
  public Reactor withDestinations(final GetDestinations destinations) {
    this.destinations = destinations;

    return this;
  }

  /**
   * Sets the environment in which this reactor will live. The default is <code>dev</code>. It will
   * become the suffix for all the topic names. Typically the value for this comes from an external
   * configuration.
   *
   * @param environment the name of the environment.
   * @return The reactor object itself.
   * @since 1.1
   */
  public Reactor withEnvironment(final String environment) {
    this.environment = environment;

    return this;
  }

  /**
   * Sets the function that creates commands from full events. The fields "_id", "_type" and "_corr"
   * will always be overwritten.
   *
   * @param eventToCommand the command creation function.
   * @return The reactor object itself.
   * @since 1.1
   */
  public Reactor withEventToCommand(final EventToCommand eventToCommand) {
    this.eventToCommand = eventToCommand;

    return this;
  }

  /**
   * Sets the optional function that filters the full events.
   *
   * @param filter the full event filter.
   * @return The reactor object itself.
   * @since 1.1
   */
  public Reactor withFilter(final Predicate<JsonObject> filter) {
    this.filter = filter;

    return this;
  }

  /**
   * Sets the full aggregate type of the event source. Events from another type are ignored.
   *
   * @param type the full agregate type of the event source.
   * @return The reactor object itself;
   * @since 1.1
   */
  public Reactor withSourceType(final String type) {
    this.sourceType = type;

    return this;
  }
}
