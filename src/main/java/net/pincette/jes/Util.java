package net.pincette.jes;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.Command.hasError;
import static net.pincette.jes.Event.applyEvent;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.LANGUAGES;
import static net.pincette.jes.JsonFields.SEQ;
import static net.pincette.jes.JsonFields.TEST;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getNumber;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.filter.Util.stream;
import static net.pincette.util.Collections.set;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import net.pincette.json.JsonUtil;

/**
 * Some general utilities.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class Util {
  private static final Set<String> TECHNICAL_FIELDS =
      set(COMMAND, CORR, ID, JWT, LANGUAGES, SEQ, TEST, TIMESTAMP, TYPE);

  private Util() {}

  /**
   * Returns a reducer that first calls <code>validator</code> and if the result doesn't contain any
   * errors it calls <code>reducer</code>.
   *
   * @param validator the given validator.
   * @param reducer the given reducer.
   * @return The composed function.
   * @since 1.0
   */
  public static Reducer compose(final Reducer validator, final Reducer reducer) {
    return (command, aggregate) ->
        validator
            .apply(command, aggregate)
            .thenComposeAsync(
                result ->
                    hasError(result) ? completedFuture(result) : reducer.apply(command, aggregate));
  }

  /**
   * Returns the username from "/_jwt/sub".
   *
   * @param json the given object.
   * @return The username.
   * @since 1.0
   */
  public static Optional<String> getUsername(final JsonObject json) {
    return getString(json, "/_jwt/sub");
  }

  /**
   * Checks if <code>json</code> is a JSON Event Sourcing object. This means it has the fields
   * <code>_id</code> and <code>_type</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.1.2
   */
  public static boolean isJesObject(final JsonObject json) {
    return json != null
        && getString(json, "/" + ID).isPresent()
        && getString(json, "/" + TYPE).isPresent();
  }

  /**
   * Checks if <code>json</code> is a managed object. This means it has the fields <code>_id</code>,
   * <code>_type</code> and <code>_seq</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean isManagedObject(final JsonObject json) {
    return isJesObject(json) && getNumber(json, "/" + SEQ).isPresent();
  }

  /**
   * Checks if <code>json</code> is a managed object with the fields <code>_id</code> and <code>
   * _type</code> set to <code>id</code> and <code>type</code> respectively.
   *
   * @param json the given object.
   * @param type the aggregate type.
   * @param id the ID of the instance.
   * @return The check report.
   * @since 1.0
   */
  public static boolean isManagedObject(final JsonObject json, final String type, final String id) {
    return isManagedObject(json)
        && type != null
        && id != null
        && getString(json, "/" + TYPE).filter(t -> t.equals(type)).isPresent()
        && getString(json, "/" + ID).filter(i -> i.equalsIgnoreCase(id)).isPresent();
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the array of events, which should start at <code>_seq</code> equal to 0 and which
   *     should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final JsonArray events) {
    return reconstruct(events.stream());
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the event parser, which should start returning objects at <code>_seq</code> equal
   *     to 0 and which should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final JsonParser events) {
    return reconstruct(stream(events));
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the stream of events, which should start at <code>_seq</code> equal to 0 and
   *     which should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final Stream<? extends JsonValue> events) {
    return events.filter(JsonUtil::isObject).map(JsonValue::asJsonObject).map(applyEvent());
  }

  /**
   * Removes all the technical fields JES uses.
   *
   * @param json the given JSON object.
   * @return The new JSON object.
   * @since 1.4.2
   * @see JsonFields
   */
  public static JsonObjectBuilder removeTechnical(final JsonObject json) {
    return TECHNICAL_FIELDS.stream()
        .reduce(createObjectBuilder(json), JsonObjectBuilder::remove, (b1, b2) -> b1);
  }
}
