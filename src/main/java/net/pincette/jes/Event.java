package net.pincette.jes;

import static java.util.regex.Pattern.compile;
import static net.pincette.jes.JsonFields.BEFORE;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.OPS;
import static net.pincette.jes.JsonFields.SEQ;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.jes.Util.isManagedObject;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createPatch;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.accumulate;
import static net.pincette.util.Util.tryToGet;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.Patch;
import net.pincette.util.Util.GeneralException;

/**
 * Utilities to work with events.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class Event {
  private static final Pattern SEQ_SUFFIX = compile("-\\d{6,}");

  private Event() {}

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> was added. It examines the
   * <code>_ops</code> field for this.
   *
   * @param event the full event, which has the <code>_before</code> field.
   * @param jsonPointer the path into the aggregate.
   * @return <code>true</code> when the event expresses the addition of the given field.
   * @since 1.4.1
   */
  public static boolean added(final JsonObject event, final String jsonPointer) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .flatMap(
            ops ->
                Optional.ofNullable(event.getJsonObject(BEFORE)).map(before -> pair(ops, before)))
        .map(pair -> Patch.added(pair.first, pair.second, jsonPointer))
        .orElse(false);
  }

  /**
   * Applies an event to an aggregate instance, which results in the next version of the aggregate.
   *
   * @param aggregate the given aggregate instance.
   * @param event the given event.
   * @return The next version of the aggregate instance.
   * @since 1.2
   */
  public static JsonObject applyEvent(final JsonObject aggregate, final JsonObject event) {
    return tryToGet(
            () ->
                create(
                        () ->
                            createObjectBuilder(
                                createPatch(event.getJsonArray(OPS))
                                    .apply(aggregate)
                                    .asJsonObject()))
                    .update(b -> b.add(ID, stripSequenceNumber(event.getString(ID))))
                    .update(b -> b.add(TYPE, event.getString(TYPE)))
                    .update(b -> b.add(CORR, event.getString(CORR)))
                    .update(b -> b.add(SEQ, event.getInt(SEQ)))
                    .updateIf(
                        () -> Optional.ofNullable(event.getJsonNumber(TIMESTAMP)),
                        (b, t) -> b.add(TIMESTAMP, t))
                    .updateIf(
                        () -> Optional.ofNullable(event.getJsonObject(JWT)),
                        (b, jwt) -> b.add(JWT, jwt))
                    .build()
                    .build(),
            e -> {
              throw new GeneralException("Event: " + string(event) + ": " + e.getMessage());
            })
        .orElse(null);
  }

  static Function<JsonObject, JsonObject> applyEvent() {
    return applyEvent(null);
  }

  static Function<JsonObject, JsonObject> applyEvent(final JsonObject snapshot) {
    return accumulate(Event::applyEvent, snapshot != null ? snapshot : emptyObject());
  }

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> has changed. It examines the
   * <code>_ops</code> field for this.
   *
   * @param event the event.
   * @param jsonPointer the path into the aggregate.
   * @return <code>true</code> when the event expresses a change of the given field.
   * @since 1.0
   */
  public static boolean changed(final JsonObject event, final String jsonPointer) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .map(ops -> Patch.changed(ops, jsonPointer))
        .orElse(false);
  }

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> has changed from the value
   * in <code>from</code> to the value in <code>to</code>. It examines the <code>_ops</code> field
   * for this.
   *
   * @param event the full event, which has the <code>_before</code> field.
   * @param jsonPointer the path into the aggregate.
   * @param from the original value.
   * @param to the new value.
   * @return <code>true</code> when the event expresses a change of the given field.
   * @since 1.0
   */
  public static boolean changed(
      final JsonObject event, final String jsonPointer, final JsonValue from, final JsonValue to) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .flatMap(
            ops ->
                Optional.ofNullable(event.getJsonObject(BEFORE))
                    .filter(
                        before ->
                            getValue(before, jsonPointer)
                                .filter(value -> value.equals(from))
                                .isPresent())
                    .map(before -> pair(ops, before)))
        .map(pair -> Patch.changed(pair.first, pair.second, jsonPointer, from, to))
        .orElse(false);
  }

  private static boolean hasOps(final JsonObject event) {
    return Optional.ofNullable(event.getJsonArray(OPS)).filter(ops -> !ops.isEmpty()).isPresent();
  }

  /**
   * Checks if <code>event</code> has the proper event structure. This means it should be a managed
   * object with an <code>_ops</code> field.
   *
   * @param event the given event.
   * @return The check report.
   * @see JsonFields
   * @see Util#isManagedObject
   * @since 1.0
   */
  public static boolean isEvent(final JsonObject event) {
    return isManagedObject(event) && hasOps(event);
  }

  /**
   * Checks if the event has the next sequence number for the aggregate. Event log replicators can
   * use this to detect corruption.
   *
   * @param aggregate the aggregate instance.
   * @param event the event.
   * @return <code>true</code> if the event is the next one, <code>false</code> otherwise.
   * @since 1.0
   */
  public static boolean isNext(final JsonObject aggregate, final JsonObject event) {
    return event.getInt(SEQ) == aggregate.getInt(SEQ) + 1;
  }

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> was removed. It examines the
   * <code>_ops</code> field for this.
   *
   * @param event the event.
   * @param jsonPointer the path into the aggregate.
   * @return <code>true</code> when the event expresses the removal of the given field.
   * @since 1.4.1
   */
  public static boolean removed(final JsonObject event, final String jsonPointer) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .map(ops -> Patch.removed(ops, jsonPointer))
        .orElse(false);
  }

  /**
   * Creates a standard message to alert event sequence errors. Events are numbered with the <code>
   * _seq</code> field. When an event listener detect a "hole" in the numbering it can use this
   * message to log the corruption.
   *
   * @param aggregate the aggregate about which the error message is produced.
   * @param event the event about which the error message is produced.
   * @return The generated error message.
   * @since 1.0
   */
  public static String sequenceErrorMessage(final JsonObject aggregate, final JsonObject event) {
    return "SEQUENCE ERROR: event received for ("
        + aggregate.getString(ID)
        + ","
        + aggregate.getString(TYPE)
        + ") with sequence number "
        + event.getInt(SEQ)
        + ", while "
        + (aggregate.getInt(SEQ) + 1)
        + " was expected";
  }

  private static String stripSequenceNumber(final String id) {
    return Optional.of(id.lastIndexOf('-'))
        .filter(index -> index != -1 && SEQ_SUFFIX.matcher(id.substring(index)).matches())
        .map(index -> id.substring(0, index))
        .orElse(id);
  }
}
