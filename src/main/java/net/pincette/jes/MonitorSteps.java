package net.pincette.jes;

import static net.pincette.util.Collections.set;

import java.util.Set;

/**
 * Represents the monitoring steps that occur in an aggregate.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class MonitorSteps {
  public static final String AGGREGATE_TOPIC = "aggregate-topic";
  public static final String COMMAND_TOPIC = "command-topic";
  public static final String ERROR = "error";
  public static final String EVENT_FULL_TOPIC = "event-full-topic";
  public static final String EVENT_TOPIC = "event-topic";
  public static final String RECEIVED = "received";
  public static final String REDUCE = "reduce";
  public static final String REPLY_TOPIC = "reply-topic";
  private static final Set<String> ALL_ERROR = set(COMMAND_TOPIC, ERROR, RECEIVED, REPLY_TOPIC);
  private static final Set<String> ALL_OK =
      set(
          AGGREGATE_TOPIC,
          COMMAND_TOPIC,
          EVENT_FULL_TOPIC,
          EVENT_TOPIC,
          RECEIVED,
          REDUCE,
          REPLY_TOPIC);

  private MonitorSteps() {}

  /**
   * Returns all monitor steps that should be present when an error has occurred.
   *
   * @return The steps.
   * @since 1.0
   */
  public static Set<String> allError() {
    return ALL_ERROR;
  }

  /**
   * Returns all monitor steps that should be present when the request was completed successfully.
   *
   * @return The steps.
   * @since 1.0
   */
  public static Set<String> allOk() {
    return ALL_OK;
  }
}
