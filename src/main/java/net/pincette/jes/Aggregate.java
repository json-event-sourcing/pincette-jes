package net.pincette.jes;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static java.lang.Boolean.FALSE;
import static java.lang.String.valueOf;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Arrays.fill;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createDiff;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createPatch;
import static net.pincette.jes.util.Command.hasError;
import static net.pincette.jes.util.Command.isAllowed;
import static net.pincette.jes.util.Command.isCommand;
import static net.pincette.jes.util.Commands.DELETE;
import static net.pincette.jes.util.Commands.PATCH;
import static net.pincette.jes.util.Commands.PUT;
import static net.pincette.jes.util.Event.isEvent;
import static net.pincette.jes.util.JsonFields.AFTER;
import static net.pincette.jes.util.JsonFields.BEFORE;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ERROR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.LANGUAGES;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.STATUS_CODE;
import static net.pincette.jes.util.JsonFields.TEST;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Mongo.find;
import static net.pincette.jes.util.Mongo.findOne;
import static net.pincette.jes.util.Mongo.restore;
import static net.pincette.jes.util.Mongo.update;
import static net.pincette.json.JsonUtil.getBoolean;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGet;

import com.mongodb.reactivestreams.client.MongoDatabase;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import net.pincette.function.SideEffect;
import net.pincette.jes.util.AuditFields;
import net.pincette.jes.util.Reducer;
import net.pincette.util.TimedCache;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * With this class JSON aggregates are managed using Kafka Streams. You give it reducers, which
 * calculate the new aggregate state with the current one and an incoming command. For every
 * aggregate instance all commands are processed sequentially. The result of a reducer execution is
 * compared with the current aggregate state and the difference is emitted as an event. A reducer
 * may also find problems in the command. In that case it should return the command, marked with
 * <code>"_error": true</code>. No event will be emitted then.
 *
 * <p>The external interface at runtime is a set op Kafka topics. Their names always have the form
 * &lt;app&gt;-&lt;type&gt;-&lt;purpose&gt;-&lt;environment&gt;. The following topics are expected
 * to exist (the names are the purpose):
 *
 * <dl>
 *   <dd>aggregate
 *   <dt>On this topic the current state of the aggregate is emitted.
 *   <dd>command
 *   <dt>Through this topic commands are received. It is the only input of the system.
 *   <dd>event
 *   <dt>On this topic the events are emitted, which contain the changes between two subsequent
 *       aggregate versions.
 *   <dd>event-full
 *   <dt>The events are also emitted on this topic, but here they have two extra fields. The <code>
 *       _before</code> field contains the previous state of the aggregate, while <code>_after
 *       </code> contains the current one. This is for consumers that want to do other kinds of
 *       analysis than the plain difference.
 *   <dd>monitor
 *   <dt>This optional topic receives monitoring events.
 *   <dd>reply
 *   <dt>On this topic either the new aggregate or the failed command it emitted. The topic is meant
 *       to be routed back to the end-user, for example through Server-Sent Events. A reactive
 *       client can pick it up and update its stores. This connects the server and the client in one
 *       reactive loop.
 * </dl>
 *
 * <p>An aggregate is a JSON document, which has the following technical fields on top of whatever
 * you put in it:
 *
 * <dl>
 *   <dd>_corr
 *   <dt>The correlation identifier that was used by the last command. It is usually a UUID.
 *   <dd>_deleted
 *   <dt>This boolean marks the aggregate instance as deleted. This is a logical delete.
 *   <dd>_id
 *   <dt>The identifier of the aggregate instance. It is usually a UUID.
 *   <dd>_jwt
 *   <dt>The decoded JSON Web Token that was used by the last command.
 *   <dd>_seq
 *   <dt>A sequence number. This is the sequence number of the last event.
 *   <dd>_type
 *   <dt>The aggregate type, which is composed as &lt;application&gt;-&lt;name&gt;.
 * </dl>
 *
 * <p>A command is a JSON document, which has the following technical fields on top of whatever you
 * put in it:
 *
 * <dl>
 *   <dd>_command
 *   <dt>The name of the command. This field is mandatory.
 *   <dd>_corr
 *   <dt>A correlation identifier. It is propagated throughout the flow. This is usually a UUID.
 *   <dd>_error
 *   <dt>This boolean indicates there is a problem with the command.
 *   <dd>_id
 *   <dt>The identifier of the aggregate instance. It is usually a UUID. This field is mandatory.
 *   <dd>_jwt
 *   <dt>The decoded JSON Web Token.
 *   <dd>_languages
 *   <dt>An array of language tags in the order of preference. When a validator or some other
 *       component wishes to send messages to the user, it can use the proper language for it.
 *   <dd>_type
 *   <dt>The aggregate type, which is composed as &lt;application&gt;-&lt;name&gt;. This field is
 *       mandatory.
 * </dl>
 *
 * <p>An event is a JSON document, which has the following technical fields:
 *
 * <dl>
 *   <dd>_after
 *   <dt>An optional field that carries the new state of the aggregate instance.
 *   <dd>_before
 *   <dt>An optional field that carries the previous state of the aggregate instance.
 *   <dd>_command
 *   <dt>The name of the command that caused the event to be created.
 *   <dd>_corr
 *   <dt>The correlation identifier that was used by the last command. It is usually a UUID.
 *   <dd>_id
 *   <dt>The identifier of the aggregate instance. It is usually a UUID.
 *   <dd>_jwt
 *   <dt>The decoded JSON Web Token that was used by the last command.
 *   <dd>_ops
 *   <dt>An array of operations as described in RFC 6902. It describes how an aggregate instance has
 *       changed after the reduction of a command.
 *   <dd>_seq
 *   <dt>A sequence number. There should not be holes in the sequence. This would indicate
 *       corruption of the event log.
 *   <dd>_timestamp
 *   <dt>The timestamp in epoch millis.
 *   <dd>_type
 *   <dt>The aggregate type, which is composed as &lt;application&gt;-&lt;name&gt;.
 * </dl>
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Aggregate {
  private static final String AGGREGATE_TOPIC = "aggregate";
  private static final String COMMAND_TOPIC = "command";
  private static final Duration DUPLICATE_WINDOW = ofSeconds(60);
  private static final String EVENT_TOPIC = "event";
  private static final String EVENT_FULL_TOPIC = "event-full";
  private static final String EXCEPTION = "exception";
  private static final String MONITOR_TOPIC = "monitor";
  private static final String REPLY_TOPIC = "reply";
  private static final String STEP = "step";
  private static final String STEP_AFTER = "after";
  private static final String STEP_COMMAND = "command";
  private static final String STEP_ERROR = "error";
  private static final String STEP_TIMESTAMP = "timestamp";
  private static final Set<String> TECHNICAL_FIELDS =
      set(COMMAND, CORR, ID, JWT, LANGUAGES, SEQ, TEST, TIMESTAMP, TYPE);
  private final Map<String, Reducer> reducers = new HashMap<>();
  private final TimedCache<String, JsonObject> aggregateCache = new TimedCache<>(DUPLICATE_WINDOW);
  private String auditTopic;
  private KStream<String, JsonObject> aggregates;
  private String app;
  private boolean breakingTheGlass;
  private StreamsBuilder builder;
  private StreamProcessor commandProcessor;
  private KStream<String, JsonObject> commands;
  private MongoDatabase database;
  private String environment = "dev";
  private KStream<String, JsonObject> events;
  private KStream<String, JsonObject> eventsFull;
  private KStream<String, JsonObject> monitor;
  private boolean monitoring;
  private Reducer reducer;
  private KStream<String, JsonObject> replies;
  private String type;

  /**
   * This will install a standard reducer for the commands <code>delete</code>, <code>patch</code>
   * and <code>put</code>.
   *
   * @since 1.0
   */
  public Aggregate() {
    withReducer(DELETE, (command, currentState) -> delete(currentState));
    withReducer(PATCH, Aggregate::patch);
    withReducer(PUT, (command, currentState) -> put(command));
  }

  private static JsonObject accessError(final JsonObject command) {
    return createObjectBuilder(command)
        .add(ERROR, true)
        .add(STATUS_CODE, 403)
        .add("message", "Forbidden")
        .build();
  }

  private static JsonObject completeCommand(final JsonObject command) {
    return !command.containsKey(TIMESTAMP)
        ? createObjectBuilder(command)
            .add(
                TIMESTAMP,
                Optional.ofNullable(command.getJsonNumber(TIMESTAMP))
                    .map(JsonNumber::longValue)
                    .orElseGet(() -> now().toEpochMilli()))
            .build()
        : command;
  }

  private static JsonObjectBuilder createAfter(
      final JsonObject newState,
      final JsonObject command,
      final String corr,
      final int seq,
      final long now) {
    return create(
            () ->
                createObjectBuilder(newState)
                    .add(CORR, corr)
                    .add(SEQ, seq)
                    .add(TIMESTAMP, now)
                    .remove(JWT))
        .updateIf(b -> command.containsKey(JWT), b -> b.add(JWT, command.getJsonObject(JWT)))
        .build();
  }

  private static JsonObject createAggregateMessage(final JsonObject aggregate) {
    return createObjectBuilder()
        .add(TYPE, aggregate.getString(TYPE))
        .add(JWT, aggregate.getJsonObject(JWT))
        .build();
  }

  private static JsonObject createError(final JsonObject command, final long timestamp) {
    return createObjectBuilder()
        .add(STEP, STEP_ERROR)
        .add(STEP_COMMAND, command)
        .add(STEP_TIMESTAMP, timestamp)
        .build();
  }

  private static JsonObject createEvent(
      final JsonObject oldState,
      final JsonObject newState,
      final JsonObject command,
      final JsonArray ops) {
    final String corr = command.getString(CORR);
    final long now = now().toEpochMilli();
    final int seq = oldState.getInt(SEQ, -1) + 1;

    return create(
            () ->
                createObjectBuilder()
                    .add(CORR, corr)
                    .add(ID, newState.getString(ID).toLowerCase())
                    .add(TYPE, newState.getString(TYPE))
                    .add(SEQ, seq)
                    .add(COMMAND, command.getString(COMMAND))
                    .add(TIMESTAMP, now)
                    .add(BEFORE, oldState)
                    .add(AFTER, createAfter(newState, command, corr, seq, now))
                    .add(OPS, ops))
        .updateIf(
            b -> command.containsKey(LANGUAGES),
            b -> b.add(LANGUAGES, command.getJsonArray(LANGUAGES)))
        .updateIf(b -> command.containsKey(JWT), b -> b.add(JWT, command.getJsonObject(JWT)))
        .build()
        .build();
  }

  private static JsonArray createOps(final JsonObject oldState, final JsonObject newState) {
    return createArrayBuilder(
            createDiff(removeTechnical(oldState).build(), removeTechnical(newState).build())
                .toJsonArray())
        .build();
  }

  private static JsonObject createStep(
      final String step, final String after, final long timestamp) {
    return createStep(step, after, timestamp, null);
  }

  private static JsonObject createStep(
      final String step, final String after, final long timestamp, final String command) {
    return create(javax.json.Json::createObjectBuilder)
        .update(b -> b.add(STEP, step))
        .update(b -> b.add(STEP_TIMESTAMP, timestamp))
        .updateIf(b -> after != null, b -> b.add(STEP_AFTER, after))
        .updateIf(b -> command != null, b -> b.add(STEP_COMMAND, command))
        .build()
        .build();
  }

  /**
   * The standard delete reducer. It sets the field <code>_deleted</code> to <code>true</code>.
   *
   * @param currentState the current state of the aggregate.
   * @return The new state of the aggregate.
   * @since 1.0
   */
  public static CompletionStage<JsonObject> delete(final JsonObject currentState) {
    return completedFuture(createObjectBuilder(currentState).add(DELETED, true).build());
  }

  private static KStream<String, JsonObject> errors(final KStream<String, JsonObject> reducer) {
    return reducer.filter((k, v) -> isCommand(v) && hasError(v));
  }

  private static String generateSeq(final long value) {
    return pad(valueOf(value), '0', 12);
  }

  private static JsonObject idsToLowerCase(final JsonObject json) {
    return createObjectBuilder(json)
        .add(ID, json.getString(ID).toLowerCase())
        .add(CORR, json.getString(CORR).toLowerCase())
        .build();
  }

  private static JsonObject makeManaged(final JsonObject state, final JsonObject command) {
    return createObjectBuilder(state)
        .add(ID, command.getString(ID))
        .add(TYPE, command.getString(TYPE))
        .build();
  }

  private static String mongoEventKey(final JsonObject json) {
    return mongoEventKey(json, json.getJsonNumber(SEQ).longValue());
  }

  private static String mongoEventKey(final JsonObject json, final long seq) {
    return json.getString(ID) + "-" + generateSeq(seq);
  }

  private static String pad(final String s, final char c, final int size) {
    return s.length() >= size ? s : (new String(pad(c, size - s.length())) + s);
  }

  private static char[] pad(final char c, final int size) {
    final char[] result = new char[size];

    fill(result, c);

    return result;
  }

  /**
   * The standard patch reducer. It expects to find the <code>_ops</code> field, which contains a
   * JSON patch and applies it to the aggregate.
   *
   * @param currentState the current state of the aggregate.
   * @param command the given command.
   * @return The new state of the aggregate.
   * @since 1.0
   */
  public static CompletionStage<JsonObject> patch(
      final JsonObject command, final JsonObject currentState) {
    return completedFuture(
        Optional.ofNullable(command.getJsonArray(OPS))
            .map(ops -> createPatch(ops).apply(currentState))
            .orElse(currentState));
  }

  private static JsonObject plainEvent(final JsonObject fullEvent) {
    return createObjectBuilder(fullEvent).remove(AFTER).remove(BEFORE).build();
  }

  /**
   * The standard put reducer. It just removes the <code>_command</code> field and uses everything
   * else as the new state of the aggregate.
   *
   * @param command the given command.
   * @return The new state of the aggregate.
   * @since 1.0
   */
  public static CompletionStage<JsonObject> put(final JsonObject command) {
    return completedFuture(createObjectBuilder(command).remove(COMMAND).build());
  }

  private static JsonObjectBuilder removeTechnical(final JsonObject json) {
    return TECHNICAL_FIELDS.stream()
        .reduce(createObjectBuilder(json), JsonObjectBuilder::remove, (b1, b2) -> b1);
  }

  private static JsonObject setException(final JsonObject command, final Exception e) {
    return createObjectBuilder(command).add(ERROR, true).add(EXCEPTION, getStackTrace(e)).build();
  }

  private static JsonObject setId(final JsonObject json, final String id) {
    return createObjectBuilder(json).add(ID, id).build();
  }

  /**
   * Returns the aggregate stream.
   *
   * @return The aggregate stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> aggregates() {
    return aggregates;
  }

  private void aggregates(final KStream<String, JsonObject> reducer) {
    aggregates =
        reducer
            .filter((k, v) -> isEvent(v) && v.containsKey(AFTER))
            .mapValues(json -> json.getJsonObject(AFTER));
    aggregates.to(topic(AGGREGATE_TOPIC));
  }

  /**
   * Returns the app name.
   *
   * @return The app name.
   * @since 1.0
   */
  public String app() {
    return app;
  }

  private void audit() {
    if (auditTopic != null) {
      events
          .mapValues(
              v ->
                  createObjectBuilder()
                      .add(AuditFields.AGGREGATE, v.getString(ID))
                      .add(AuditFields.TYPE, v.getString(TYPE))
                      .add(AuditFields.COMMAND, v.getString(COMMAND))
                      .add(AuditFields.TIMESTAMP, v.getJsonNumber(TIMESTAMP).longValue())
                      .add(AuditFields.USER, getString(v, "/_jwt/sub").orElse("anonymous"))
                      .add(
                          AuditFields.BREAKING_THE_GLASS,
                          getBoolean(v, "/_jwt/breakingTheGlass").orElse(false))
                      .build())
          .to(auditTopic);
    }
  }

  /**
   * This builds the Kafka Streams topology for the aggregate.
   *
   * @return The builder that was given before.
   * @since 1.0
   */
  public StreamsBuilder build() {
    assert app != null
        && builder != null
        && environment != null
        && type != null
        && database != null;

    commands = createCommands();

    final KStream<String, JsonObject> red = reducer();

    aggregates(red);
    replies(red);
    events(red);
    monitorTopic(aggregates, AGGREGATE_TOPIC);
    monitorTopic(events, EVENT_TOPIC);
    monitorTopic(replies, REPLY_TOPIC);
    monitorTopic(eventsFull, EVENT_FULL_TOPIC);
    monitorReducer(red);
    audit();

    return builder;
  }

  /**
   * Returns the command stream, after applying potential command processors.
   *
   * @return The command stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> commands() {
    return commands;
  }

  private KStream<String, JsonObject> createCommands() {
    final KStream<String, JsonObject> com = builder.stream(topic(COMMAND_TOPIC));
    final KStream<String, JsonObject> commandFilter =
        com.filter((k, v) -> isCommand(v))
            .map((k, v) -> new KeyValue<>(k.toLowerCase(), idsToLowerCase(v)))
            .mapValues(Aggregate::completeCommand);

    monitorCommands(commandFilter);

    return (commandProcessor != null
        ? commandProcessor.apply(commandFilter, builder)
        : commandFilter);
  }

  /**
   * Returns the environment.
   *
   * @return The environment.
   * @since 1.0
   */
  public String environment() {
    return environment;
  }

  /**
   * Returns the event stream.
   *
   * @return The event stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> events() {
    return events;
  }

  private void events(final KStream<String, JsonObject> reducer) {
    eventsFull = reducer.filter((k, v) -> isEvent(v));
    events = eventsFull.mapValues(Aggregate::plainEvent);
    eventsFull.to(topic(EVENT_FULL_TOPIC));
    events.to(topic(EVENT_TOPIC));
  }

  /**
   * Returns the full event stream.
   *
   * @return The event stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> eventsFull() {
    return eventsFull;
  }

  private CompletionStage<JsonObject> executeReducer(
      final JsonObject command, final JsonObject currentState) {
    return Optional.ofNullable(reducer)
        .map(red -> red.apply(command, currentState))
        .orElseGet(
            () ->
                Optional.ofNullable(reducers.get(command.getString(COMMAND)))
                    .map(red -> red.apply(command, currentState))
                    .orElseGet(() -> completedFuture(currentState)));
  }

  /**
   * Returns the full aggregate type, which is composed as &lt;app&gt;-&lt;type&gt;.
   *
   * @return The full aggregate type.
   * @since 1.1
   */
  public String fullType() {
    return app + "-" + type;
  }

  private CompletionStage<JsonObject> getCurrentState(final String id) {
    return aggregateCache
        .get(id)
        .map(currentState -> (CompletionStage<JsonObject>) completedFuture(currentState))
        .orElseGet(
            () ->
                getMongoCurrentState(id)
                    .thenComposeAsync(
                        currentState ->
                            currentState
                                .map(state -> (CompletionStage<JsonObject>) completedFuture(state))
                                .orElseGet(() -> restore(id, fullType(), environment, database))));
  }

  private CompletionStage<Optional<JsonObject>> getMongoCurrentState(final String id) {
    return getMongoEntity(mongoAggregateCollection(), id);
  }

  private CompletionStage<Optional<JsonObject>> getMongoEntity(
      final String collection, final String id) {
    return findOne(database.getCollection(collection), eq(ID, id));
  }

  private CompletionStage<Boolean> isDuplicate(final JsonObject command) {
    return find(
            database.getCollection(mongoEventCollection()),
            and(
                regex(ID, "^" + command.getString(ID) + ".*"),
                eq(CORR, command.getString(CORR)),
                eq(COMMAND, command.getString(COMMAND))))
        .thenApply(result -> !result.isEmpty());
  }

  /**
   * Returns the monitor stream.
   *
   * @return The monitor stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> monitor() {
    if (monitor == null) {
      monitor = builder.stream(topic(MONITOR_TOPIC));
    }

    return monitor;
  }

  private String mongoAggregateCollection() {
    return fullType() + "-" + environment;
  }

  private String mongoEventCollection() {
    return fullType() + "-event-" + environment;
  }

  private void monitorCommands(final KStream<String, JsonObject> commands) {
    if (monitoring) {
      commands
          .filter((k, v) -> v.containsKey(CORR))
          .flatMap(
              (k, v) ->
                  list(
                      new KeyValue<>(
                          v.getString(CORR),
                          createStep(
                              MonitorSteps.RECEIVED,
                              null,
                              v.getJsonNumber(TIMESTAMP).longValue(),
                              v.getString(COMMAND))),
                      new KeyValue<>(
                          v.getString(CORR),
                          createStep(
                              MonitorSteps.COMMAND_TOPIC,
                              MonitorSteps.RECEIVED,
                              now().toEpochMilli(),
                              v.getString(COMMAND)))))
          .to(topic(MONITOR_TOPIC));
    }
  }

  private void monitorReducer(final KStream<String, JsonObject> reducer) {
    if (monitoring) {
      reducer
          .filter((k, v) -> v.containsKey(CORR) && !hasError(v))
          .map(
              (k, v) ->
                  new KeyValue<>(
                      v.getString(CORR),
                      createStep(
                          MonitorSteps.REDUCE, MonitorSteps.COMMAND_TOPIC, now().toEpochMilli())))
          .to(topic(MONITOR_TOPIC));

      reducer
          .filter((k, v) -> v.containsKey(CORR) && hasError(v))
          .map((k, v) -> new KeyValue<>(v.getString(CORR), createError(v, now().toEpochMilli())))
          .to(topic(MONITOR_TOPIC));
    }
  }

  private void monitorTopic(final KStream<String, JsonObject> stream, final String name) {
    if (monitoring) {
      stream
          .filter((k, v) -> v.containsKey(CORR))
          .map(
              (k, v) ->
                  new KeyValue<>(
                      v.getString(CORR),
                      createStep(name + "-topic", MonitorSteps.REDUCE, now().toEpochMilli())))
          .to(topic(MONITOR_TOPIC));
    }
  }

  private JsonObject processNewState(
      final JsonObject oldState, final JsonObject newState, final JsonObject command) {
    return Optional.ofNullable(newState)
        .filter(state -> !hasError(state))
        .map(state -> createOps(oldState, newState))
        .filter(ops -> !ops.isEmpty())
        .map(ops -> createEvent(oldState, newState, command, ops))
        .map(
            event ->
                SideEffect.<JsonObject>run(
                        () -> aggregateCache.put(event.getString(ID), event.getJsonObject(AFTER)))
                    .andThenGet(() -> event))
        .orElse(newState);
  }

  private JsonObject reduce(final JsonObject command) {
    return tryToGet(
            () ->
                isDuplicate(command)
                    .thenComposeAsync(
                        result ->
                            FALSE.equals(result)
                                ? reduceCommand(command).thenComposeAsync(this::saveReduction)
                                : completedFuture(null))
                    .toCompletableFuture()
                    .get(),
            e -> setException(command, e))
        .orElse(null);
  }

  private CompletionStage<JsonObject> reduceCommand(final JsonObject command) {
    return getCurrentState(command.getString(ID))
        .thenApply(currentState -> makeManaged(currentState, command))
        .thenComposeAsync(
            currentState ->
                isAllowed(currentState, command, breakingTheGlass)
                    ? executeReducer(command, currentState)
                        .thenApply(newState -> processNewState(currentState, newState, command))
                    : completedFuture(accessError(command)));
  }

  private KStream<String, JsonObject> reducer() {
    return commands.mapValues(this::reduce).filter((k, v) -> v != null);
  }

  /**
   * Returns the reply stream.
   *
   * @return The reply stream.
   * @since 1.0
   */
  public KStream<String, JsonObject> replies() {
    return replies;
  }

  private void replies(final KStream<String, JsonObject> reducer) {
    replies =
        aggregates.flatMapValues(v -> list(v, createAggregateMessage(v))).merge(errors(reducer));

    replies.to(topic(REPLY_TOPIC));
  }

  private CompletionStage<JsonObject> saveReduction(final JsonObject reduction) {
    return isEvent(reduction)
        ? updateMongoEvent(plainEvent(reduction))
            .thenComposeAsync(
                result -> update(reduction.getJsonObject(AFTER), environment, database))
            .thenApply(result -> reduction)
        : completedFuture(reduction);
  }

  /**
   * Returns the topic name in the form &lt;application&gt;-&lt;type&gt;-purpose-&lt;
   * environment&gt;.
   *
   * @param purpose one of "aggregate", "command", "event", "event-full" or "reply".
   * @return The topic name.
   */
  public String topic(final String purpose) {
    return fullType() + "-" + purpose + "-" + environment;
  }

  /**
   * Returns the aggregate type.
   *
   * @return The aggregate type.
   * @since 1.0
   */
  public String type() {
    return type;
  }

  private CompletionStage<Boolean> updateMongoEvent(final JsonObject event) {
    final String id = mongoEventKey(event);

    return update(setId(event, id), id, mongoEventCollection(), database)
        .thenApply(result -> must(result, r -> r));
  }

  /**
   * Sets the audit Kafka topic, in which case auditing information is published on it. These are
   * JSON messages with the fields defined in <code>AuditFields</code>.
   *
   * @param auditTopic the Kafka topic for auditing.
   * @return The aggregate object itself.
   * @since 1.0
   * @see net.pincette.jes.util.AuditFields
   */
  public Aggregate withAudit(final String auditTopic) {
    this.auditTopic = auditTopic;

    return this;
  }

  /**
   * Sets the name of the application. This will become the prefix of the aggregate type.
   *
   * @param app the application name.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withApp(final String app) {
    this.app = app;

    return this;
  }

  /**
   * Honors the JWT field <code>breakingTheGlass</code> when checking ACLs. This should always be
   * used together with auditing.
   *
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withBreakingTheGlass() {
    breakingTheGlass = true;

    return this;
  }

  /**
   * Sets the Kafka Streams builder that will be used to create the topology.
   *
   * @param builder the given builder.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withBuilder(final StreamsBuilder builder) {
    this.builder = builder;

    return this;
  }

  /**
   * Sets a function with which a Kafka Stream can be inserted between the command topic and the
   * reducers. This method can be called several times. The result will be a function that chains
   * everything in the order of the invocations.
   *
   * @param commandProcessor the processor.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withCommandProcessor(final StreamProcessor commandProcessor) {
    final StreamProcessor previous = this.commandProcessor;

    this.commandProcessor =
        previous != null
            ? (s, b) -> commandProcessor.apply(previous.apply(s, b), b)
            : commandProcessor;

    return this;
  }

  /**
   * Sets the environment in which this aggregate will live. The default is <code>dev</code>. It
   * will become the suffix for all the topic names. Typically the value for this comes from an
   * external configuration.
   *
   * @param environment the name of the environment.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withEnvironment(final String environment) {
    this.environment = environment;

    return this;
  }

  /**
   * Turns on monitoring. This publishes monitoring messages on the
   * &lt;aggregate-type&gt;-monitor-&lt;environment&gt; topic. A message is in JSON and always
   * contains the fields "step" and "timestamp". The former is defined in <code>MonitorSteps</code>.
   * The latter is an epoch millis value. The optional field "after" contains the step that proceeds
   * this one. The optional field "command" contains the name of a command. This is turned off by
   * default.
   *
   * @param monitoring whether monitoring is desired or not.
   * @return The aggregate object itself.
   * @since 1.0
   * @see MonitorSteps
   */
  public Aggregate withMonitoring(final boolean monitoring) {
    this.monitoring = monitoring;

    return this;
  }

  /**
   * The MongoDB database in which the events and aggregates are written.
   *
   * @param database the database connection.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withMongoDatabase(final MongoDatabase database) {
    this.database = database;

    return this;
  }

  /**
   * Sets the reducer for the given command.
   *
   * @param command the name of the command, which will match the <code>_command</code> field.
   * @param reducer the reducer function.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withReducer(final String command, final Reducer reducer) {
    reducers.put(command, reducer);

    return this;
  }

  /**
   * Sets the reducer for all commands, which means the reducer does the dispatching itself. Note
   * that the individual reducers are not tried when this is set.
   *
   * @param reducer the reducer function.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withReducer(final Reducer reducer) {
    this.reducer = reducer;

    return this;
  }

  /**
   * Sets the aggregate type, which will become the suffix for the full aggregate type.
   *
   * @param type the type.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withType(final String type) {
    this.type = type;

    return this;
  }
}
