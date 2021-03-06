package net.pincette.jes;

import static com.mongodb.WriteConcern.MAJORITY;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Boolean.FALSE;
import static java.lang.String.valueOf;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static javax.json.JsonValue.NULL;
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
import static net.pincette.jes.util.Mongo.addNotDeleted;
import static net.pincette.jes.util.Mongo.restore;
import static net.pincette.jes.util.Mongo.updateAggregate;
import static net.pincette.jes.util.Streams.duplicateFilter;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createPatch;
import static net.pincette.json.JsonUtil.getBoolean;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.insert;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToGet;

import com.mongodb.ReadConcern;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.jes.util.AuditFields;
import net.pincette.jes.util.Reducer;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;
import net.pincette.util.TimedCache;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.bson.Document;
import org.bson.conversions.Bson;

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
  private static final String REDUCER_COMMAND = "command";
  private static final String REDUCER_STATE = "state";
  private static final String REPLY_TOPIC = "reply";
  private static final String STEP = "step";
  private static final String STEP_AFTER = "after";
  private static final String STEP_COMMAND = "command";
  private static final String STEP_ERROR = "error";
  private static final String STEP_TIMESTAMP = "timestamp";
  private static final Set<String> TECHNICAL_FIELDS =
      set(COMMAND, CORR, ID, JWT, LANGUAGES, SEQ, TEST, TIMESTAMP, TYPE);
  private static final String UNIQUE_TOPIC = "unique";
  private final Map<String, Reducer> reducers = new HashMap<>();
  private final TimedCache<String, JsonObject> aggregateCache = new TimedCache<>(DUPLICATE_WINDOW);
  private String auditTopic;
  private MongoCollection<Document> aggregateCollection;
  private KStream<String, JsonObject> aggregates;
  private String app;
  private boolean breakingTheGlass;
  private StreamsBuilder builder;
  private StreamProcessor commandProcessor;
  private KStream<String, JsonObject> commands;
  private MongoDatabase database;
  private MongoDatabase databaseArchive;
  private String environment = "dev";
  private MongoCollection<Document> eventCollection;
  private KStream<String, JsonObject> events;
  private KStream<String, JsonObject> eventsFull;
  private Logger logger;
  private KStream<String, JsonObject> monitor;
  private boolean monitoring;
  private Reducer reducer;
  private KStream<String, JsonObject> replies;
  private String type;
  private JsonValue uniqueExpression;
  private Function<JsonObject, JsonValue> uniqueFunction;

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

  private static String commandDuplicateKey(final JsonObject command) {
    return command.getString(ID) + command.getString(CORR) + command.getString(COMMAND);
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

  private static JsonObject createSource(final JsonObject command, final JsonObject state) {
    return createObjectBuilder().add(REDUCER_STATE, state).add(REDUCER_COMMAND, command).build();
  }

  private static JsonObject createStep(
      final String step, final String after, final long timestamp) {
    return createStep(step, after, timestamp, null);
  }

  private static JsonObject createStep(
      final String step, final String after, final long timestamp, final String command) {
    return create(JsonUtil::createObjectBuilder)
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
    return create(() -> createObjectBuilder(state))
        .updateIf(b -> !state.containsKey(ID), b -> b.add(ID, command.getString(ID)))
        .updateIf(b -> !state.containsKey(TYPE), b -> b.add(TYPE, command.getString(TYPE)))
        .build()
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

  /**
   * Wraps a generic transformer in a <code>Reducer</code>. The first argument will be the command
   * and the second the current state of the aggregate.
   *
   * @param transformer the given transformer.
   * @return The wrapped transformer.
   * @since 1.1.4
   */
  public static Reducer reducer(final BinaryOperator<JsonObject> transformer) {
    return (command, state) -> completedFuture(transformer.apply(command, state));
  }

  /**
   * Wraps a sequence of generic transformers in a <code>Reducer</code>. The result of one
   * transformer is fed to the next. The JSON object that is given to the sequence has the fields
   * <code>command</code> and <code>state</code>. The transformer should produce the new state.
   *
   * @param transformers the given transformer sequence.
   * @return The wrapped transformer sequence.
   * @since 1.1.4
   */
  @SafeVarargs
  public static Reducer reducer(final UnaryOperator<JsonObject>... transformers) {
    final UnaryOperator<JsonObject> function =
        stream(transformers).reduce(json -> json, (result, t) -> (j -> t.apply(result.apply(j))));

    return (command, state) -> completedFuture(function.apply(createSource(command, state)));
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

  private static JsonObject uniqueError(final JsonObject command) {
    return createObjectBuilder(command)
        .add(ERROR, true)
        .add(STATUS_CODE, 400)
        .add("message", "Missing unique expression fields")
        .build();
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

    aggregateCollection = database.getCollection(mongoAggregateCollection());
    eventCollection = database.getCollection(mongoEventCollection());
    commands = createCommands();
    unique();

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

  private String cacheKey(final JsonObject command) {
    return uniqueFunction != null ? string(commandKey(command)) : command.getString(ID);
  }

  private JsonObject checkUnique(final JsonObject command) {
    return uniqueFunction != null && commandKey(command) == null ? uniqueError(command) : command;
  }

  private JsonValue commandKey(final JsonObject command) {
    return ofNullable(uniqueFunction)
        .map(f -> f.apply(command))
        .filter(k -> !k.equals(NULL))
        .orElse(null);
  }

  private KStream<String, JsonObject> commandSource() {
    return uniqueFunction != null ? builder.stream(topic(UNIQUE_TOPIC)) : commands;
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
        duplicateFilter(
            com.filter((k, v) -> isCommand(v))
                .map((k, v) -> new KeyValue<>(k.toLowerCase(), idsToLowerCase(v)))
                .mapValues(Aggregate::completeCommand),
            (k, v) -> commandDuplicateKey(v),
            ofSeconds(60));

    monitorCommands(commandFilter);

    return (commandProcessor != null
        ? commandProcessor.apply(commandFilter, builder)
        : commandFilter);
  }

  private CompletionStage<Boolean> deleteMongoAggregate(final JsonObject aggregate) {
    trace(aggregate, a -> true, () -> "Deleting aggregate " + string(aggregate));

    return deleteOne(aggregateCollection, eq(ID, aggregate.getString(ID)))
        .thenApply(result -> must(result, DeleteResult::wasAcknowledged))
        .thenApply(result -> true);
  }

  private CompletionStage<Boolean> deleteMongoEvent(final JsonObject event) {
    trace(event, e -> true, () -> "Deleting event " + string(event));

    return deleteOne(eventCollection, eq(ID, mongoEventKey(event)))
        .thenApply(result -> must(result, DeleteResult::wasAcknowledged))
        .thenApply(result -> true);
  }

  private Optional<CompletionStage<Boolean>> deleteReduction(final JsonObject reduction) {
    return getBoolean(reduction, "/" + AFTER + "/" + DELETED)
        .filter(deleted -> deleted)
        .map(deleted -> deleteMongoAggregate(reduction.getJsonObject(AFTER)));
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

  private CompletionStage<Pair<JsonObject, Boolean>> getCurrentState(final JsonObject command) {
    return aggregateCache
        .get(cacheKey(command))
        .map(
            currentState ->
                (CompletionStage<Pair<JsonObject, Boolean>>)
                    completedFuture(pair(currentState, false)))
        .orElseGet(() -> getMongoCurrentState(command));
  }

  private CompletionStage<Pair<JsonObject, Boolean>> getMongoCurrentState(
      final JsonObject command) {
    return findOne(aggregateCollection, mongoStateCriterion(command))
        .thenComposeAsync(
            currentState ->
                currentState
                    .map(
                        state ->
                            (CompletionStage<Pair<JsonObject, Boolean>>)
                                completedFuture(pair(state, false)))
                    .orElseGet(
                        () -> restoreFromCommand(command).thenApply(state -> pair(state, true))));
  }

  private CompletionStage<JsonObject> handleAggregate(
      final JsonObject reduction, final boolean reconstructed) {
    return tryWith(() -> deleteReduction(reduction).orElse(null))
        .or(() -> updateReductionNew(reduction, reconstructed).orElse(null))
        .or(() -> updateReductionExisting(reduction, reconstructed).orElse(null))
        .get()
        .map(result -> result.thenApply(res -> must(res, r -> r)).thenApply(res -> reduction))
        .orElseGet(() -> completedFuture(reduction));
  }

  private CompletionStage<Boolean> insertMongoEvent(final JsonObject event) {
    trace(event, e -> true, () -> "Inserting event " + string(event));

    return insert(eventCollection, setId(event, mongoEventKey(event)))
        .thenApply(result -> must(result, r -> r));
  }

  private CompletionStage<Boolean> isDuplicate(final JsonObject command) {
    return aggregate(
            eventCollection,
            list(
                match(
                    and(
                        regex(ID, "^" + command.getString(ID) + ".*"),
                        eq(CORR, command.getString(CORR)),
                        eq(COMMAND, command.getString(COMMAND)))),
                project(include(ID))))
        .thenApply(
            result ->
                trace(result, r -> !r.isEmpty(), () -> "Duplicate command " + string(command)))
        .thenApply(result -> !result.isEmpty());
  }

  private JsonObject keepId(final JsonObject currentState, final JsonObject command) {
    return uniqueFunction != null
        ? createObjectBuilder(command).add(ID, currentState.getString(ID)).build()
        : command;
  }

  private void logException(final Throwable e) {
    if (logger != null) {
      logger.log(SEVERE, e.getMessage(), e);
    }
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

  private Bson mongoStateCriterion(final JsonObject command) {
    return ofNullable(uniqueFunction)
        .map(f -> f.apply(command))
        .map(this::mongoStateQuery)
        .orElseGet(() -> eq(ID, command.getString(ID)));
  }

  private Bson mongoStateQuery(final JsonValue value) {
    return addNotDeleted(
        fromJson(
            isObject(value)
                ? value.asJsonObject()
                : createObjectBuilder()
                    .add(
                        "$expr",
                        createObjectBuilder()
                            .add("$eq", createArrayBuilder().add(uniqueExpression).add(value)))
                    .build()));
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

  private CompletionStage<JsonObject> processCommand(final JsonObject command) {
    return reduceCommand(command)
        .thenComposeAsync(
            pair ->
                saveReduction(pair.first, reduction -> handleAggregate(reduction, pair.second)));
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
                        () -> aggregateCache.put(cacheKey(command), event.getJsonObject(AFTER)))
                    .andThenGet(() -> event))
        .orElse(newState);
  }

  private JsonObject reduce(final JsonObject command) {
    return tryToGet(
            () ->
                isDuplicate(command)
                    .thenComposeAsync(
                        result ->
                            FALSE.equals(result) ? processCommand(command) : completedFuture(null))
                    .toCompletableFuture()
                    .get(),
            e ->
                SideEffect.<JsonObject>run(() -> logException(e))
                    .andThenGet(() -> setException(command, e)))
        .orElse(null);
  }

  private CompletionStage<Pair<JsonObject, Boolean>> reduceCommand(final JsonObject command) {
    final JsonObject checked = checkUnique(command);

    return hasError(checked)
        ? completedFuture(pair(checked, false))
        : getCurrentState(command)
            .thenApply(pair -> pair(makeManaged(pair.first, command), pair.second))
            .thenComposeAsync(
                pair ->
                    reduceIfAllowed(pair.first, keepId(pair.first, command))
                        .thenApply(newState -> pair(newState, pair.second)));
  }

  private CompletionStage<JsonObject> reduceIfAllowed(
      final JsonObject currentState, final JsonObject command) {
    return isAllowed(currentState, command, breakingTheGlass)
        ? executeReducer(command, currentState)
            .thenApply(newState -> processNewState(currentState, newState, command))
        : completedFuture(accessError(command));
  }

  private KStream<String, JsonObject> reducer() {
    return commandSource()
        .mapValues(json -> trace(json, j -> true, () -> "Reducing " + string(json)))
        .mapValues(this::reduce)
        .mapValues(
            json ->
                trace(
                    json,
                    j -> true,
                    () -> "Reduction result: " + (json != null ? string(json) : "null")))
        .filter((k, v) -> v != null);
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

  private CompletionStage<JsonObject> restoreFromCommand(final JsonObject command) {
    return restore(command.getString(ID), fullType(), environment, databaseArchive);
  }

  private CompletionStage<JsonObject> saveReduction(
      final JsonObject reduction,
      final Function<JsonObject, CompletionStage<JsonObject>> handleAggregate) {
    return isEvent(reduction)
        ? insertMongoEvent(plainEvent(reduction))
            .thenComposeAsync(
                result ->
                    handleAggregate
                        .apply(reduction)
                        .exceptionally(
                            e -> {
                              deleteMongoEvent(reduction);
                              rethrow(e);
                              return null;
                            }))
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

  private <T> T trace(final T value, final Predicate<T> test, final Supplier<String> message) {
    if (logger != null && test.test(value)) {
      logger.finest(message);
    }

    return value;
  }

  private void unique() {
    if (uniqueFunction != null) {
      final KStream<String, Pair<JsonObject, JsonValue>> applied =
          commands.mapValues(v -> pair(v, uniqueFunction.apply(v)));

      applied
          .filter((k, p) -> !p.second.equals(NULL))
          .map((k, p) -> new KeyValue<>(string(p.second), p.first))
          .to(topic(UNIQUE_TOPIC));
    }
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

  private CompletionStage<Boolean> updateMongoAggregate(final JsonObject aggregate) {
    trace(aggregate, a -> true, () -> "Replacing aggregate " + string(aggregate));

    return update(aggregateCollection, aggregate).thenApply(result -> must(result, r -> r));
  }

  private Optional<CompletionStage<Boolean>> updateReductionNew(
      final JsonObject reduction, final boolean reconstructed) {
    return Optional.of(reduction.getJsonObject(BEFORE))
        .filter(before -> reconstructed)
        .map(before -> updateMongoAggregate(reduction.getJsonObject(AFTER)));
  }

  private Optional<CompletionStage<Boolean>> updateReductionExisting(
      final JsonObject reduction, final boolean reconstructed) {
    return Optional.of(reduction.getJsonObject(BEFORE))
        .filter(before -> !reconstructed)
        .map(
            before ->
                trace(
                    before,
                    b -> true,
                    () -> "Updating aggregate " + string(reduction.getJsonObject(AFTER))))
        .map(
            before ->
                updateAggregate(aggregateCollection, reduction.getJsonObject(BEFORE), reduction));
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
   * Sets the logger, which will be used when the log level is <code>FINEST</code>.
   *
   * @param logger the logger.
   * @return The aggregate object itself.
   * @since 1.3
   */
  public Aggregate withLogger(final Logger logger) {
    this.logger = logger;

    return this;
  }

  /**
   * The MongoDB database in which the events and aggregates are written. The database will be used
   * with majority read and write concerns.
   *
   * @param database the database connection.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate withMongoDatabase(final MongoDatabase database) {
    this.database = database.withReadConcern(ReadConcern.LINEARIZABLE).withWriteConcern(MAJORITY);

    if (this.databaseArchive == null) {
      this.databaseArchive = database;
    }

    return this;
  }

  /**
   * The MongoDB archive database that is used to reconstruct aggregate instances from the event
   * log. This is for the MongoDB Atlas Online Archive feature. It may be <code>null</code>, in
   * which case the regular database connection will be used.
   *
   * @param database the database connection.
   * @return The aggregate object itself.
   * @since 1.2.5
   */
  public Aggregate withMongoDatabaseArchive(final MongoDatabase database) {
    this.databaseArchive = database != null ? database : this.database;

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

  /**
   * When the expression is given it is used on commands to obtain an alternate unique key. This can
   * be used to avoid the creation of duplicates according to some business criterion. The "unique"
   * Kafka topic must exist in this case.
   *
   * @param expression a MongoDB expression.
   * @return The aggregate object itself.
   * @since 1.2
   */
  public Aggregate withUniqueExpression(final JsonValue expression) {
    uniqueExpression = expression;
    uniqueFunction = function(expression);

    return this;
  }
}
