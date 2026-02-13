package net.pincette.jes;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Stream.concat;
import static javax.json.JsonValue.NULL;
import static net.pincette.jes.Command.hasError;
import static net.pincette.jes.Command.isAllowed;
import static net.pincette.jes.Command.isCommand;
import static net.pincette.jes.Commands.DELETE;
import static net.pincette.jes.Commands.PATCH;
import static net.pincette.jes.Commands.PUT;
import static net.pincette.jes.Event.isEvent;
import static net.pincette.jes.JsonFields.AFTER;
import static net.pincette.jes.JsonFields.BEFORE;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.DELETED;
import static net.pincette.jes.JsonFields.ERROR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.LANGUAGES;
import static net.pincette.jes.JsonFields.OPS;
import static net.pincette.jes.JsonFields.SEQ;
import static net.pincette.jes.JsonFields.STATUS_CODE;
import static net.pincette.jes.JsonFields.TEST;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createPatch;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getBoolean;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.merge;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Collection.exec;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.mongo.Patch.updateOperators;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.BackpressureTimout.backpressureTimeout;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Combine.combine;
import static net.pincette.rs.Commit.commit;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.NotFilter.notFilter;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.asValue;
import static net.pincette.rs.Util.carryOver;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.rs.Util.sharded;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetForever;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.function.SupplierWithException;
import net.pincette.json.JsonUtil;
import net.pincette.rs.Fanout;
import net.pincette.rs.Merge;
import net.pincette.rs.PassThrough;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.Streams;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * This class manages the state of JSON aggregates. You give it reducers, which calculate the new
 * aggregate state with the current one and an incoming command. For every aggregate instance all
 * commands are processed sequentially. The result of a reducer execution is compared with the
 * current aggregate state and the difference is emitted as an event. A reducer may also find
 * problems in the command. In that case it should return the command, marked with <code>
 * "_error": true</code>. No event will be emitted then.
 *
 * <p>The external interface at runtime is a set of topics. Their names always have the form
 * &lt;app&gt;-&lt;type&gt;-&lt;purpose&gt;[-&lt;environment&gt;]. The following topics are expected
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
 *   <dd>_seq
 *   <dt>A sequence number. If this field is present it should have the same value as that field in
 *       the aggregate instance. Otherwise the command is ignored.
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
 * @param <T> the value type for the topic source.
 * @param <U> the value type for the topic sink.
 * @author Werner Donné
 * @since 1.0
 */
public class Aggregate<T, U> {
  private static final String AGGREGATE_TOPIC = "aggregate";
  private static final Duration BACK_OFF = ofSeconds(5);
  private static final Duration BUFFER_TIMEOUT = ofMillis(50);
  private static final String COMMAND_TOPIC = "command";
  private static final Duration DUPLICATE_WINDOW = ofSeconds(5);
  private static final String EVENT_TOPIC = "event";
  private static final String EVENT_FULL_TOPIC = "event-full";
  private static final String EXCEPTION = "exception";
  private static final String MESSAGE = "message";
  private static final String REDUCER_COMMAND = "command";
  private static final String REDUCER_STATE = "state";
  private static final String REPLY_TOPIC = "reply";
  private static final String SET = "$set";
  private static final Set<String> TECHNICAL_FIELDS =
      set(COMMAND, CORR, ID, JWT, LANGUAGES, SEQ, TEST, TIMESTAMP, TYPE);
  private static final String UNIQUE_TOPIC = "unique";

  private final Map<
          String, Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>>>
      commandProcessors = new HashMap<>();
  private final Map<String, Reducer> reducers = new HashMap<>();
  private final Map<
          String, Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>>>
      reducerProcessors = new HashMap<>();
  private MongoCollection<Document> aggregateCollection;
  private String app;
  private Duration backpressureTimeout;
  private boolean breakingTheGlass;
  private Streams<String, JsonObject, T, U> builder;
  private MongoClient client;
  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> commandProcessor;
  private MongoDatabase database;
  private String environment;
  private Logger logger;
  private Reducer reducer;
  private int shards = 1;
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
        .add(MESSAGE, "Forbidden")
        .build();
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> aggregates() {
    return box(
        filter((Message<String, JsonObject> m) -> isEvent(m.value) && m.value.containsKey(AFTER)),
        map(
            m ->
                m.withValue(
                    create(() -> createObjectBuilder(m.value.getJsonObject(AFTER)))
                        .updateIf(
                            () -> ofNullable(m.value.getJsonObject(JWT)), (b, v) -> b.add(JWT, v))
                        .build()
                        .build())));
  }

  private static Optional<JsonObject> beforeWithoutTechnical(final JsonObject event) {
    return ofNullable(event.getJsonObject(BEFORE))
        .map(Aggregate::removeTechnical)
        .map(JsonObjectBuilder::build);
  }

  private static JsonObject command(final Message<String, JsonObject> message) {
    return message.value.getJsonObject(REDUCER_COMMAND);
  }

  private static String commandDuplicateKey(final JsonObject command) {
    return command.getString(ID) + command.getString(CORR) + command.getString(COMMAND);
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> commands() {
    return pipe(filter((Message<String, JsonObject> m) -> isCommand(m.value)))
        .then(map(m -> m.withKey(m.key.toLowerCase()).withValue(idsToLowerCase(m.value))))
        .then(map(m -> m.withValue(completeCommand(m.value))))
        .then(duplicateFilter(m -> commandDuplicateKey(m.value), DUPLICATE_WINDOW));
  }

  private static JsonObject completeCommand(final JsonObject command) {
    return !command.containsKey(TIMESTAMP)
        ? createObjectBuilder(command)
            .add(
                TIMESTAMP,
                ofNullable(command.getJsonNumber(TIMESTAMP))
                    .map(JsonNumber::longValue)
                    .orElseGet(() -> now().toEpochMilli()))
            .build()
        : command;
  }

  private static JsonObjectBuilder createAfter(
      final JsonObject newState, final String corr, final int seq, final long now) {
    return createObjectBuilder(newState)
        .add(CORR, corr)
        .add(SEQ, seq)
        .add(TIMESTAMP, now)
        .remove(JWT);
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
                    .add(AFTER, createAfter(newState, corr, seq, now))
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

  private static ClientSession createSession(final MongoClient client) {
    return asValue(
        toFlowPublisher(
            client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())));
  }

  private static JsonObject createSource(final JsonObject command, final JsonObject state) {
    return createObjectBuilder().add(REDUCER_STATE, state).add(REDUCER_COMMAND, command).build();
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

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> errors() {
    return filter(m -> isCommand(m.value) && hasError(m.value));
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> events() {
    return map(m -> m.withValue(plainEvent(m.value)));
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> eventsFull() {
    return filter(m -> isEvent(m.value));
  }

  private static JsonObject exception(final JsonObject command, final Throwable e) {
    return createObjectBuilder(command).add(ERROR, true).add(EXCEPTION, getStackTrace(e)).build();
  }

  private static Message<String, JsonObject> exceptionMessage(final Throwable t) {
    return message(null, exception(emptyObject(), t));
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> forCommand(
      final String command) {
    return filter(m -> command.equals(m.value.getString(COMMAND)));
  }

  private static JsonObject idsToLowerCase(final JsonObject json) {
    return createObjectBuilder(json)
        .add(ID, json.getString(ID).toLowerCase())
        .add(CORR, json.getString(CORR).toLowerCase())
        .build();
  }

  private static boolean isMatchingCommand(
      final JsonObject command, final JsonObject currentState) {
    return Optional.of(command.getInt(SEQ, -1))
        .map(seq -> seq == -1 || seq == currentState.getInt(SEQ, -1))
        .orElse(true);
  }

  private static JsonObject makeManaged(final JsonObject command, final JsonObject state) {
    return create(() -> createObjectBuilder(state))
        .updateIf(b -> !state.containsKey(ID), b -> b.add(ID, command.getString(ID)))
        .updateIf(b -> !state.containsKey(TYPE), b -> b.add(TYPE, command.getString(TYPE)))
        .build()
        .build();
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>>
      matchingFilter() {
    return filter(m -> isMatchingCommand(command(m), state(m)));
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
        ofNullable(command.getJsonArray(OPS))
            .map(ops -> createPatch(ops).apply(currentState))
            .orElse(currentState));
  }

  private static JsonObject plainEvent(final JsonObject fullEvent) {
    return createObjectBuilder(fullEvent).remove(AFTER).remove(BEFORE).build();
  }

  private static MongoDatabase prepareDatabase(final MongoDatabase database) {
    return database.withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY);
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

  private static JsonObject state(final Message<String, JsonObject> message) {
    return message.value.getJsonObject(REDUCER_STATE);
  }

  private static JsonObject technicalUpdateOperator(final JsonObject event) {
    return createObjectBuilder()
        .add(
            SET,
            createObjectBuilder()
                .add(SEQ, event.getInt(SEQ))
                .add(CORR, event.getString(CORR))
                .add(TIMESTAMP, event.getJsonNumber(TIMESTAMP)))
        .build();
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>> unique(
      final Function<JsonObject, JsonValue> uniqueFunction) {
    return pipe(map((Message<String, JsonObject> m) -> pair(m, uniqueFunction.apply(m.value))))
        .then(notFilter(pair -> pair.second.equals(NULL)))
        .then(map(pair -> pair.first.withKey(string(pair.second))));
  }

  private static CompletionStage<Boolean> updateAggregate(
      final MongoCollection<Document> collection,
      final JsonObject currentState,
      final JsonObject event,
      final ClientSession session) {
    final Bson filter = eq(ID, currentState.getString(ID));
    final List<UpdateOneModel<Document>> operators =
        concat(
                Stream.of(technicalUpdateOperator(event)),
                updateOperators(
                    currentState,
                    event.getJsonArray(OPS).stream()
                        .filter(JsonUtil::isObject)
                        .map(JsonValue::asJsonObject)))
            .map(op -> new UpdateOneModel<Document>(filter, fromJson(op)))
            .toList();
    final BulkWriteOptions options = new BulkWriteOptions().ordered(true);

    return exec(collection, c -> c.bulkWrite(session, operators, options))
        .thenApply(BulkWriteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
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

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> allowedProcessor() {
    return map(
        m ->
            isAllowed(state(m), command(m), breakingTheGlass)
                ? m
                : m.withValue(accessError(command(m))));
  }

  /**
   * This builds the <code>Streams</code> topology for the aggregate.
   *
   * @return The builder that was given before.
   * @since 1.0
   */
  public Streams<String, JsonObject, T, U> build() {
    must(app != null && builder != null && type != null && database != null && client != null);
    aggregateCollection =
        Optional.of(database).map(d -> d.getCollection(mongoAggregateCollection())).orElse(null);

    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> errors = errors();
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> eventsFull =
        eventsFull();

    return commandSource(createCommands())
        .process(
            backpressureTimeout(
                backpressureTimeout,
                () -> "No backpressure signal from the reducer of the app " + fullType()))
        .process(
            shards > 1
                ? sharded(
                    () -> box(buffer(100, BUFFER_TIMEOUT), reducer(createSession(client))),
                      // The timeout causes quicker command commits when traffic is low.
                    shards,
                    m -> m.key.hashCode())
                : reducer(createSession(client)))
        .subscribe(Fanout.of(eventsFull, errors))
        .to(topic(EVENT_FULL_TOPIC), eventsFull)
        .to(topic(REPLY_TOPIC), errors)
        .from(topic(EVENT_FULL_TOPIC), aggregates())
        .to(topic(REPLY_TOPIC))
        .from(topic(EVENT_FULL_TOPIC), aggregates())
        .to(topic(AGGREGATE_TOPIC))
        .from(topic(EVENT_FULL_TOPIC), events())
        .to(topic(EVENT_TOPIC));
  }

  private Streams<String, JsonObject, T, U> commandSource(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> commands) {
    return uniqueFunction != null
        ? builder
            .from(topic(COMMAND_TOPIC), unique(uniqueFunction))
            .to(topic(UNIQUE_TOPIC))
            .from(topic(UNIQUE_TOPIC), box(filter(m -> isCommand(m.value)), commands))
        : builder.from(topic(COMMAND_TOPIC), commands);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> createCommands() {
    return commandProcessor != null ? box(commands(), commandProcessor) : commands();
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> currentStateProcessor(
      final ClientSession session) {
    return mapAsyncSequential(
        m ->
            getForever(
                () ->
                    getCurrentState(m.value, session)
                        .thenApply(
                            state -> m.withValue(createSource(keepId(m.value, state), state)))));
  }

  private CompletionStage<Boolean> deleteMongoAggregate(
      final JsonObject aggregate, final ClientSession session) {
    trace(aggregate, a -> true, () -> "Deleting aggregate " + string(aggregate));

    return deleteOne(aggregateCollection, session, eq(ID, aggregate.getString(ID)))
        .thenApply(DeleteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private Optional<CompletionStage<Boolean>> deleteReduction(
      final JsonObject reduction, final ClientSession session) {
    return getBoolean(reduction, "/" + AFTER + "/" + DELETED)
        .filter(deleted -> deleted)
        .map(deleted -> deleteMongoAggregate(reduction.getJsonObject(AFTER), session));
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

  private String environmentSuffix() {
    return environment != null ? ("-" + environment) : "";
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> epilogueProcessor(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> preprocessor,
      final ClientSession session) {
    return pipe(preprocessor)
        .then(buffer(1)) // Ensure serialisation.
        .then(currentStateProcessor(session))
        .then(matchingFilter())
        .then(allowedProcessor());
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

  private CompletionStage<JsonObject> getCurrentState(
      final JsonObject command, final ClientSession session) {
    return getMongoCurrentState(command, session).thenApply(state -> makeManaged(command, state));
  }

  private <R> CompletionStage<R> getForever(final SupplierWithException<CompletionStage<R>> get) {
    return tryToGetForever(get, BACK_OFF, this::logException);
  }

  private CompletionStage<JsonObject> getMongoCurrentState(
      final JsonObject command, final ClientSession session) {
    return findOne(aggregateCollection, session, mongoStateCriterion(command))
        .thenApply(currentState -> currentState.orElseGet(JsonUtil::emptyObject));
  }

  private CompletionStage<JsonObject> handleAggregate(
      final JsonObject reduction, final ClientSession session) {
    return tryWith(() -> deleteReduction(reduction, session).orElse(null))
        .or(() -> updateReductionNew(reduction, session).orElse(null))
        .or(() -> updateReductionExisting(reduction, session).orElse(null))
        .get()
        .map(result -> result.thenApply(res -> must(res, r -> r)).thenApply(res -> reduction))
        .orElseGet(() -> completedFuture(reduction));
  }

  private JsonObject keepId(final JsonObject command, final JsonObject currentState) {
    return uniqueFunction != null
        ? createObjectBuilder(command).add(ID, currentState.getString(ID)).build()
        : command;
  }

  private void logException(final Throwable e) {
    if (logger != null) {
      logger.log(SEVERE, e, () -> type() + ": " + e.getMessage());
    }
  }

  private String mongoAggregateCollection() {
    return fullType() + environmentSuffix();
  }

  private Bson mongoStateCriterion(final JsonObject command) {
    return ofNullable(uniqueFunction)
        .map(f -> f.apply(command))
        .map(this::mongoStateQuery)
        .orElseGet(() -> eq(ID, command.getString(ID)));
  }

  private Bson mongoStateQuery(final JsonValue value) {
    return fromJson(
        isObject(value)
            ? value.asJsonObject()
            : createObjectBuilder()
                .add(
                    "$expr",
                    createObjectBuilder()
                        .add("$eq", createArrayBuilder().add(uniqueExpression).add(value)))
                .build());
  }

  private JsonObject processNewState(
      final JsonObject oldState, final JsonObject newState, final JsonObject command) {
    return ofNullable(newState)
        .filter(state -> !hasError(state))
        .map(state -> createOps(oldState, newState))
        .filter(ops -> !ops.isEmpty())
        .map(ops -> createEvent(oldState, newState, command, ops))
        .orElse(newState);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reduceProcessor(
      final Reducer reducer) {
    return mapAsyncSequential(
        m ->
            tryToGet(
                    () ->
                        reducer
                            .apply(command(m), state(m))
                            .thenApply(m::withValue)
                            .exceptionally(Aggregate::exceptionMessage),
                    t -> completedFuture(exceptionMessage(t)))
                .orElse(null));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducer(
      final ClientSession session) {
    return pipe(map(
            (Message<String, JsonObject> m) ->
                m.withValue(trace(m.value, j -> true, () -> "Reducing " + string(m.value)))))
        .then(reducer != null ? reducerGeneral(session) : reducerSpecific(session))
        .then(
            map(
                m ->
                    m.withValue(
                        trace(
                            m.value,
                            j -> true,
                            () ->
                                "Reduction result: "
                                    + (m.value != null ? string(m.value) : "null")))))
        .then(filter(m -> m.value != null && !m.value.isEmpty()));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducerGeneral(
      final ClientSession session) {
    return reducerProcessor(passThrough(), reduceProcessor(reducer), session);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducerProcessor(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> preprocessor,
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducer,
      final ClientSession session) {
    final var epilogue = epilogueProcessor(preprocessor, session);
    final var errors = filter((Message<String, JsonObject> m) -> hasError(m.value));
    final var red = reducerProcessor(reducer, session);

    epilogue.subscribe(Fanout.of(errors, red));

    return combine(epilogue, Merge.of(errors, red));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducerProcessor(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducer,
      final ClientSession session) {
    return pipe(filter((Message<String, JsonObject> m) -> !hasError(m.value)))
        .then(
            carryOver(
                reducer,
                m -> pair(command(m), state(m)),
                (newState, pair) ->
                    hasError(newState.value)
                        ? newState.withValue(merge(pair.first, newState.value))
                        : newState.withValue(
                            processNewState(pair.second, newState.value, pair.first))))
        .then(saveProcessor(session))
        .then(buffer(1)); // Ensure serialisation.
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducerProcessor(
      final String command,
      final Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>> reducer,
      final ClientSession session) {
    return pipe(forCommand(command))
        .then(
            reducerProcessor(
                ofNullable(commandProcessors.get(command))
                    .map(Supplier::get)
                    .orElseGet(PassThrough::passThrough),
                reducer.get(),
                session));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> reducerSpecific(
      final ClientSession session) {
    final List<Processor<Message<String, JsonObject>, Message<String, JsonObject>>> processors =
        concat(
                reducerProcessors.entrySet().stream()
                    .map(e -> reducerProcessor(e.getKey(), e.getValue(), session)),
                reducers.entrySet().stream()
                    .map(
                        e ->
                            reducerProcessor(
                                e.getKey(), () -> reduceProcessor(e.getValue()), session)))
            .toList();
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> all = passThrough();

    all.subscribe(Fanout.of(processors));

    return combine(all, Merge.of(processors));
  }

  private CompletionStage<JsonObject> save(final JsonObject newState, final ClientSession session) {
    return saveReduction(newState, json -> handleAggregate(json, session));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> saveProcessor(
      final ClientSession session) {
    // By putting this in commit phase, we are sure the event is first published on the Kafka
    // topic. If saving to MongoDB would fail, then the command will be delivered again with the
    // old state. If the save occurred first, then the command would see the new state, which
    // would probably have no effect. The consequence is that the event message is logically lost.
    // By saving last, the event may be emitted twice, but that preserves the at least once
    // semantics.
    return commit(
        list ->
            !hasError(list.getFirst().value) // Everything is serialised.
                ? getForever(() -> save(list.getFirst().value, session).thenApply(json -> true))
                : completedFuture(true));
  }

  private CompletionStage<JsonObject> saveReduction(
      final JsonObject reduction,
      final Function<JsonObject, CompletionStage<JsonObject>> handleAggregate) {
    return isEvent(reduction) ? handleAggregate.apply(reduction) : completedFuture(reduction);
  }

  /**
   * Returns the topic name in the form &lt;application&gt;-&lt;type&gt;-purpose-&lt;
   * environment&gt;.
   *
   * @param purpose one of "aggregate", "command", "event", "event-full" or "reply".
   * @return The topic name.
   */
  public String topic(final String purpose) {
    return fullType() + "-" + purpose + environmentSuffix();
  }

  private <V> V trace(final V value, final Predicate<V> test, final Supplier<String> message) {
    if (logger != null && test.test(value)) {
      logger.finest(() -> currentTimeMillis() + " " + message.get());
    }

    return value;
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

  private CompletionStage<Boolean> updateMongoAggregate(
      final JsonObject aggregate, final ClientSession session) {
    trace(aggregate, a -> true, () -> "Replacing aggregate " + string(aggregate));

    return update(aggregateCollection, aggregate, session)
        .thenApply(result -> must(result, r -> r));
  }

  private Optional<CompletionStage<Boolean>> updateReductionNew(
      final JsonObject reduction, final ClientSession session) {
    return beforeWithoutTechnical(reduction)
        .filter(JsonObject::isEmpty)
        .map(before -> updateMongoAggregate(reduction.getJsonObject(AFTER), session));
  }

  private Optional<CompletionStage<Boolean>> updateReductionExisting(
      final JsonObject reduction, final ClientSession session) {
    return beforeWithoutTechnical(reduction)
        .filter(before -> !before.isEmpty())
        .map(
            before ->
                trace(
                    before,
                    b -> true,
                    () -> "Updating aggregate " + string(reduction.getJsonObject(AFTER))))
        .map(
            before ->
                updateAggregate(
                    aggregateCollection, reduction.getJsonObject(BEFORE), reduction, session));
  }

  /**
   * Sets the name of the application. This will become the prefix of the aggregate type.
   *
   * @param app the application name.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate<T, U> withApp(final String app) {
    this.app = app;

    return this;
  }

  public Aggregate<T, U> withBackpressureTimeout(final Duration backpressureTimeout) {
    this.backpressureTimeout = backpressureTimeout;

    return this;
  }

  /**
   * Honors the JWT field <code>breakingTheGlass</code> when checking ACLs. This should always be
   * used together with auditing.
   *
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate<T, U> withBreakingTheGlass() {
    breakingTheGlass = true;

    return this;
  }

  /**
   * Sets the <code>Streams</code> builder that will be used to create the topology.
   *
   * @param builder the given builder.
   * @return The aggregate object itself.
   * @since 3.0
   */
  public Aggregate<T, U> withBuilder(final Streams<String, JsonObject, T, U> builder) {
    this.builder = builder;

    return this;
  }

  /**
   * Inserts a processor between the command topic and the reducers.
   *
   * @param commandProcessor the processor.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate<T, U> withCommandProcessor(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> commandProcessor) {
    this.commandProcessor = commandProcessor;

    return this;
  }

  /**
   * Inserts a processor between the command topic and a specific reducer. If there also is a
   * general preprocessor, then it will come before it.
   *
   * @param command the name of the command.
   * @param commandProcessor the processor.
   * @return The aggregate object itself.
   * @since 3.1
   */
  public Aggregate<T, U> withCommandProcessor(
      final String command,
      final Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>>
          commandProcessor) {
    commandProcessors.put(command, commandProcessor);

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
  public Aggregate<T, U> withEnvironment(final String environment) {
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
  public Aggregate<T, U> withLogger(final Logger logger) {
    this.logger = logger;

    return this;
  }

  /**
   * Sets the MongoDB client that is needed to create a session.
   *
   * @param client the given client.
   * @return The aggregate object itself.
   * @since 4.0
   */
  public Aggregate<T, U> withMongoClient(final MongoClient client) {
    this.client = client;

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
  public Aggregate<T, U> withMongoDatabase(final MongoDatabase database) {
    this.database = prepareDatabase(database);

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
  public Aggregate<T, U> withReducer(final String command, final Reducer reducer) {
    if (!reducerProcessors.containsKey(command)) {
      reducers.put(command, reducer);
    }

    return this;
  }

  /**
   * Sets the reducer for the given command. The processor will receive messages with the <code>
   * command</code> and <code>aggregate</code> fields. It should produce the new aggregate. The ID
   * of the messages is the aggregate ID.
   *
   * @param command the name of the command, which will match the <code>_command</code> field.
   * @param reducer the reducer processor.
   * @return The aggregate object itself.
   * @since 3.1
   */
  public Aggregate<T, U> withReducer(
      final String command,
      final Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>> reducer) {
    reducerProcessors.put(command, reducer);
    reducers.remove(command);

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
  public Aggregate<T, U> withReducer(final Reducer reducer) {
    this.reducer = reducer;

    return this;
  }

  /**
   * Sets the number of shards, which increases parallelism through consistent hashing.
   *
   * @param shards the number of shards.
   * @return The aggregate object itself.
   * @since 4.0.0
   */
  public Aggregate<T, U> withShards(final int shards) {
    this.shards = shards;

    return this;
  }

  /**
   * Sets the aggregate type, which will become the suffix for the full aggregate type.
   *
   * @param type the type.
   * @return The aggregate object itself.
   * @since 1.0
   */
  public Aggregate<T, U> withType(final String type) {
    this.type = type;

    return this;
  }

  /**
   * When the expression is given it is used on commands to obtain an alternate unique key. This can
   * be used to avoid the creation of duplicates according to some business criterion. The "unique"
   * topic must exist in this case.
   *
   * @param expression a MongoDB expression.
   * @return The aggregate object itself.
   * @since 1.2
   */
  public Aggregate<T, U> withUniqueExpression(final JsonValue expression) {
    uniqueExpression = expression;
    uniqueFunction = function(expression);

    return this;
  }
}
