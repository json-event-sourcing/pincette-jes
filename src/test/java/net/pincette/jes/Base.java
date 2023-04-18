package net.pincette.jes;

import static java.lang.Integer.MAX_VALUE;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.LogManager.getLogManager;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.Commands.GET;
import static net.pincette.jes.Commands.PATCH;
import static net.pincette.jes.Commands.PUT;
import static net.pincette.jes.JsonFields.ACL_WRITE;
import static net.pincette.jes.JsonFields.AFTER;
import static net.pincette.jes.JsonFields.BEFORE;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.SEQ;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Reducer.forEachJoin;
import static net.pincette.rs.kafka.KafkaPublisher.publisher;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.rs.kafka.Util.fromPublisher;
import static net.pincette.rs.kafka.Util.fromSubscriber;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.pincette.kafka.json.JsonDeserializer;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.rs.Source;
import net.pincette.rs.kafka.ConsumerEvent;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.Streams;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class Base {
  static final JsonObject ACL = o(f(GET, a(v("test"))), f(ACL_WRITE, a(v("test"))));
  static final String AGGREGATE = "aggregate";
  static final String APP = "test";
  static final String COMMAND = "command";
  static final String EVENT = "event";
  static final String EVENT_FULL = "event-full";
  static final String MINUS = "minus";
  static final String PLUS = "plus";
  static final String REPLY = "reply";
  static final String TYPE = "test";
  static final String UNIQUE = "unique";
  static final String VALUE = "value";
  static final Map<String, Object> COMMON_CONFIG =
      map(pair(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
  static final Map<String, Object> CONSUMER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
              pair(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class),
              pair(GROUP_ID_CONFIG, "test-" + randomUUID()),
              pair(ENABLE_AUTO_COMMIT_CONFIG, false)));
  static final Map<String, Object> PRODUCER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(ACKS_CONFIG, "all"),
              pair(ENABLE_IDEMPOTENCE_CONFIG, true),
              pair(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
              pair(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class)));
  private static final Set<String> TOPICS =
      set(COMMAND, EVENT, EVENT_FULL, AGGREGATE, REPLY, UNIQUE);

  static {
    tryToDoRethrow(
        () ->
            getLogManager()
                .readConfiguration(Base.class.getResourceAsStream("/logging.properties")));
  }

  protected static Resources resources;

  @AfterAll
  public static void afterAll() {
    resources.close();
  }

  private static Streams<
          String,
          JsonObject,
          ConsumerRecord<String, JsonObject>,
          ProducerRecord<String, JsonObject>>
      aggregate(
          final Streams<
                  String,
                  JsonObject,
                  ConsumerRecord<String, JsonObject>,
                  ProducerRecord<String, JsonObject>>
              streams,
          final String environment,
          final boolean withProcessor) {
    final var aggregate =
        new Aggregate<ConsumerRecord<String, JsonObject>, ProducerRecord<String, JsonObject>>()
            .withApp(APP)
            .withType(TYPE)
            .withMongoDatabase(resources.database)
            .withEnvironment(environment)
            .withBuilder(streams)
            .withLogger(getLogger("net.pincette.jes.test"))
            .withUniqueExpression(createValue("$" + UNIQUE))
            .withReducer(PATCH, Aggregate::patch)
            .withReducer(
                PUT,
                (command, currentState) ->
                    completedFuture(
                        createObjectBuilder(command)
                            .remove(JsonFields.COMMAND)
                            .add(JsonFields.ACL, ACL)
                            .build()));

    return (withProcessor
            ? aggregate
                .withReducer(PLUS, reduceProcessor(v -> v + 1))
                .withReducer(MINUS, reduceProcessor(v -> v - 1))
                .withCommandProcessor(PLUS, map(c -> c))
                .withCommandProcessor(PATCH, map(c -> c))
            : aggregate
                .withReducer(PLUS, (command, currentState) -> reduce(currentState, v -> v + 1))
                .withReducer(MINUS, (command, currentState) -> reduce(currentState, v -> v - 1)))
        .build();
  }

  @BeforeAll
  public static void beforeAll() {
    resources = new Resources();
  }

  private static void cleanUpCollections() {
    final var type = fullType();

    forEachJoin(
        with(toFlowPublisher(resources.database.listCollectionNames()))
            .filter(name -> name.startsWith(type))
            .get(),
        Base::drop);
  }

  private static int compare(final String v1, final String v2) {
    return v1 != null && v2 != null ? v1.compareTo(v2) : compareAbsent(v1, v2);
  }

  private static int compare(final JsonObject o1, final JsonObject o2) {
    return tryWith(
            () ->
                compareResult(
                    o1.getString(JsonFields.TYPE).compareTo(o2.getString(JsonFields.TYPE))))
        .or(() -> compareResult(compare(o1.getString(ID, null), o2.getString(ID, null))))
        .or(() -> compareResult(compare(o1.getString(CORR, null), o2.getString(CORR, null))))
        .or(() -> compareResult(o1.getInt(SEQ, MAX_VALUE) - o2.getInt(SEQ, MAX_VALUE)))
        .get()
        .orElse(0);
  }

  private static <T> int compareAbsent(final T v1, final T v2) {
    final Supplier<Integer> tryOther = () -> v1 != null && v2 == null ? -1 : 0;

    return v1 == null && v2 != null ? 1 : tryOther.get();
  }

  private static Optional<Integer> compareResult(final int result) {
    return Optional.of(result).filter(r -> r != 0);
  }

  private static KafkaConsumer<String, JsonObject> consumer() {
    return new KafkaConsumer<>(CONSUMER_CONFIG);
  }

  private static void createTopics(final String environment) {
    tryToDoWithRethrow(
        () -> Admin.create(COMMON_CONFIG),
        admin ->
            net.pincette.rs.kafka.Util.createTopics(
                    TOPICS.stream().map(name -> newTopic(name, environment)).collect(toSet()),
                    admin)
                .toCompletableFuture()
                .join());
  }

  private static Streams<
          String,
          JsonObject,
          ConsumerRecord<String, JsonObject>,
          ProducerRecord<String, JsonObject>>
      createStreams() {
    return Streams.streams(
        fromPublisher(
            publisher(Base::consumer)
                .withEventHandler(
                    (event, consumer) -> {
                      if (event == ConsumerEvent.STARTED) {
                        consumer.seekToBeginning(consumer.assignment());
                      }
                    })),
        fromSubscriber(subscriber(Base::producer)));
  }

  private static void deleteTopics(final String environment) {
    tryToDoWithRethrow(
        () -> Admin.create(COMMON_CONFIG),
        admin ->
            net.pincette.rs.kafka.Util.deleteTopics(
                    TOPICS.stream().map(name -> topic(name, environment)).collect(toSet()), admin)
                .toCompletableFuture()
                .join());
  }

  private static void drop(final String collection) {
    forEachJoin(toFlowPublisher(resources.database.getCollection(collection).drop()), v -> {});
  }

  static String fullType() {
    return APP + "-" + TYPE;
  }

  private static List<Message<String, JsonObject>> inputMessages(final List<JsonObject> messages) {
    return messages.stream()
        .map(
            m ->
                message(
                    ofNullable(m.getString(ID, null)).orElseGet(() -> randomUUID().toString()), m))
        .collect(toList());
  }

  private static List<JsonObject> loadMessages(final String resource) {
    return ofNullable(Base.class.getResource(resource))
        .flatMap(r -> tryToGetRethrow(r::toURI))
        .map(URI::getPath)
        .map(File::new)
        .map(File::listFiles)
        .map(Arrays::stream)
        .map(Stream::sorted)
        .map(stream -> stream.map(Base::readJson).collect(toList()))
        .orElseGet(Collections::emptyList);
  }

  private static JsonObject newState(final JsonObject currentState, final IntUnaryOperator op) {
    return createObjectBuilder(currentState)
        .add(VALUE, op.applyAsInt(currentState.getInt(VALUE, 0)))
        .add(JsonFields.ACL, ACL)
        .build();
  }

  private static NewTopic newTopic(final String name, final String environment) {
    return new NewTopic(topic(name, environment), 1, (short) 1);
  }

  private static KafkaProducer<String, JsonObject> producer() {
    return new KafkaProducer<>(PRODUCER_CONFIG);
  }

  private static JsonObject readJson(final File file) {
    return tryToGetWithRethrow(() -> createReader(new FileReader(file)), JsonReader::readObject)
        .orElse(null);
  }

  private static CompletionStage<JsonObject> reduce(
      final JsonObject currentState, final IntUnaryOperator op) {
    return completedFuture(newState(currentState, op));
  }

  private static Processor<Message<String, JsonObject>, Message<String, JsonObject>>
      reduceProcessor(final IntUnaryOperator op) {
    return map(m -> m.withValue(newState(m.value.getJsonObject("aggregate"), op)));
  }

  private static List<JsonObject> removeStackTrace(final List<JsonObject> jsons) {
    return jsons.stream().map(Base::removeStackTrace).collect(toList());
  }

  private static JsonObject removeStackTrace(final JsonObject json) {
    return create(() -> createObjectBuilder(json))
        .updateIf(
            () -> ofNullable(json.getString("exception", null)), (b, v) -> b.add("exception", ""))
        .build()
        .build();
  }

  static Message<String, JsonObject> removeTimestamps(final Message<String, JsonObject> message) {
    return message.withValue(
        create(() -> createObjectBuilder(message.value).remove(TIMESTAMP))
            .updateIf(
                () -> ofNullable(message.value.getJsonObject(BEFORE)),
                (b, o) -> b.add(BEFORE, createObjectBuilder(o).remove(TIMESTAMP)))
            .updateIf(
                () -> ofNullable(message.value.getJsonObject(AFTER)),
                (b, o) -> b.add(AFTER, createObjectBuilder(o).remove(TIMESTAMP)))
            .build()
            .build());
  }

  private static String resource(final String testName, final String kind) {
    return "/tests/" + testName + "/" + kind;
  }

  private static List<JsonObject> sort(final List<JsonObject> results) {
    return results.stream().sorted(Base::compare).collect(toList());
  }

  private static String topic(final String name, final String environment) {
    return fullType() + "-" + name + (environment != null ? ("-" + environment) : "");
  }

  private static Consumer<Publisher<Message<String, JsonObject>>> values(
      final List<JsonObject> expected,
      final List<JsonObject> results,
      final Streams<
              String,
              JsonObject,
              ConsumerRecord<String, JsonObject>,
              ProducerRecord<String, JsonObject>>
          streams,
      final AtomicInteger running) {
    return pub ->
        pub.subscribe(
            lambdaSubscriber(
                message -> {
                  results.add(removeTimestamps(message).value);

                  if (results.size() == expected.size() && running.decrementAndGet() == 0) {
                    streams.stop();
                  }
                }));
  }

  @AfterEach
  public void afterEach() {
    cleanUpCollections();
    deleteTopics(null);
    deleteTopics("dev");
  }

  @BeforeEach
  public void beforeEach() {
    cleanUpCollections();
    deleteTopics(null);
    deleteTopics("dev");
    createTopics(null);
    createTopics("dev");
  }

  protected void runTest(final String name) {
    runTest(name, null, false);
  }

  protected void runTest(final String name, final String environment, final boolean withProcessor) {
    final Streams<
            String,
            JsonObject,
            ConsumerRecord<String, JsonObject>,
            ProducerRecord<String, JsonObject>>
        streams = createStreams();
    final List<JsonObject> expectedAggregates = loadMessages(resource(name, AGGREGATE));
    final List<JsonObject> expectedEvents = loadMessages(resource(name, EVENT));
    final List<JsonObject> expectedEventsFull = loadMessages(resource(name, EVENT_FULL));
    final List<JsonObject> expectedReplies = loadMessages(resource(name, REPLY));
    final List<JsonObject> resultAggregates = new ArrayList<>();
    final List<JsonObject> resultEvents = new ArrayList<>();
    final List<JsonObject> resultEventsFull = new ArrayList<>();
    final List<JsonObject> resultReplies = new ArrayList<>();
    final AtomicInteger running = new AtomicInteger(4);

    aggregate(streams, environment, withProcessor)
        .to(
            topic(COMMAND, environment),
            Source.of(inputMessages(loadMessages(resource(name, COMMAND)))))
        .consume(
            topic(AGGREGATE, environment),
            values(expectedAggregates, resultAggregates, streams, running))
        .consume(topic(EVENT, environment), values(expectedEvents, resultEvents, streams, running))
        .consume(
            topic(EVENT_FULL, environment),
            values(expectedEventsFull, resultEventsFull, streams, running))
        .consume(
            topic(REPLY, environment), values(expectedReplies, resultReplies, streams, running))
        .start();

    assertEquals(expectedEventsFull, resultEventsFull);
    assertEquals(expectedAggregates, resultAggregates);
    assertEquals(expectedEvents, resultEvents);
    assertEquals(sort(removeStackTrace(expectedReplies)), sort(removeStackTrace(resultReplies)));
  }
}
