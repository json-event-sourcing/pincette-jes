package net.pincette.jes;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.util.Command.createCommandBuilder;
import static net.pincette.jes.util.Commands.GET;
import static net.pincette.jes.util.Commands.PUT;
import static net.pincette.jes.util.JsonFields.ACL_WRITE;
import static net.pincette.jes.util.JsonFields.AFTER;
import static net.pincette.jes.util.JsonFields.BEFORE;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.ROLES;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.Util.isManagedObject;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.forEachJoin;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import net.pincette.jes.util.JsonDeserializer;
import net.pincette.jes.util.JsonFields;
import net.pincette.jes.util.JsonSerde;
import net.pincette.jes.util.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class Base {
  static final JsonObject ACL = o(f(GET, a(v("test"))), f(ACL_WRITE, a(v("test"))));
  static final String AGGREGATE = "aggregate";
  static final String APP = "test";
  static final JsonObject BAD_USER = o(f(SUB, v("bad")));
  static final String COMMAND = "command";
  static final String ENV = "dev";
  static final String EVENT = "event";
  static final String EVENT_FULL = "event-full";
  static final JsonObject GOOD_USER = o(f(SUB, v("good")), f(ROLES, a(v("test"))));
  static final String MINUS = "minus";
  static final String PLUS = "plus";
  static final String REPLY = "reply";
  static final JsonObject SYSTEM = o(f(SUB, v("system")));
  static final String TYPE = "test";
  static final String UNIQUE = "unique";
  static final String VALUE = "value";
  private static final Properties CONFIG = config();
  private static final List<String> OUTPUTS = list(AGGREGATE, EVENT, EVENT_FULL, REPLY);
  protected static Resources resources;

  private static StreamsBuilder aggregate() {
    return new Aggregate()
        .withApp(APP)
        .withType(TYPE)
        .withMongoDatabase(resources.database)
        .withEnvironment(ENV)
        .withBuilder(new StreamsBuilder())
        .withReducer(PLUS, (command, currentState) -> reduce(currentState, v -> v + 1))
        .withReducer(MINUS, (command, currentState) -> reduce(currentState, v -> v - 1))
        .withReducer(
            PUT,
            (command, currentState) ->
                completedFuture(
                    createObjectBuilder(command)
                        .remove(JsonFields.COMMAND)
                        .add(JsonFields.ACL, ACL)
                        .build()))
        .withUniqueExpression(createValue("$" + UNIQUE))
        .build();
  }

  @AfterAll
  public static void after() {
    cleanUpCollections();
    resources.close();
  }

  @BeforeAll
  public static void beforeAll() {
    resources = new Resources();
  }

  private static void cleanUpCollections() {
    final String type = fullType();

    forEachJoin(
        with(resources.database.listCollectionNames()).filter(name -> name.startsWith(type)).get(),
        Base::drop);
  }

  static JsonObjectBuilder command(final String name, final String id, final String corr) {
    return createCommandBuilder(fullType(), id, name).add(CORR, corr);
  }

  private static Properties config() {
    final Properties properties = new Properties();

    properties.put(APPLICATION_ID_CONFIG, APP);
    properties.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

    return properties;
  }

  private static TestInputTopic<String, JsonObject> createInputTopic(
      final TopologyTestDriver driver, final String name) {
    return driver.createInputTopic(topic(name), new StringSerializer(), new JsonSerializer());
  }

  private static TestOutputTopic<String, JsonObject> createOutputTopic(
      final TopologyTestDriver driver, final String name) {
    return driver.createOutputTopic(topic(name), new StringDeserializer(), new JsonDeserializer());
  }

  private static void drop(final String collection) {
    forEachJoin(resources.database.getCollection(collection).drop(), v -> {});
  }

  static String fullType() {
    return APP + "-" + TYPE;
  }

  private static List<TestRecord<String, JsonObject>> inputMessages(
      final List<JsonObject> messages) {
    return messages.stream()
        .map(
            m ->
                new TestRecord<>(
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

  private static JsonObject readJson(final File file) {
    return tryToGetWithRethrow(
            () -> new FileInputStream(file), stream -> createReader(stream).readObject())
        .orElse(null);
  }

  private static CompletionStage<JsonObject> reduce(
      final JsonObject currentState, final IntUnaryOperator op) {
    return completedFuture(
        createObjectBuilder(currentState)
            .add(VALUE, op.applyAsInt(currentState.getInt(VALUE, 0)))
            .add(JsonFields.ACL, ACL)
            .build());
  }

  static List<JsonObject> removeTimestamps(final List<JsonObject> messages) {
    return messages.stream()
        .map(message -> must(message, m -> !isManagedObject(m) || m.containsKey(TIMESTAMP)))
        .map(
            message ->
                create(() -> createObjectBuilder(message).remove(TIMESTAMP))
                    .updateIf(
                        () -> ofNullable(message.getJsonObject(BEFORE)),
                        (b, o) -> b.add(BEFORE, createObjectBuilder(o).remove(TIMESTAMP)))
                    .updateIf(
                        () -> ofNullable(message.getJsonObject(AFTER)),
                        (b, o) -> b.add(AFTER, createObjectBuilder(o).remove(TIMESTAMP)))
                    .build()
                    .build())
        .collect(toList());
  }

  private static String resource(final String testName, final String kind) {
    return "/tests/" + testName + "/" + kind;
  }

  private static String topic(final String name) {
    return fullType() + "-" + name + "-" + ENV;
  }

  private static <T> Map<String, T> topics(
      final TopologyTestDriver driver,
      final List<String> names,
      final BiFunction<TopologyTestDriver, String, T> create) {
    return names.stream()
        .map(name -> pair(name, create.apply(driver, name)))
        .collect(toMap(p -> p.first, p -> p.second));
  }

  private static List<JsonObject> values(final TestOutputTopic<String, JsonObject> topic) {
    return removeTimestamps(topic.readValuesToList());
  }

  @BeforeEach
  public void beforeEach() {
    cleanUpCollections();
  }

  protected void runTest(final String name) {
    tryToDoWithRethrow(
        () -> new TopologyTestDriver(aggregate().build(), CONFIG),
        testDriver -> {
          final Map<String, TestInputTopic<String, JsonObject>> inputTopics =
              topics(testDriver, list(COMMAND), Base::createInputTopic);
          final Map<String, TestOutputTopic<String, JsonObject>> outputTopics =
              topics(testDriver, OUTPUTS, Base::createOutputTopic);

          inputTopics
              .get(COMMAND)
              .pipeRecordList(inputMessages(loadMessages(resource(name, COMMAND))));

          OUTPUTS.forEach(
              kind ->
                  assertEquals(loadMessages(resource(name, kind)), values(outputTopics.get(kind))));
        });
  }
}
