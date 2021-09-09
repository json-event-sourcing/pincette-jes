package net.pincette.jes;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static net.pincette.util.Util.tryToDoRethrow;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Properties;

class Resources implements AutoCloseable {
  final MongoClient client;
  final MongoDatabase database;

  Resources() {
    final Properties properties = new Properties();

    tryToDoRethrow(
        () -> properties.load(Resources.class.getResourceAsStream("/config.properties")));
    client = create(properties.getProperty("mongodb.uri"));
    database = client.getDatabase(properties.getProperty("mongodb.database"));
  }

  public void close() {
    client.close();
  }
}
