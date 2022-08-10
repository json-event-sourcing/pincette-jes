module net.pincette.jes {
  requires java.json;
  requires org.reactivestreams;
  requires org.mongodb.driver.core;
  requires net.pincette.json;
  requires net.pincette.mongo;
  requires net.pincette.common;
  requires net.pincette.rs;
  requires net.pincette.rs.streams;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  requires java.logging;

  exports net.pincette.jes;
}
