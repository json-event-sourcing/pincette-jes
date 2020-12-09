module net.pincette.jes {
  requires java.json;
  requires org.reactivestreams;
  requires org.mongodb.driver.core;
  requires net.pincette.jes.util;
  requires net.pincette.json;
  requires net.pincette.mongo;
  requires net.pincette.common;
  requires kafka.streams;
  requires net.pincette.rs;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  exports net.pincette.jes;
}
