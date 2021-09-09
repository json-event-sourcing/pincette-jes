package net.pincette.jes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestAggregate extends Base {
  @Test
  @DisplayName("duplicate")
  void duplicate() {
    runTest("duplicate");
  }

  @Test
  @DisplayName("plusminus")
  void plusminus() {
    runTest("plusminus");
  }

  @Test
  @DisplayName("put")
  void put() {
    runTest("put");
  }

  @Test
  @DisplayName("put_twice")
  void putTwice() {
    runTest("put_twice");
  }

  @Test
  @DisplayName("put_bad_user")
  void putBadUser() {
    runTest("put_bad_user");
  }

  @Test
  @DisplayName("put_good_user")
  void putGoodUser() {
    runTest("put_good_user");
  }

  @Test
  @DisplayName("unknown")
  void unknown() {
    runTest("unknown");
  }
}
