package net.pincette.jes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestAggregate extends Base {
  @Test
  @DisplayName("plusminus")
  void plusminus() {
    runTest("plusminus");
    runTest("plusminus", "dev");
  }

  @Test
  @DisplayName("put")
  void put() {
    runTest("put");
    runTest("put", "dev");
  }

  @Test
  @DisplayName("put_twice")
  void putTwice() {
    runTest("put_twice");
    runTest("put_twice", "dev");
  }

  @Test
  @DisplayName("put_bad_user")
  void putBadUser() {
    runTest("put_bad_user");
    runTest("put_bad_user", "dev");
  }

  @Test
  @DisplayName("put_good_user")
  void putGoodUser() {
    runTest("put_good_user");
    runTest("put_good_user", "dev");
  }
}
