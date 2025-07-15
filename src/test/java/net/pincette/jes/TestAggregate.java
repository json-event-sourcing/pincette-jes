package net.pincette.jes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestAggregate extends Base {
  @Test
  @DisplayName("patch")
  void patch() {
    runTest("patch");
    runTest("patch", "dev", true);
  }

  @Test
  @DisplayName("patch_bad")
  void patchBad() {
    runTest("patch_bad");
    runTest("patch_bad", "dev", true);
  }

  @Test
  @DisplayName("plusminus")
  void plusminus() {
    runTest("plusminus");
    runTest("plusminus", "dev", true);
  }

  @Test
  @DisplayName("put")
  void put() {
    runTest("put");
    runTest("put", "dev", true);
  }

  @Test
  @DisplayName("put_twice")
  void putTwice() {
    runTest("put_twice");
    runTest("put_twice", "dev", true);
  }

  @Test
  @DisplayName("put_bad_user")
  void putBadUser() {
    runTest("put_bad_user");
    runTest("put_bad_user", "dev", true);
  }

  @Test
  @DisplayName("put_good_user")
  void putGoodUser() {
    runTest("put_good_user");
    runTest("put_good_user", "dev", true);
  }

  @Test
  @DisplayName("performance")
  void performance() {
    runPerformanceTest(10000);
  }
}
