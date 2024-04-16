package net.pincette.jes;

/**
 * Built-in command names.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class Commands {
  /** Logical delete of an aggregate instance. */
  public static final String DELETE = "delete";

  /** Gets an aggregate. */
  public static final String GET = "get";

  /** Applies a JSON patch. */
  public static final String PATCH = "patch";

  /** Replaces the state of an aggregate instance. */
  public static final String PUT = "put";

  private Commands() {}
}
