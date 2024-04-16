package net.pincette.jes;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import javax.json.JsonObject;

/**
 * The first argument is the command. The second one is the current state of the aggregate.
 *
 * @author Werner Donné
 * @since 1.0
 */
@FunctionalInterface
public interface Reducer extends BiFunction<JsonObject, JsonObject, CompletionStage<JsonObject>> {}
