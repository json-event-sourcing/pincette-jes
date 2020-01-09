package net.pincette.jes;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.json.JsonObject;

/**
 * An implementation should create a command from a full event, which has the "_after" and "_before"
 * fields. When the function returns <code>null</code> or a JSON object that is not a command, the
 * result will be ignored.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
@FunctionalInterface
public interface EventToCommand extends Function<JsonObject, CompletionStage<JsonObject>> {}
