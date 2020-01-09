package net.pincette.jes;

import java.util.function.Function;
import javax.json.JsonObject;
import org.reactivestreams.Publisher;

/**
 * An implementation should return the identifiers of the aggregate instances to which commands
 * should be sent. This means at least the field "_id" should be present in the results. The
 * argument is a full event, i.e. it has the "_after" and "_before" fields.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
@FunctionalInterface
public interface GetDestinations extends Function<JsonObject, Publisher<JsonObject>> {}
