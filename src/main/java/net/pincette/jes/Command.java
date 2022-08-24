package net.pincette.jes;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static net.pincette.jes.Commands.PATCH;
import static net.pincette.jes.JsonFields.ACL;
import static net.pincette.jes.JsonFields.ACL_WRITE;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ERROR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.OPS;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.jes.Util.getUsername;
import static net.pincette.jes.Util.isJesObject;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getBoolean;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Or.tryWith;

import java.util.Optional;
import java.util.Set;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import net.pincette.json.JsonUtil;

/**
 * Some command utilities.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Command {
  private Command() {}

  /**
   * Creates a command without data.
   *
   * @param type the aggregate type.
   * @param id the ID of the aggregate instance.
   * @param name the name of the command.
   * @return The JSON representation of the command.
   * @since 1.0
   */
  public static JsonObject createCommand(final String type, final String id, final String name) {
    return createCommandBuilder(type, id, name).build();
  }

  /**
   * Creates a command builder without data. You can then add more fields to it.
   *
   * @param type the aggregate type.
   * @param id the ID of the aggregate instance.
   * @param name the name of the command.
   * @return The JSON builder.
   * @since 1.0
   */
  public static JsonObjectBuilder createCommandBuilder(
      final String type, final String id, final String name) {
    return createObjectBuilder().add(TYPE, type).add(ID, id.toLowerCase()).add(COMMAND, name);
  }

  /**
   * Creates a command which takes over the fields <code>_type</code>, <code>_id</code> and <code>
   * _jwt</code> from <code>json</code>.
   *
   * @param json the aggregate or event.
   * @param name the name of the command.
   * @return The new command.
   * @since 1.0
   */
  public static JsonObject createCommandFor(final JsonObject json, final String name) {
    return createCommandBuilderFor(json, name).build();
  }

  /**
   * Creates a command builder which takes over the fields <code>_type</code>, <code>_id</code> and
   * <code>
   * _jwt</code> from <code>json</code>.
   *
   * @param json the aggregate or event.
   * @param name the name of the command.
   * @return The new command.
   * @since 1.0
   */
  public static JsonObjectBuilder createCommandBuilderFor(
      final JsonObject json, final String name) {
    return create(() -> createCommandBuilder(json.getString(TYPE), json.getString(ID), name))
        .updateIf(
            builder -> json.containsKey(JWT), builder -> builder.add(JWT, json.getJsonObject(JWT)))
        .build();
  }

  private static Optional<JsonArray> getAllowedRoles(
      final JsonObject state, final JsonObject command) {
    return tryWith(() -> getArray(state, "/" + ACL + "/" + command.getString(COMMAND)).orElse(null))
        .or(() -> getArray(state, "/" + ACL + "/" + ACL_WRITE).orElse(null))
        .get();
  }

  /**
   * Checks if <code>json</code> is marked with an error, which means the field <code>_error</code>
   * is set to <code>true</code>.
   *
   * @param json the given object, usually a command.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean hasError(final JsonObject json) {
    return json != null && (json.getBoolean(ERROR, false) || !json.getBoolean("valid", true));
  }

  /**
   * Checks whether or not the given command is allowed for the given aggregate, based on the _acl
   * field in the aggregate.
   *
   * @param currentState the given aggregate.
   * @param command the given command.
   * @param breakingTheGlass when set this overrules the _acl field. <b>This should only be used
   *     with auditing turned on.</b>
   * @return Whether the command can go through or not.
   * @since 1.0.1
   */
  public static boolean isAllowed(
      final JsonObject currentState, final JsonObject command, final boolean breakingTheGlass) {
    return getUsername(command)
        .map(
            user ->
                user.equals("system")
                    || !currentState.containsKey(ACL)
                    || (breakingTheGlass
                        && getBoolean(command, "/_jwt/breakingTheGlass").orElse(false))
                    || getArray(command, "/_jwt/roles")
                        .map(JsonUtil::strings)
                        .map(
                            roles ->
                                isAllowed(
                                    currentState,
                                    command,
                                    concat(roles, of(user)).collect(toSet())))
                        .orElse(false))
        .orElse(false);
  }

  private static boolean isAllowed(
      final JsonObject state, final JsonObject command, final Set<String> roles) {
    final Optional<JsonArray> allowed = getAllowedRoles(state, command);

    return allowed.isEmpty()
        || allowed
            .map(JsonUtil::strings)
            .map(principals -> principals.collect(toSet()))
            .map(principals -> !intersection(principals, roles).isEmpty())
            .orElse(false);
  }

  /**
   * Checks if <code>json</code> is a command. This means it has the fields <code>_id</code>, <code>
   * _type</code> and <code>_command</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean isCommand(final JsonObject json) {
    return isJesObject(json)
        && getString(json, "/" + CORR).isPresent()
        && getString(json, "/" + COMMAND).isPresent()
        && (PATCH.equals(json.getString(COMMAND)) || !json.containsKey(OPS));
  }

  /**
   * Checks if the <code>command</code> has the <code>name</code>.
   *
   * @param command the given command.
   * @param name the name to be checked.
   * @return The check report.
   * @since 1.0
   */
  public static boolean isCommand(final JsonObject command, final String name) {
    return isCommand(command) && name != null && name.equals(command.getString(COMMAND));
  }
}
