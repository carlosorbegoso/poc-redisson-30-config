package com.sky;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.redisson.api.*;
import org.redisson.config.BaseConfig;
import org.redisson.config.SingleServerConfig;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

@Path("/redis")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RedisResource {

    @Inject
    RedissonClient redissonClient;

    // --- String (Bucket) ---

    @POST
    @Path("/set/{key}")
    public Response set(@PathParam("key") String key, String value) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        bucket.set(value, Duration.ofMinutes(5));
        return Response.ok(Map.of("key", key, "value", value, "ttl", "5 minutes")).build();
    }

    @GET
    @Path("/get/{key}")
    public Response get(@PathParam("key") String key) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        String value = bucket.get();
        if (value == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("key", key, "error", "Key not found")).build();
        }
        return Response.ok(Map.of("key", key, "value", value)).build();
    }

    @DELETE
    @Path("/delete/{key}")
    public Response delete(@PathParam("key") String key) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        boolean deleted = bucket.delete();
        return Response.ok(Map.of("key", key, "deleted", deleted)).build();
    }

    // --- Atomic Counter ---

    @POST
    @Path("/counter/{key}/increment")
    public Response increment(@PathParam("key") String key) {
        RAtomicLong counter = redissonClient.getAtomicLong(key);
        long newValue = counter.incrementAndGet();
        return Response.ok(Map.of("key", key, "value", newValue)).build();
    }

    @GET
    @Path("/counter/{key}")
    public Response getCounter(@PathParam("key") String key) {
        RAtomicLong counter = redissonClient.getAtomicLong(key);
        return Response.ok(Map.of("key", key, "value", counter.get())).build();
    }

    // --- List ---

    @POST
    @Path("/list/{key}")
    public Response addToList(@PathParam("key") String key, String value) {
        RList<String> list = redissonClient.getList(key);
        list.add(value);
        return Response.ok(Map.of("key", key, "size", list.size(), "added", value)).build();
    }

    @GET
    @Path("/list/{key}")
    public Response getList(@PathParam("key") String key) {
        RList<String> list = redissonClient.getList(key);
        return Response.ok(Map.of("key", key, "values", list.readAll(), "size", list.size())).build();
    }

    // --- Map ---

    @POST
    @Path("/map/{key}/{field}")
    public Response putMap(@PathParam("key") String key, @PathParam("field") String field, String value) {
        RMap<String, String> map = redissonClient.getMap(key);
        map.put(field, value);
        return Response.ok(Map.of("key", key, "field", field, "value", value)).build();
    }

    @GET
    @Path("/map/{key}")
    public Response getMap(@PathParam("key") String key) {
        RMap<String, String> map = redissonClient.getMap(key);
        return Response.ok(Map.of("key", key, "entries", map.readAllMap())).build();
    }

    // --- Config Inspector ---

    @GET
    @Path("/config")
    public Response config() {
        org.redisson.config.Config cfg = redissonClient.getConfig();
        org.redisson.config.SingleServerConfig s = cfg.useSingleServer();

        // Valores configurados en application.yml (para comparar)
        Map<String, Object> expected = new HashMap<>();
        expected.put("address", "redis://localhost:6379");
        expected.put("connectionPoolSize", 64);
        expected.put("connectionMinimumIdleSize", 10);
        expected.put("idleConnectionTimeout", 10000);
        expected.put("connectTimeout", 10000);
        expected.put("timeout", 3000);
        expected.put("threads", 16);
        expected.put("nettyThreads", 32);

        // Valores actuales leídos desde el cliente Redisson en runtime
        Map<String, Object> actual = new HashMap<>();
        actual.put("address", s.getAddress());
        actual.put("connectionPoolSize", s.getConnectionPoolSize());
        actual.put("subscriptionConnectionPoolSize", s.getSubscriptionConnectionPoolSize());
        actual.put("connectionMinimumIdleSize", s.getConnectionMinimumIdleSize());
        actual.put("subscriptionConnectionMinimumIdleSize", s.getSubscriptionConnectionMinimumIdleSize());
        actual.put("database", s.getDatabase());
        actual.put("dnsMonitoringInterval", s.getDnsMonitoringInterval());
        actual.put("idleConnectionTimeout", s.getIdleConnectionTimeout());
        actual.put("connectTimeout", s.getConnectTimeout());
        actual.put("timeout", s.getTimeout());
        actual.put("subscriptionTimeout", s.getSubscriptionTimeout());
        actual.put("retryAttempts", s.getRetryAttempts());
        actual.put("retryInterval", s.getRetryInterval());
        actual.put("subscriptionsPerConnection", s.getSubscriptionsPerConnection());
        actual.put("keepAlive", s.isKeepAlive());
        actual.put("tcpNoDelay", s.isTcpNoDelay());
        actual.put("pingConnectionInterval", s.getPingConnectionInterval());
        actual.put("clientName", s.getClientName());
        actual.put("password", s.getPassword() == null ? null : "***");
        actual.put("threads", cfg.getThreads());
        actual.put("nettyThreads", cfg.getNettyThreads());

        // Validacion solo de los campos definidos en application.yml
        Map<String, Object> validation = new HashMap<>();
        validation.put("address",                  validate(s.getAddress(), "redis://localhost:6379"));
        validation.put("connectionPoolSize",        validate(s.getConnectionPoolSize(), 64));
        validation.put("connectionMinimumIdleSize", validate(s.getConnectionMinimumIdleSize(), 10));
        validation.put("idleConnectionTimeout",     validate(s.getIdleConnectionTimeout(), 10000));
        validation.put("connectTimeout",            validate(s.getConnectTimeout(), 10000));
        validation.put("timeout",                   validate(s.getTimeout(), 3000));
        validation.put("threads",                   validate(cfg.getThreads(), 16));
        validation.put("nettyThreads",              validate(cfg.getNettyThreads(), 32));

        Map<String, Object> result = new HashMap<>();
        result.put("actual", actual);
        result.put("validation", validation);
        return Response.ok(result).build();
    }

    private String validate(Object actual, Object expected) {
        return expected.equals(actual)
                ? "OK (" + actual + ")"
                : "FAIL — esperado: " + expected + ", actual: " + actual;
    }

    // --- Available Config Discovery ---

    @GET
    @Path("/available-config")
    public Response availableConfig() {
        // Extraer setters de SingleServerConfig + BaseConfig via reflexión
        List<Map<String, String>> singleServerProps = new ArrayList<>();
        List<Map<String, String>> commonProps = new ArrayList<>();

        Set<String> seenSingle = new HashSet<>();
        Set<String> seenBase = new HashSet<>();

        // SingleServerConfig (campos propios)
        for (Method m : SingleServerConfig.class.getDeclaredMethods()) {
            if (!m.getName().startsWith("set") || m.getParameterCount() != 1) continue;
            String camel = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
            String kebab = camelToKebab(camel);
            if (seenSingle.add(kebab)) {
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put("property", "quarkus.redisson.single-server-config." + kebab);
                entry.put("type", m.getParameterTypes()[0].getSimpleName());
                entry.put("currentValue", getCurrentValue(redissonClient.getConfig().useSingleServer(), m));
                singleServerProps.add(entry);
            }
        }

        // BaseConfig (campos heredados)
        for (Method m : BaseConfig.class.getDeclaredMethods()) {
            if (!m.getName().startsWith("set") || m.getParameterCount() != 1) continue;
            String camel = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
            String kebab = camelToKebab(camel);
            if (seenBase.add(kebab)) {
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put("property", "quarkus.redisson.single-server-config." + kebab);
                entry.put("type", m.getParameterTypes()[0].getSimpleName());
                entry.put("currentValue", getCurrentValue(redissonClient.getConfig().useSingleServer(), m));
                commonProps.add(entry);
            }
        }

        singleServerProps.sort(Comparator.comparing(e -> e.get("property")));
        commonProps.sort(Comparator.comparing(e -> e.get("property")));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("note", "Propiedades leídas por reflexión desde SingleServerConfig y BaseConfig de Redisson 3.43.0");
        result.put("rootProps", List.of(
            Map.of("property", "quarkus.redisson.threads", "type", "int", "currentValue", String.valueOf(redissonClient.getConfig().getThreads())),
            Map.of("property", "quarkus.redisson.netty-threads", "type", "int", "currentValue", String.valueOf(redissonClient.getConfig().getNettyThreads())),
            Map.of("property", "quarkus.redisson.file", "type", "String", "currentValue", "null — usa application.yml")
        ));
        result.put("singleServerConfig_ownFields", singleServerProps);
        result.put("singleServerConfig_inheritedFromBaseConfig", commonProps);
        return Response.ok(result).build();
    }

    private String camelToKebab(String camel) {
        return camel.replaceAll("([A-Z])", "-$1").toLowerCase();
    }

    private String getCurrentValue(Object obj, Method setter) {
        try {
            String getterName = "get" + setter.getName().substring(3);
            Method getter = obj.getClass().getMethod(getterName);
            Object val = getter.invoke(obj);
            if (val == null) return "null";
            if (setter.getName().toLowerCase().contains("password")) return "***";
            return val.toString();
        } catch (Exception e) {
            try {
                String boolGetter = "is" + setter.getName().substring(3);
                Method getter = obj.getClass().getMethod(boolGetter);
                Object val = getter.invoke(obj);
                return val != null ? val.toString() : "null";
            } catch (Exception ex) {
                return "n/a";
            }
        }
    }

    // --- Health Check / Ping ---

    @GET
    @Path("/ping")
    public Response ping() {
        Map<String, Object> result = new HashMap<>();
        try {
            RBucket<String> ping = redissonClient.getBucket("__ping__");
            ping.set("pong", Duration.ofSeconds(10));
            String pong = ping.get();
            result.put("status", "UP");
            result.put("response", pong);
            result.put("redissonVersion", redissonClient.getConfig().useSingleServer() != null ? "SingleServer" : "Cluster");
        } catch (Exception e) {
            result.put("status", "DOWN");
            result.put("error", e.getMessage());
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(result).build();
        }
        return Response.ok(result).build();
    }
}
