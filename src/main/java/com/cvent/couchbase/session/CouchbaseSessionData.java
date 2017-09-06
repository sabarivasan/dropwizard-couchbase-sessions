package com.cvent.couchbase.session;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.session.SessionData;

import java.io.IOException;
import java.util.Map;

/**
 * This class offers a container for users to store and retrieve session attributes in Couchbase.
 * Add a parameter of type CouchbaseSessionData and annotate it with @CouchbaseSession.
 *
 * @author sabarivasan
 */
public final class CouchbaseSessionData extends SessionData {
    // The following 2 pieces of information (context path and vhost) are passed by Jetty when a new SessionData object
    // is created. These are stored as attributes and persisted in Couchbase and set on deserialization
    private static final String CPATH = "SESSION_CPATH";
    private static final String VHOST = "SESSION_VHOST";

    /**
     * If dirty, session needs to be (re)persisted
     */
    private boolean dirty = false;


    /**
     * Do NOT serialize this into couchbase as it will become incorrect as soon as it's saved. This should be
     * transient and only used for the life of this in-memory session
     */
    private long cas;

    /**
     * Default to having write mode disabled for sessions to protect developers from doing something they didn't
     * intend since we're forced into using HttpSession interface.
     */
    private boolean write = false;

    public CouchbaseSessionData(String id, String cpath, String vhost, long created, long accessed,
                                long lastAccessed, long maxInactiveMs) {
        super(id, cpath, vhost, created, accessed, lastAccessed, maxInactiveMs);
        // Store context path and vhost as attributes so we have them when we serialize to couch
        super.setAttribute(CPATH, cpath);
        super.setAttribute(VHOST, vhost);
    }

    /**
     * Get the value of write
     *
     * @return the value of write
     */
    public boolean isWrite() {
        return write;
    }

    /**
     * Set the value of write
     *
     * @param write new value of write
     */
    public void setWrite(boolean write) {
        this.write = write;
    }

    /**
     * Get the value of cas
     *
     * @return the value of cas
     */
    public long getCas() {
        return cas;
    }

    /**
     * Set the value of cas
     *
     * @param cas new value of cas
     */
    public void setCas(long cas) {
        this.cas = cas;
    }

    @Override
    public Object setAttribute(String name, Object value) {
        CouchbaseSessionDataStore.assertWritableSession(this, "setAttribute");
        return super.setAttribute(name, value);
    }

    @Override
    public String toString() {
        return "Session id=" + getId() + ",dirty=" + dirty + ",created="
                + getCreated() + ",accessed=" + getAccessed() + ",lastAccessed=" + getAccessed()
                + ",maxInterval=" + getMaxInactiveMs() + ",lastSaved=" + getLastSaved();
    }

    /**
     * Serializes the
     * @param mapper
     * @return
     * @throws JsonProcessingException
     */
    String serialize(ObjectMapper mapper) throws JsonProcessingException {
        SessionJson json = new SessionJson();
        json.setAttributes(getAllAttributes());
        json.setLastSaved(getLastSaved());
        json.setCreationTime(getCreated());
        json.setSessionId(getId());
        json.setMaxInactiveInterval(getMaxInactiveMs());
        return mapper.writeValueAsString(json);
    }

    /**
     * Deserialize a JSON String into a CouchbaseSessionData object
     * @param content       the serialized JSON string
     * @param cas           the couchbase cas
     * @param mapper        the Jackson object mapper
     * @return              the CouchbaseSessionData object
     * @throws IOException  in case something bad happens
     */
    static CouchbaseSessionData deserialize(String content, long cas, ObjectMapper mapper)
            throws IOException {
        SessionJson json = mapper.readValue(content, SessionJson.class);
        CouchbaseSessionData session = new CouchbaseSessionData(json.getSessionId(),
                                                                (String) json.getAttributes().get(CPATH),
                                                                (String) json.getAttributes().get(VHOST),
                                                                json.getCreationTime(),
                                                                System.currentTimeMillis(),
                                                                json.getLastSaved(),
                                                                json.getMaxInactiveInterval());
        session.setCas(cas);
        session.setLastSaved(json.getLastSaved());
        json.getAttributes().entrySet().forEach(e -> session.setAttribute(e.getKey(), e.getValue()));
        return session;
    }

    /**
     * A simple container class that allows us to specify exactly what data type we want to serialize to/from JSON
     * without mucking with the parent class and/or fancy serialization techniques in Jackson
     */
    private static class SessionJson {

        private Map<String, Object> attributes;

        private long creationTime;

        private String sessionId;

        private long lastSaved;

        private long maxInactiveInterval;

        /**
         * Get the value of maxInactiveInterval
         *
         * @return the value of maxInactiveInterval
         */
        public long getMaxInactiveInterval() {
            return maxInactiveInterval;
        }

        /**
         * Set the value of maxInactiveInterval
         *
         * @param maxInactiveInterval new value of maxInactiveInterval
         */
        public void setMaxInactiveInterval(long maxInactiveInterval) {
            this.maxInactiveInterval = maxInactiveInterval;
        }

        /**
         * Get the value of lastSaved
         *
         * @return the value of lastSaved
         */
        public long getLastSaved() {
            return lastSaved;
        }

        /**
         * Set the value of lastSaved
         *
         * @param lastSaved new value of lastSaved
         */
        public void setLastSaved(long lastSaved) {
            this.lastSaved = lastSaved;
        }

        /**
         * Get the value of sessionId
         *
         * @return the value of sessionId
         */
        public String getSessionId() {
            return sessionId;
        }

        /**
         * Set the value of sessionId
         *
         * @param sessionId new value of sessionId
         */
        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        /**
         * Get the value of creationTime
         *
         * @return the value of creationTime
         */
        public long getCreationTime() {
            return creationTime;
        }

        /**
         * Set the value of creationTime
         *
         * @param createdTime new value of creationTime
         */
        public void setCreationTime(long createdTime) {
            this.creationTime = createdTime;
        }

        /**
         * Get the value of attributes
         *
         * @return the value of attributes
         */
        public Map<String, Object> getAttributes() {
            return attributes;
        }

        /**
         * Set the value of attributes
         *
         * @param attributes new value of attributes
         */
        public void setAttributes(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

    }
}
