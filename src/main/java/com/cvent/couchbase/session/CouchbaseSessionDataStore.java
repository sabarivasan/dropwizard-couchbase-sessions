package com.cvent.couchbase.session;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionData;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of session data store for Couchbase + Jetty. This session manager stores documents as JSON into a
 * specific couchbase bucket using the format of keyPrefix+sessionId (ie. dev::app::session::8a9df9asdfasfasdf9asdf)
 *
 * It's expected that the lifecycle management of the couchbase Bucket API be managed outside of the session manager.
 * This is primarily because we'd like to reuse components of the initialization process that's built into dropwizard
 * and would prefer not to manage that here as well.
 *
 * A session will remain active if there is activity.
 *
 * If a read fails for an IOException then this will fallback and try to read the session from a replica.
 */
public class CouchbaseSessionDataStore extends AbstractSessionDataStore {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CouchbaseSessionDataStore.class);

    private final Bucket bucket;
    private final ObjectMapper mapper;
    private final String keyPrefix;
    private final int maxInactiveInterval;

    public CouchbaseSessionDataStore(String keyPrefix, Bucket bucket, ObjectMapper mapper, int maxInactiveInterval) {
        this.bucket = bucket;
        this.mapper = mapper;
        this.keyPrefix = keyPrefix;
        this.maxInactiveInterval = maxInactiveInterval;
    }

    @Override
    public SessionData newSessionData(String id, long created, long accessed, long lastAccessed, long maxInactiveMs) {
        return new CouchbaseSessionData(id, _context.getCanonicalContextPath(), _context.getVhost(), created,
                                        accessed, lastAccessed, maxInactiveMs);
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        if (!isRunning() || data == null || !(data instanceof CouchbaseSessionData)) {
            return;
        }

        CouchbaseSessionData session = (CouchbaseSessionData) data;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Add session {}", session);
        }

        if (lastSaveTime == 0) {
            try {
                RawJsonDocument doc = RawJsonDocument.create(getKey(id), maxInactiveInterval,
                                                             session.serialize(mapper));
                bucket.insert(doc);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Failed serialize session to JSON " + session, ex);
            }
        } else {
            assertWritableSession(session, "updateSession");
            try {
                session.setLastSaved(System.currentTimeMillis());
                RawJsonDocument doc = RawJsonDocument.create(getKey(id), maxInactiveInterval,
                                                             session.serialize(mapper), session.getCas());
                bucket.upsert(doc);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Failed serialize session to JSON " + session, ex);
            }

        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updated session " + session);
        }

    }

    /**
     * Verify that the session is writable according to @CouchbaseSession annotation and if not then throw
     * UnsupportedOperationException to protect the developer from possibly doing something wrong with concurrent
     * writes of the session state object.
     *
     * @param session
     * @param methodName    The name of the method calling this method so that it's easy for debugging
     */
    private static void assertWritableSession(CouchbaseSessionData session, String methodName) {
        if (!session.isWrite()) {
            throw new UnsupportedOperationException(
                    methodName + "() - Write operation not supported. "
                            + "See CouchbaseSession annotation and be mindful of "
                            + "allowing concurrent threads access the same session as it will cause failures");
        }
    }


    private String getKey(String id) {
        return keyPrefix + id;
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        return candidates;
    }

    @Override
    public boolean isPassivating() {
        return true;
    }

    @Override
    public boolean exists(String id) throws Exception {
        return null != load(id);
    }

    @Override
    public SessionData load(String id) throws Exception {
        String key = getKey(id);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get session {}", key);
        }

        try {
            RawJsonDocument doc = bucket.getAndTouch(key, maxInactiveInterval, RawJsonDocument.class);
            if (doc == null) {
                return null;
            }

            try {
                return CouchbaseSessionData.deserialize(doc.content(), doc.cas(), mapper);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to deserialize session " + key, ex);
            }
        } catch (CouchbaseException ex) {
            LOG.warn("Read failed to master, attempting read from replica for {}", key);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Read failed to master, attempting read from replica for " + key, ex);
            }

            //We should only read from a replica if there was a failure reading from the primary master.  This typically
            //should only occur when there's a network issue or during an auto-failover (outage).
            RawJsonDocument replicaDoc
                    = bucket.getFromReplica(key, ReplicaMode.FIRST, RawJsonDocument.class).get(0);

            if (replicaDoc == null) {
                return null;
            }

            try {
                return CouchbaseSessionData.deserialize(replicaDoc.content(), replicaDoc.cas(), mapper);
            } catch (IOException replicaEx) {
                throw new RuntimeException("Failed to deserialize replica session " + key, replicaEx);
            }
        }
    }

    @Override
    public boolean delete(String id) throws Exception {
        String key = getKey(id);
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeSession() key={}", key);
        }

        try {
            //We are not using CAS when removing because 1) it's not available and 2) since we're removing the session
            //we don't care about consistency because the update will fail by any other thread anyways because the
            //session won't exist which will create the behavior we want and 3) this removeSession api isn't really
            //called in our use.
            bucket.remove(key, RawJsonDocument.class);

            return true;
        } catch (DocumentDoesNotExistException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to remove key {} because it did not exist", key);
            }
            return false;
        } catch (Exception ex) {
            LOG.warn("Failed to remove session", ex);
            return false;
        }

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


    /**
     * CouchbaseHttpSession is the real instance of a session that's managed by Jetty
     */
    public static final class CouchbaseSessionData extends SessionData {
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
            setAttribute(CPATH, cpath);
            setAttribute(VHOST, vhost);
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
            assertWritableSession(this, "setAttribute");
            return super.setAttribute(name, value);
        }

        @Override
        public String toString() {
            return "Session id=" + getId() + ",dirty=" + dirty + ",created="
                    + getCreated() + ",accessed=" + getAccessed() + ",lastAccessed=" + getAccessed()
                    + ",maxInterval=" + getMaxInactiveMs() + ",lastSaved=" + getLastSaved();
        }

        private String serialize(ObjectMapper mapper) throws JsonProcessingException {
            SessionJson json = new SessionJson();
            json.setAttributes(getAllAttributes());
            json.setLastSaved(getLastSaved());
            json.setCreationTime(getCreated());
            json.setSessionId(getId());
            json.setMaxInactiveInterval(getMaxInactiveMs());
            return mapper.writeValueAsString(json);
        }

        private static CouchbaseSessionData deserialize(String content, long cas, ObjectMapper mapper)
                throws IOException {
            SessionJson json = mapper.readValue(content, SessionJson.class);

            CouchbaseSessionData session = new CouchbaseSessionData(json.getSessionId(),
                     (String) json.getAttributes().get(CPATH), (String) json.getAttributes().get(VHOST),
                     json.getCreationTime(), System.currentTimeMillis(), json.getLastSaved(),
                     json.getMaxInactiveInterval());
            session.setCas(cas);
            session.setLastSaved(json.getLastSaved());
            json.getAttributes().entrySet().forEach(e -> session.setAttribute(e.getKey(), e.getValue()));
            return session;
        }
    }
}
