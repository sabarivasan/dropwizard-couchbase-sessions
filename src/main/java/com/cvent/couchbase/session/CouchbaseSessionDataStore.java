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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created session " + session);
            }
        } else if (data.isDirty()) {
            assertWritableSession(session, "updateSession");
            try {
                session.setLastSaved(System.currentTimeMillis());
                RawJsonDocument doc = RawJsonDocument.create(getKey(id), maxInactiveInterval,
                                                             session.serialize(mapper), session.getCas());
                bucket.upsert(doc);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Failed serialize session to JSON " + session, ex);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Updated session " + session);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Request to write session {} ignored since it's not dirty", session);
            }
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
    public static void assertWritableSession(CouchbaseSessionData session, String methodName) {
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


}
