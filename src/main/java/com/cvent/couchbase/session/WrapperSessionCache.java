package com.cvent.couchbase.session;

import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.Session;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.server.session.SessionHandler;

import javax.servlet.http.HttpServletRequest;

/**
 * This wrapper exists only so we can expose SessionData from
 */
public class WrapperSessionCache extends DefaultSessionCache {

    /**
     * @param manager
     */
    public WrapperSessionCache(SessionHandler manager) {
        super(manager);
    }

    @Override
    public Session newSession(HttpServletRequest request, SessionData data) {
        return new CouchbaseSession(getSessionHandler(), request, data);
    }

    @Override
    public Session newSession(SessionData data) {
        return new CouchbaseSession(getSessionHandler(), data);
    }


    /**
     * This wraps Jetty Session to make getSessionData() public
     */
    public static class CouchbaseSession extends Session {

        public CouchbaseSession(SessionHandler handler, HttpServletRequest request, SessionData data) {
            super(handler, request, data);
        }

        public CouchbaseSession(SessionHandler handler, SessionData data) {
            super(handler, data);
        }

        @Override
        public SessionData getSessionData() {
            return super.getSessionData();
        }

    }
}
