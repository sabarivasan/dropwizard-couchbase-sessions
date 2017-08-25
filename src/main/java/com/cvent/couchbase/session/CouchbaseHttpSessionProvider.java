package com.cvent.couchbase.session;

import com.cvent.couchbase.session.CouchbaseSessionDataStore.CouchbaseSessionData;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

/**
 * Provides the HttpSession entity for any resource annotated with @CouchbaseSession annotated method parameter
 * 
 * @author bryan
 */
@Provider
public class CouchbaseHttpSessionProvider implements InjectableProvider<CouchbaseSession, Parameter> {

    private final ThreadLocal<HttpServletRequest> request;

    public CouchbaseHttpSessionProvider(@Context ThreadLocal<HttpServletRequest> request) {
        this.request = request;
    }

    @Override
    public ComponentScope getScope() {
        return ComponentScope.PerRequest;
    }

    @Override
    public Injectable<?> getInjectable(ComponentContext ic, final CouchbaseSession session, Parameter parameter) {
        if (parameter.getParameterClass().isAssignableFrom(CouchbaseSessionData.class)) {
            return () -> {
                final HttpServletRequest req = request.get();
                if (req != null) {

                    WrapperSessionCache.CouchbaseSession couchSession =
                            (WrapperSessionCache.CouchbaseSession) req.getSession(session.create());
                    CouchbaseSessionData couchbaseSessionData = (CouchbaseSessionData) couchSession.getSessionData();
                    if (session.write()) {
                        couchbaseSessionData.setWrite(true);
                    }

                    return couchbaseSessionData;
                }
                return null;
            };
        }
        return null;
    }
}
