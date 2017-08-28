package com.cvent.couchbase.session;

import com.cvent.couchbase.session.CouchbaseSessionDataStore.CouchbaseSessionData;
import org.eclipse.jetty.server.session.SessionData;
import org.glassfish.hk2.api.Injectee;
import org.glassfish.hk2.api.InjectionResolver;
import org.glassfish.hk2.api.ServiceHandle;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

/**
 * Provides the HttpSession entity for any resource annotated with @CouchbaseSession annotated method parameter
 * 
 * @author bryan
 */

public class CouchbaseHttpSessionProvider implements InjectionResolver<CouchbaseSession> {

    private final HttpServletRequest req;

    @Inject
    public CouchbaseHttpSessionProvider(HttpServletRequest request) {
        this.req = request;
    }

//    @Override
//    public Injectable<?> getInjectable(ComponentContext ic, final CouchbaseSession session, Parameter parameter) {
//        if (parameter.getParameterClass().isAssignableFrom(CouchbaseSessionData.class)) {
//            return () -> {
//                final HttpServletRequest req = this.req.get();
//                if (req != null) {
//                    WrapperSessionCache.CouchbaseSession couchSession =
//                            (WrapperSessionCache.CouchbaseSession) req.getSession(session.create());
//                    CouchbaseSessionData couchbaseSessionData = (CouchbaseSessionData) couchSession.getSessionData();
//                    if (session.write()) {
//                        couchbaseSessionData.setWrite(true);
//                    }
//
//                    return couchbaseSessionData;
//                }
//                return null;
//            };
//        }
//        return null;
//    }

    @Override
    public Object resolve(Injectee injectee, ServiceHandle<?> root) {
        if (injectee.getRequiredType().getClass().isAssignableFrom(SessionData.class) && req != null) {
                CouchbaseSession session = injectee.getParent().getAnnotation(CouchbaseSession.class);
                WrapperSessionCache.CouchbaseSession couchSession =
                        (WrapperSessionCache.CouchbaseSession) req.getSession(session.create());
                CouchbaseSessionData couchbaseSessionData = (CouchbaseSessionData) couchSession.getSessionData();
                if (session.write()) {
                    couchbaseSessionData.setWrite(true);
                }
                return couchbaseSessionData;
        } else {
            return null;
        }
    }

    @Override
    public boolean isConstructorParameterIndicator() {
        return false;
    }

    @Override
    public boolean isMethodParameterIndicator() {
        return true;
    }


}
