package com.cvent.couchbase.session;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.internal.inject.AbstractContainerRequestValueFactory;
import org.glassfish.jersey.server.internal.inject.AbstractValueFactoryProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.internal.inject.ParamInjectionResolver;
import org.glassfish.jersey.server.model.Parameter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

/**
 * Provides the CouchbaseSessionData entity for any resource annotated with @CouchbaseSession annotated method parameter
 *
 * @author bryan
 */
@Singleton
public class CouchbaseHttpSessionProvider extends ParamInjectionResolver<CouchbaseSession> {

    public CouchbaseHttpSessionProvider() {
        super(CouchbaseSessionFactoryProvider.class);
    }

    /**
     * factory provider
     *
     */
    public static class CouchbaseSessionFactoryProvider extends AbstractValueFactoryProvider {

        /**
         * Initialize the provider.
         *
         * @param mpep                   multivalued map parameter extractor provider.
         * @param locator                HK2 service locator.
         */

        @Inject
        public CouchbaseSessionFactoryProvider(MultivaluedParameterExtractorProvider mpep,
                                               ServiceLocator locator) {
            super(mpep, locator, Parameter.Source.UNKNOWN);
        }

        @Override
        public Factory<? extends CouchbaseSessionData> createValueFactory(Parameter parameter) {
            Class<?> paramType = parameter.getRawType();
            CouchbaseSession annotation = parameter.getAnnotation(CouchbaseSession.class);
            if (annotation != null && paramType.isAssignableFrom(CouchbaseSessionData.class)) {
                return new CouchbaseSessionFactory(annotation);
            } else {
                return null;
            }
        }

    }

    /**
     * A factory that extracts the session from the http request
     */
    public static class CouchbaseSessionFactory extends AbstractContainerRequestValueFactory<CouchbaseSessionData> {
        @Context
        private HttpServletRequest request;

        private final CouchbaseSession annotation;

        public CouchbaseSessionFactory(CouchbaseSession annotation) {
            this.annotation = annotation;
        }

        @Override
        public CouchbaseSessionData provide() {
            WrapperSessionCache.CouchbaseSession couchSession =
                    (WrapperSessionCache.CouchbaseSession) request.getSession(annotation.create());
            final CouchbaseSessionData couchbaseSessionData = (CouchbaseSessionData) couchSession.getSessionData();
            if (annotation.write()) {
                couchbaseSessionData.setWrite(true);
            }
            return couchbaseSessionData;
        }
    }
}
