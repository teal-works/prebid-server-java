package org.prebid.server.bids;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.protocol.HTTP;
import org.checkerframework.checker.index.qual.NonNegative;
import org.prebid.server.auction.model.IpAddress;
import org.prebid.server.auction.requestfactory.Ortb2ImplicitParametersResolver;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.proto.Uids;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.Logger;
import org.prebid.server.log.LoggerFactory;
import org.prebid.server.model.HttpRequestContext;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class EnhancedCookieSync {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedCookieSync.class);

    private static final long TTL = 86400000L;

    private final JacksonMapper mapper;
    private final Ortb2ImplicitParametersResolver implicitParametersResolver;

    private final Cache<String, Uids> userData;

    public EnhancedCookieSync(JacksonMapper mapper,
                              Ortb2ImplicitParametersResolver implicitParametersResolver) {

        this.mapper = Objects.requireNonNull(mapper);
        this.implicitParametersResolver = Objects.requireNonNull(implicitParametersResolver);

        userData = Caffeine.newBuilder()
                .expireAfter(new Expiry<String, Uids>() {
                    @Override
                    public long expireAfterCreate(String s, Uids uids, long l) {
                        return TimeUnit.MILLISECONDS.toNanos(TTL);
                    }

                    @Override
                    public long expireAfterUpdate(String s, Uids uids,
                                                  long l, @NonNegative long l1) {
                        return TimeUnit.MILLISECONDS.toNanos(TTL);
                    }

                    @Override
                    public long expireAfterRead(String s, Uids uids,
                                                long l, @NonNegative long l1) {
                        return TimeUnit.MILLISECONDS.toNanos(TTL);
                    }
                })
                .build();
    }

    public UidsCookie enhanceUids(UidsCookie uidsCookie, RoutingContext routingContext) {
        final HttpRequestContext request = HttpRequestContext.from(routingContext);
        return enhanceUids(uidsCookie, request);
    }

    public UidsCookie enhanceUids(UidsCookie uidsCookie, HttpRequestContext httpRequest) {
        final String cacheKey = generateCacheKey(httpRequest);
        final Uids fromCache = userData.getIfPresent(cacheKey);
        if (uidsCookie.hasLiveUids()) {
            if (fromCache == null) {
                userData.put(cacheKey, uidsCookie.getCookieUids());
                return uidsCookie;
            } else {
                if (mergeUids(uidsCookie, fromCache)) {
                    userData.put(cacheKey, uidsCookie.getCookieUids());
                }
            }
        } else {
            if (fromCache != null) {
                return new UidsCookie(fromCache, mapper);
            }
        }
        return uidsCookie;
    }

    private boolean mergeUids(UidsCookie uidsCookie, Uids fromCache) {
        final boolean[] hasBeenUpdated = {false};
        fromCache.getUids().forEach((family, uid) -> {
            if (!uidsCookie.hasLiveUidFrom(family)
                    && uid.getExpires().toInstant().toEpochMilli() > System.currentTimeMillis()) {
                uidsCookie.updateUid(family, uid.getUid());
                hasBeenUpdated[0] = true;
            }
        });
        return hasBeenUpdated[0];
    }

    public void updateEnhancedUids(UidsCookie uidsCookie, RoutingContext routingContext) {
        final String cacheKey = generateCacheKey(routingContext);
        userData.put(cacheKey, uidsCookie.getCookieUids());
    }

    private String generateCacheKey(RoutingContext routingContext) {
        final HttpRequestContext request = HttpRequestContext.from(routingContext);
        return generateCacheKey(request);
    }

    private String generateCacheKey(HttpRequestContext httpRequest) {
        final IpAddress ip = implicitParametersResolver.findIpFromRequest(httpRequest);
        final String ipAddress = ip.getIp();
        final String ua = httpRequest.getHeaders().get(HTTP.USER_AGENT);
        return (ipAddress == null ? "" : ipAddress).concat(ua == null ? "" : ua);
    }

    private void updateCache(String cacheKey, Uids uids) {
        userData.put(cacheKey, uids);
    }

}
