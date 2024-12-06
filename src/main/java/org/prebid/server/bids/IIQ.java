package org.prebid.server.bids;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Policy;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Eid;
import com.iab.openrtb.request.User;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import io.netty.channel.ConnectTimeoutException;
import io.vertx.core.Future;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.checkerframework.checker.index.qual.NonNegative;
import org.prebid.server.auction.model.IpAddress;
import org.prebid.server.auction.requestfactory.Ortb2ImplicitParametersResolver;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.floors.proto.FetchStatus;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.Logger;
import org.prebid.server.log.LoggerFactory;
import org.prebid.server.model.HttpRequestContext;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.vertx.httpclient.HttpClient;
import org.prebid.server.vertx.httpclient.model.HttpClientResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IIQ {

    private static final Logger logger = LoggerFactory.getLogger(IIQ.class);

    private static final String URL_TEMPLATE =
            "https://api.intentiq.com/profiles_engine/ProfilesEngineServlet?at=39&mi=10&dpi=1641699010&pt=17"
                    + "&dpn=1&srvrReq=true&ip=[SOURCE_CLIENT_IPv4]&ipv6=[SOURCE_CLIENT_IPv6]"
                    + "&uas=[URL_ENCODED_SOURCE_CLIENT_USER_AGENT]&pcid=[3RD_PARTY_ID]&ref=bidsPBS"
                    + "&idtype=0&iiqidtype=2&iiqpcid=[1ST_PARTY_ID]&iiqpciddate=[1ST_PARTY_ID_EPOCH_CREATION_DATE]";
    private static final long MAX_TTL = 172800000L;

    private final HttpClient httpClient;
    private final JacksonMapper mapper;
    private final Ortb2ImplicitParametersResolver implicitParametersResolver;

    private final Set<String> fetchInProgress;
    private final Cache<String, ResponseCacheInfo> fetchedData;
    private final Policy.VarExpiration cacheExpiration;

    public IIQ(HttpClient httpClient,
               JacksonMapper mapper,
               Ortb2ImplicitParametersResolver implicitParametersResolver) {

        this.httpClient = Objects.requireNonNull(httpClient);
        this.mapper = Objects.requireNonNull(mapper);
        this.implicitParametersResolver = Objects.requireNonNull(implicitParametersResolver);

        fetchInProgress = new ConcurrentHashSet<>();
        fetchedData = Caffeine.newBuilder()
                .expireAfter(new Expiry<String, ResponseCacheInfo>() {
                    @Override
                    public long expireAfterCreate(String s, ResponseCacheInfo responseCacheInfo, long l) {
                        return TimeUnit.MILLISECONDS.toNanos(
                                responseCacheInfo.cacheTtl == null ? MAX_TTL : responseCacheInfo.cacheTtl);
                    }

                    @Override
                    public long expireAfterUpdate(String s, ResponseCacheInfo responseCacheInfo,
                                                  long l, @NonNegative long l1) {
                        return l1;
                    }

                    @Override
                    public long expireAfterRead(String s, ResponseCacheInfo responseCacheInfo,
                                                long l, @NonNegative long l1) {
                        return l1;
                    }
                })
                .<String, ResponseCacheInfo>build();
        cacheExpiration = fetchedData.policy().expireVariably().get();
    }

    private String getEndpointURL(Device device, String thirdPartyID, String firstPartyID,
                                  String firstPartyCreationEPOC) {
        return URL_TEMPLATE.replace("[SOURCE_CLIENT_IPv4]", device.getIp())
                .replace("[SOURCE_CLIENT_IPv6]", device.getIpv6())
                .replace("[URL_ENCODED_SOURCE_CLIENT_USER_AGENT]", HttpUtil.encodeUrl(device.getUa()))
                .replace("[3RD_PARTY_ID]", thirdPartyID)
                .replace("[1ST_PARTY_ID]", firstPartyID)
                .replace("[1ST_PARTY_ID_EPOCH_CREATION_DATE]", firstPartyCreationEPOC != null
                        ? firstPartyCreationEPOC : String.valueOf(Instant.now().toEpochMilli()));
    }

    private JsonNode fetch(Device device, String thirdPartyID, String firstPartyID,
                          String firstPartyCreationEPOC) {
        //final String cacheKey = device.getIp() + device.getIpv6() + device.getUa() + thirdPartyID + firstPartyID;
        final String cacheKey = device.getIp() + device.getIpv6() + device.getUa();
        final ResponseCacheInfo cachedData = fetchedData.asMap().get(cacheKey);

        if (cachedData == null) {
            fetchPriceFloorData(device, thirdPartyID, firstPartyID, firstPartyCreationEPOC, cacheKey);
        } else {
            return cachedData.data;
        }

        return null;
    }

    private void fetchPriceFloorData(Device device, String thirdPartyID,
                                     String firstPartyID, String firstPartyCreationEPOC, String cacheKey) {
        final String fetchUrl = getEndpointURL(device, thirdPartyID, firstPartyID, firstPartyCreationEPOC);
        if (!fetchInProgress.contains(cacheKey)) {
            fetchPriceFloorDataAsynchronous(cacheKey, fetchUrl);
        }
    }

    private void fetchPriceFloorDataAsynchronous(String cacheKey, String url) {
        final long timeout = 5000L;

        fetchInProgress.add(cacheKey);
        logger.info(url);
        httpClient.get(url, timeout)
                .map(response -> parseResponse(response, cacheKey))
                .recover(throwable -> recoverFromFailedFetching(throwable, cacheKey))
                .map(this::updateCache);
    }

    private ResponseCacheInfo parseResponse(HttpClientResponse httpClientResponse, String cacheKey) {

        final int statusCode = httpClientResponse.getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            throw new PreBidException("Failed to request for IIQ with status %s"
                    .formatted(statusCode));
        }
        final String body = httpClientResponse.getBody();
        logger.info(body);

        if (StringUtils.isBlank(body)) {
            throw new PreBidException(
                    "Failed to parse IIQ response, response body can not be empty");
        }

        final JsonNode data = parseData(body);

        long ttl = MAX_TTL;
        if (data.get("cttl") != null) {
            try {
                ttl = Long.parseLong(data.get("cttl").asText());
            } catch (NumberFormatException e) {
                // fail silently
            }
        }

        return ResponseCacheInfo.of(data,
                FetchStatus.success,
                ttl,
                cacheKey);
    }

    private JsonNode parseData(String body) {
        final JsonNode data;
        try {
            data = mapper.mapper().readTree(body);
        } catch (JsonProcessingException e) {
            throw new PreBidException("Failed to parse IIQ response: %s"
                    .formatted(ExceptionUtils.getMessage(e)));
        }
        return data;
    }

    private Future<ResponseCacheInfo> recoverFromFailedFetching(Throwable throwable, String cacheKey) {
        final FetchStatus fetchStatus;
        if (throwable instanceof TimeoutException || throwable instanceof ConnectTimeoutException) {
            fetchStatus = FetchStatus.timeout;
            logger.error("IIQ timeout.");
        } else {
            fetchStatus = FetchStatus.error;
            logger.error(
                    "Failed to fetch from IIQ endpoint, with a reason : %s "
                            .formatted(throwable.getMessage()));
        }

        return Future.succeededFuture(ResponseCacheInfo.withStatus(fetchStatus));
    }

    private JsonNode updateCache(ResponseCacheInfo cacheInfo) {
        final String cacheKey = cacheInfo.cacheKey;
        if (cacheInfo.getFetchStatus() == FetchStatus.success || !fetchedData.asMap().containsKey(cacheKey)) {
            //fetchedData.put(cacheKey, cacheInfo.data);
            cacheExpiration.put(cacheKey, cacheInfo, cacheInfo.cacheTtl, TimeUnit.MILLISECONDS);
        }

        fetchInProgress.remove(cacheKey);

        return cacheInfo.data;
    }

    public BidRequest enrichWithIIQ(BidRequest bidRequest, HttpRequestContext request) {
        //TODO: geo restriction
        //TODO: 1st and 3rd party IDs from device object
        final Device device = bidRequest.getDevice();
        Device deviceWithIP = device;
        final IpAddress ip = implicitParametersResolver.findIpFromRequest(request);
        final IPAddressString addressString
                = new IPAddressString(ip == null ? request.getRemoteHost() : ip.getIp());
        final IPAddress ipAddress;
        try {
            ipAddress = addressString.toAddress();
        } catch (AddressStringException e) {
            throw new PreBidException(
                    "Failed to parse IP address for IIQ");
        }
        if (deviceWithIP.getIp() == null) {
            deviceWithIP = deviceWithIP.toBuilder().ip(ipAddress.isIPv4() ? ipAddress.toString() : "").build();
        }
        if (deviceWithIP.getIpv6() == null) {
            deviceWithIP = deviceWithIP.toBuilder().ipv6(ipAddress.isIPv6() ? ipAddress.toString() : "").build();
        }
        final JsonNode data = fetch(deviceWithIP, "", "", "");
        if (data != null) {
            if (data.get("data") != null && data.get("data").get("eids") != null) {
                final ObjectReader reader = mapper.mapper().readerFor(new TypeReference<List<Eid>>() {
                });
                try {
                    final List<Eid> exisitingEuids = bidRequest.getUser().getEids();
                    final List<Eid> iiqEids = reader.readValue(data.get("data").get("eids"));
                    final List<Eid> euids = new ArrayList<>();
                    if (exisitingEuids != null) {
                        exisitingEuids.forEach(id -> {
                            euids.add(id);
                            iiqEids.removeIf(e -> e.getSource().equals(id.getSource()));
                        });
                    }
                    if (iiqEids != null) {
                        euids.addAll(iiqEids);
                    }
                    if (!euids.isEmpty()) {
                        final User user = bidRequest.getUser().toBuilder()
                                .eids(euids).build();
                        return bidRequest.toBuilder()
                                .user(user).build();
                    }
                } catch (IOException e) {
                    throw new PreBidException("Failed to parse IIQ eids");
                }
            }

        }
        return bidRequest;
    }

    @Value(staticConstructor = "of")
    private static class ResponseCacheInfo {

        JsonNode data;

        FetchStatus fetchStatus;

        Long cacheTtl;

        String cacheKey;

        public static ResponseCacheInfo withStatus(FetchStatus status) {
            return ResponseCacheInfo.of(null, status, null, null);
        }
    }
}
