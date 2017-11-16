package org.rtb.vexing.adapter.rubicon;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Source;
import com.iab.openrtb.request.User;
import com.iab.openrtb.response.BidResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rtb.vexing.adapter.Adapter;
import org.rtb.vexing.adapter.rubicon.model.RubiconBannerExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconBannerExtRp;
import org.rtb.vexing.adapter.rubicon.model.RubiconImpExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconImpExtRp;
import org.rtb.vexing.adapter.rubicon.model.RubiconParams;
import org.rtb.vexing.adapter.rubicon.model.RubiconPubExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconPubExtRp;
import org.rtb.vexing.adapter.rubicon.model.RubiconSiteExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconSiteExtRp;
import org.rtb.vexing.adapter.rubicon.model.RubiconTargetingExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconUserExt;
import org.rtb.vexing.adapter.rubicon.model.RubiconUserExtRp;
import org.rtb.vexing.metric.AccountMetrics;
import org.rtb.vexing.metric.AdapterMetrics;
import org.rtb.vexing.metric.MetricName;
import org.rtb.vexing.metric.Metrics;
import org.rtb.vexing.model.AdUnitBid;
import org.rtb.vexing.model.BidResult;
import org.rtb.vexing.model.Bidder;
import org.rtb.vexing.model.BidderResult;
import org.rtb.vexing.model.PreBidRequestContext;
import org.rtb.vexing.model.request.PreBidRequest;
import org.rtb.vexing.model.response.Bid;
import org.rtb.vexing.model.response.BidderDebug;
import org.rtb.vexing.model.response.BidderStatus;
import org.rtb.vexing.model.response.UsersyncInfo;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RubiconAdapter implements Adapter {

    private static final Logger logger = LoggerFactory.getLogger(RubiconAdapter.class);

    // RubiconParams and UsersyncInfo fields are not in snake-case
    private static final ObjectMapper DEFAULT_NAMING_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String APPLICATION_JSON =
            HttpHeaderValues.APPLICATION_JSON.toString() + ";" + HttpHeaderValues.CHARSET.toString() + "=" + "utf-8";
    private static final String PREBID_SERVER_USER_AGENT = "prebid-server/1.0";
    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);

    private final String endpoint;
    private final URL endpointUrl;
    private final UsersyncInfo usersyncInfo;
    private final String authHeader;

    private final HttpClient httpClient;
    private final Metrics metrics;
    private final AdapterMetrics adapterMetrics;

    private Clock clock = Clock.systemDefaultZone();

    public RubiconAdapter(String endpoint, String usersyncUrl, String xapiUsername, String xapiPassword,
                          HttpClient httpClient, Metrics metrics) {
        this.endpoint = Objects.requireNonNull(endpoint);
        endpointUrl = parseUrl(this.endpoint);
        usersyncInfo = UsersyncInfo.builder()
                .url(Objects.requireNonNull(usersyncUrl))
                .type("redirect")
                .supportCORS(false)
                .build();
        authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (Objects.requireNonNull(xapiUsername) + ':' + Objects.requireNonNull(xapiPassword)).getBytes());

        this.httpClient = Objects.requireNonNull(httpClient);
        this.metrics = Objects.requireNonNull(metrics);
        adapterMetrics = metrics.forAdapter(Type.rubicon);
    }

    private static URL parseUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("URL supplied is not valid", e);
        }
    }

    @Override
    public Future<BidderResult> requestBids(Bidder bidder, PreBidRequestContext preBidRequestContext) {
        final long bidderStarted = clock.millis();

        final AccountMetrics accountMetrics = metrics.forAccount(preBidRequestContext.preBidRequest.accountId);
        final AdapterMetrics accountAdapterMetrics = accountMetrics.forAdapter(Type.rubicon);

        adapterMetrics.incCounter(MetricName.requests);
        accountAdapterMetrics.incCounter(MetricName.requests);

        final List<Future> requestBidFutures = bidder.adUnitBids.stream()
                .map(adUnitBid -> requestSingleBid(adUnitBid, preBidRequestContext, accountAdapterMetrics))
                .collect(Collectors.toList());

        final Future<BidderResult> bidderResultFuture = Future.future();
        // FIXME: error handling
        CompositeFuture.join(requestBidFutures).setHandler(requestBidsResult ->
                bidderResultFuture.complete(toBidderResult(bidder, preBidRequestContext,
                        requestBidsResult.result().list(), bidderStarted, accountMetrics, accountAdapterMetrics)));

        return bidderResultFuture;
    }

    private Future<BidResult> requestSingleBid(AdUnitBid adUnitBid, PreBidRequestContext preBidRequestContext,
                                               AdapterMetrics accountAdapterMetrics) {
        final RubiconParams rubiconParams = DEFAULT_NAMING_MAPPER.convertValue(adUnitBid.params, RubiconParams.class);

        final long timeout = preBidRequestContext.timeout;

        final BidRequest bidRequest = BidRequest.builder()
                .id(preBidRequestContext.preBidRequest.tid)
                .app(preBidRequestContext.preBidRequest.app)
                .at(1)
                .tmax(timeout)
                .imp(Collections.singletonList(makeImp(adUnitBid, rubiconParams, preBidRequestContext)))
                .site(makeSite(rubiconParams, preBidRequestContext))
                .device(makeDevice(preBidRequestContext))
                .user(makeUser(rubiconParams, preBidRequestContext))
                .source(makeSource(preBidRequestContext.preBidRequest))
                .build();

        // FIXME: remove
        logger.debug("Bid request is {0}", Json.encodePrettily(bidRequest));

        final String bidRequestBody = Json.encode(bidRequest);
        final BidderDebug.BidderDebugBuilder bidderDebugBuilder = beginBidderDebug(bidRequestBody);

        final Future<BidResult> future = Future.future();
        httpClient.post(portFromUrl(endpointUrl), endpointUrl.getHost(), endpointUrl.getFile(),
                bidResponseHandler(future, bidderDebugBuilder, adUnitBid, accountAdapterMetrics))
                .putHeader(HttpHeaders.AUTHORIZATION, authHeader)
                .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .putHeader(HttpHeaders.ACCEPT, HttpHeaderValues.APPLICATION_JSON)
                .putHeader(HttpHeaders.USER_AGENT, PREBID_SERVER_USER_AGENT)
                .setTimeout(timeout)
                .exceptionHandler(e -> {
                    logger.error("Error occurred while sending bid request to an exchange", e);

                    if (!future.isComplete()) {
                        if (e instanceof TimeoutException) {
                            adapterMetrics.incCounter(MetricName.timeout_requests);
                            accountAdapterMetrics.incCounter(MetricName.timeout_requests);
                        } else {
                            adapterMetrics.incCounter(MetricName.error_requests);
                            accountAdapterMetrics.incCounter(MetricName.error_requests);
                        }

                        // FIXME: error handling
                        future.complete(null);
                    }
                })
                .end(bidRequestBody);
        return future;
    }

    private static Imp makeImp(AdUnitBid adUnitBid, RubiconParams rubiconParams, PreBidRequestContext
            preBidRequestContext) {
        return Imp.builder()
                .id(adUnitBid.adUnitCode)
                .secure(preBidRequestContext.secure)
                .instl(adUnitBid.instl)
                .banner(makeBanner(adUnitBid))
                .ext(Json.mapper.valueToTree(makeImpExt(rubiconParams)))
                .build();
    }

    private static Banner makeBanner(AdUnitBid adUnitBid) {
        return Banner.builder()
                .w(adUnitBid.sizes.get(0).getW())
                .h(adUnitBid.sizes.get(0).getH())
                .format(adUnitBid.sizes)
                .topframe(adUnitBid.topframe)
                .ext(Json.mapper.valueToTree(makeBannerExt(adUnitBid.sizes)))
                .build();
    }

    private static RubiconBannerExt makeBannerExt(List<Format> sizes) {
        final List<Integer> rubiconSizeIds = sizes.stream()
                .map(RubiconSize::toId)
                .filter(id -> id > 0)
                .collect(Collectors.toList());
        return RubiconBannerExt.builder()
                .rp(RubiconBannerExtRp.builder()
                        // FIXME: error handling
                        .sizeId(rubiconSizeIds.get(0))
                        .altSizeIds(rubiconSizeIds.size() > 1
                                ? rubiconSizeIds.subList(1, rubiconSizeIds.size()) : null)
                        .mime("text/html")
                        .build())
                .build();
    }

    private static RubiconImpExt makeImpExt(RubiconParams rubiconParams) {
        return RubiconImpExt.builder()
                .rp(RubiconImpExtRp.builder()
                        .zoneId(rubiconParams.zoneId)
                        .target(!rubiconParams.inventory.isNull() ? rubiconParams.inventory : null)
                        .build())
                .build();
    }

    private Site makeSite(RubiconParams rubiconParams, PreBidRequestContext preBidRequestContext) {
        return Site.builder()
                .domain(preBidRequestContext.domain)
                .page(preBidRequestContext.referer)
                .publisher(makePublisher(rubiconParams))
                .ext(Json.mapper.valueToTree(makeSiteExt(rubiconParams)))
                .build();
    }

    private static Publisher makePublisher(RubiconParams rubiconParams) {
        return Publisher.builder()
                .ext(Json.mapper.valueToTree(makePublisherExt(rubiconParams)))
                .build();
    }

    private static RubiconPubExt makePublisherExt(RubiconParams rubiconParams) {
        return RubiconPubExt.builder()
                .rp(RubiconPubExtRp.builder().accountId(rubiconParams.accountId).build())
                .build();
    }

    private static RubiconSiteExt makeSiteExt(RubiconParams rubiconParams) {
        return RubiconSiteExt.builder()
                .rp(RubiconSiteExtRp.builder().siteId(rubiconParams.siteId).build())
                .build();
    }

    private static Device makeDevice(PreBidRequestContext preBidRequestContext) {
        return Device.builder()
                .ua(preBidRequestContext.ua)
                .ip(preBidRequestContext.ip)
                .build();
    }

    private User makeUser(RubiconParams rubiconParams, PreBidRequestContext preBidRequestContext) {
        // create a copy since user might be shared with other adapters
        final User.UserBuilder userBuilder =
                preBidRequestContext.preBidRequest.app != null ? preBidRequestContext.preBidRequest.user.toBuilder()
                        : User.builder()
                        .buyeruid(preBidRequestContext.uidsCookie.uidFrom(familyName()))
                        // id is a UID for "adnxs" (see logic in open-source implementation)
                        .id(preBidRequestContext.uidsCookie.uidFrom("adnxs"));

        return userBuilder
                .ext(Json.mapper.valueToTree(makeUserExt(rubiconParams)))
                .build();
    }

    private static RubiconUserExt makeUserExt(RubiconParams rubiconParams) {
        return !rubiconParams.visitor.isNull() ? RubiconUserExt.builder()
                .rp(RubiconUserExtRp.builder().target(rubiconParams.visitor).build())
                .build()
                : null;
    }

    private static Source makeSource(PreBidRequest preBidRequest) {
        return Source.builder()
                .fd(1)
                .tid(preBidRequest.tid)
                .build();
    }

    private static int portFromUrl(URL url) {
        final int port = url.getPort();
        return port != -1 ? port : url.getDefaultPort();
    }

    private Handler<HttpClientResponse> bidResponseHandler(
            Future<BidResult> future, BidderDebug.BidderDebugBuilder bidderDebugBuilder, AdUnitBid adUnitBid,
            AdapterMetrics accountAdapterMetrics) {
        return response -> {
            if (response.statusCode() == 200) {
                response
                        .bodyHandler(buffer -> {
                            final String body = buffer.toString();

                            // FIXME: remove
                            logger.debug("Bid response body raw: {0}", body);
                            logger.debug("Bid response: {0}",
                                    Json.encodePrettily(Json.decodeValue(body, BidResponse.class)));

                            future.complete(toBidResult(bidderDebugBuilder, response, body,
                                    Json.decodeValue(body, BidResponse.class), adUnitBid));
                        })
                        .exceptionHandler(exception -> {
                            logger.error("Error occurred during bid response handling", exception);

                            adapterMetrics.incCounter(MetricName.error_requests);
                            accountAdapterMetrics.incCounter(MetricName.error_requests);

                            // FIXME: error handling
                            future.complete(null);
                        });
            } else {
                response.bodyHandler(buffer ->
                        logger.error("Bid response code is {0}, body: {1}",
                                response.statusCode(), buffer.toString()));

                adapterMetrics.incCounter(MetricName.error_requests);
                accountAdapterMetrics.incCounter(MetricName.error_requests);

                // FIXME: error handling
                future.complete(null);
            }
        };
    }

    private static BidResult toBidResult(
            BidderDebug.BidderDebugBuilder bidderDebugBuilder, HttpClientResponse response, String body,
            BidResponse bidResponse, AdUnitBid adUnitBid) {

        return BidResult.builder()
                .bidBuilder(toBidBuilder(bidResponse, adUnitBid))
                .bidderDebug(completeBidderDebug(bidderDebugBuilder, response, body))
                .build();
    }

    private BidderDebug.BidderDebugBuilder beginBidderDebug(String bidRequestBody) {
        return BidderDebug.builder()
                .requestUri(endpoint)
                .requestBody(bidRequestBody);
    }

    private static BidderDebug completeBidderDebug(BidderDebug.BidderDebugBuilder bidderDebugBuilder,
                                                   HttpClientResponse response, String body) {
        return bidderDebugBuilder
                .responseBody(body)
                .statusCode(response.statusCode())
                .build();
    }

    private static Bid.BidBuilder toBidBuilder(BidResponse bidResponse, AdUnitBid adUnitBid) {
        final Bid.BidBuilder builder;

        if (bidResponse.getSeatbid() != null && !bidResponse.getSeatbid().isEmpty()
                && bidResponse.getSeatbid().get(0).getBid() != null
                && !bidResponse.getSeatbid().get(0).getBid().isEmpty()) {

            final com.iab.openrtb.response.Bid bid = bidResponse.getSeatbid().get(0).getBid().get(0);

            builder = Bid.builder()
                    .code(bid.getImpid())
                    .price(bid.getPrice()) // FIXME: now 0 is serialized as "0.0", but should be just "0"
                    .adm(bid.getAdm())
                    .creativeId(bid.getCrid())
                    .width(bid.getW())
                    .height(bid.getH())
                    .dealId(bid.getDealid())
                    .adServerTargeting(toAdServerTargetingOrNull(bid))
                    .bidder(adUnitBid.bidderCode)
                    .bidId(adUnitBid.bidId);
        } else {
            builder = null;
        }

        return builder;
    }

    private static Map<String, String> toAdServerTargetingOrNull(com.iab.openrtb.response.Bid bid) {
        final RubiconTargetingExt rubiconTargetingExt = Json.mapper.convertValue(bid.getExt(),
                RubiconTargetingExt.class);
        return rubiconTargetingExt != null && rubiconTargetingExt.rp != null && rubiconTargetingExt.rp.targeting != null
                ? rubiconTargetingExt.rp.targeting.stream().collect(Collectors.toMap(t -> t.key, t -> t.values.get(0)))
                : null;
    }

    private BidderResult toBidderResult(Bidder bidder, PreBidRequestContext preBidRequestContext,
                                        List<BidResult> bidResults, long bidderStarted,
                                        AccountMetrics accountMetrics, AdapterMetrics accountAdapterMetrics) {
        final Integer responseTime = Math.toIntExact(clock.millis() - bidderStarted);

        adapterMetrics.updateTimer(MetricName.request_time, responseTime);
        accountAdapterMetrics.updateTimer(MetricName.request_time, responseTime);

        final List<Bid.BidBuilder> bidBuilders = bidResults.stream()
                .map(br -> br.bidBuilder)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        final BidderStatus.BidderStatusBuilder bidderStatusBuilder = BidderStatus.builder()
                .bidder(bidder.bidderCode)
                .responseTime(responseTime)
                .numBids(bidBuilders.size());

        if (!bidBuilders.isEmpty()) {
            updateBidReceivedMetrics(accountMetrics, accountAdapterMetrics, bidBuilders.size());
        } else {
            bidderStatusBuilder.noBid(true);

            adapterMetrics.incCounter(MetricName.no_bid_requests);
            accountAdapterMetrics.incCounter(MetricName.no_bid_requests);
        }

        if (preBidRequestContext.preBidRequest.app == null && preBidRequestContext.uidsCookie.uidFrom(familyName())
                == null) {
            bidderStatusBuilder
                    .noCookie(true)
                    .usersync(usersyncInfo());

            adapterMetrics.incCounter(MetricName.no_cookie_requests);
            accountAdapterMetrics.incCounter(MetricName.no_cookie_requests);
        }

        if (preBidRequestContext.isDebug) {
            bidderStatusBuilder
                    .debug(bidResults.stream().map(b -> b.bidderDebug).collect(Collectors.toList()));
        }

        final List<Bid> bids = bidBuilders.stream()
                .map(b -> b.responseTime(responseTime))
                .map(Bid.BidBuilder::build)
                .peek(bid -> updateCpmMetrics(accountMetrics, accountAdapterMetrics, bid))
                .collect(Collectors.toList());

        return BidderResult.builder()
                .bidderStatus(bidderStatusBuilder.build())
                .bids(bids)
                .build();
    }

    private void updateBidReceivedMetrics(AccountMetrics accountMetrics, AdapterMetrics accountAdapterMetrics,
                                          long numBids) {
        accountMetrics.incCounter(MetricName.bids_received, numBids);
        accountAdapterMetrics.incCounter(MetricName.bids_received, numBids);
    }

    private void updateCpmMetrics(AccountMetrics accountMetrics, AdapterMetrics accountAdapterMetrics, Bid bid) {
        final long cpm = bid.price.multiply(THOUSAND).longValue();
        adapterMetrics.updateHistogram(MetricName.prices, cpm);
        accountMetrics.updateHistogram(MetricName.prices, cpm);
        accountAdapterMetrics.updateHistogram(MetricName.prices, cpm);
    }

    @Override
    public String familyName() {
        return "rubicon";
    }

    @Override
    public JsonNode usersyncInfo() {
        return DEFAULT_NAMING_MAPPER.valueToTree(usersyncInfo);
    }
}
