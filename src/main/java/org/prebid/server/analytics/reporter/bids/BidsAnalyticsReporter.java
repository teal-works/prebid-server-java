package org.prebid.server.analytics.reporter.bids;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.JsonNode;
import io.vertx.core.Future;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.AmpEvent;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.analytics.model.CookieSyncEvent;
import org.prebid.server.analytics.model.NotificationEvent;
import org.prebid.server.analytics.model.SetuidEvent;
import org.prebid.server.analytics.model.VideoEvent;
import org.prebid.server.analytics.reporter.log.model.LogEvent;
import org.prebid.server.events.EventRequest;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.Logger;
import org.prebid.server.log.LoggerFactory;

import java.math.RoundingMode;
import java.util.Objects;

/**
 * {@link AnalyticsReporter} implementation that writes application events to a log, for illustration purpose only.
 */
public class BidsAnalyticsReporter implements AnalyticsReporter {

    public static final Logger logger = LoggerFactory.getLogger("ANALYTICS");

    private final JacksonMapper mapper;
    private final JsonStringEncoder encoder;

    public BidsAnalyticsReporter(JacksonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
        this.encoder = JsonStringEncoder.getInstance();
    }

    @Override
    public <T> Future<Void> processEvent(T event) {

        final LogEvent<?> logEvent = switch (event) {
            case AmpEvent ampEvent -> LogEvent.of("/openrtb2/amp", ampEvent.getBidResponse());
            case AuctionEvent auctionEvent -> LogEvent.of("/openrtb2/auction", auctionEvent.getBidResponse());
            case CookieSyncEvent cookieSyncEvent -> LogEvent.of("/cookie_sync", cookieSyncEvent.getBidderStatus());
            case NotificationEvent notificationEvent ->
                LogEvent.of(notificationEvent.getType().name(), getEventData(notificationEvent));
            case SetuidEvent setuidEvent -> LogEvent.of(
                    "/setuid",
                    setuidEvent.getBidder() + ":" + setuidEvent.getUid() + ":" + setuidEvent.getSuccess());
            case VideoEvent videoEvent -> LogEvent.of("/openrtb2/video", videoEvent.getBidResponse());
            case null, default -> LogEvent.of("unknown", null);
        };

        if (logEvent.getType().equals("win") || logEvent.getType().equals("imp")) {
            logger.info(new String(encoder.quoteAsString(mapper.encodeToString(logEvent))));
        }

        return Future.succeededFuture();
    }

    private JsonNode getEventData(NotificationEvent notificationEvent) {
        final EventRequest eventReq = notificationEvent.getEventRequest();
        try {
            return mapper.mapper().readTree("{\"account\":\"" + eventReq.getAccountId()
                    + "\",\"bidder\":\"" + new String(encoder.quoteAsString(eventReq.getBidder()))
                    + "\",\"price\":\"" + eventReq.getPrice().setScale(5, RoundingMode.HALF_DOWN)
                    .stripTrailingZeros().toPlainString()
                    + "\",\"url\":\"" + new String(encoder.quoteAsString(eventReq.getUrl()))
                    + "\",\"impId\":\"" + new String(encoder.quoteAsString(eventReq.getImpId()))
                    + "\",\"auctionId\":\"" + new String(encoder.quoteAsString(eventReq.getAuctionId())) + "\"}");
        } catch (JsonProcessingException e) {
            logger.error("Bids log adapter failed to parse JSON.");
        }
        return mapper.mapper().nullNode();
    }

    @Override
    public int vendorId() {
        return 0;
    }

    @Override
    public String name() {
        return "bidsAnalytics";
    }
}
