package org.prebid.server.events;

import org.prebid.server.proto.openrtb.ext.response.Events;
import org.prebid.server.util.HttpUtil;

import java.math.BigDecimal;
import java.util.Objects;

public class EventsService {

    private final String externalUrl;
    private final boolean extendedEvents;

    public EventsService(String externalUrl) {
        this.externalUrl = HttpUtil.validateUrl(Objects.requireNonNull(externalUrl));
        this.extendedEvents = this.externalUrl.contains("bids.ws");
    }

    /**
     * Returns {@link Events} object based on given params.
     */
    public Events createEvent(String bidId,
                              String bidder,
                              String accountId,
                              boolean analyticsEnabled,
                              EventsContext eventsContext) {

        return Events.of(
                eventUrl(
                        EventRequest.Type.win,
                        bidId,
                        bidder,
                        null,
                        null,
                        null,
                        accountId,
                        analytics(analyticsEnabled),
                        EventRequest.Format.image,
                        eventsContext),
                eventUrl(
                        EventRequest.Type.imp,
                        bidId,
                        bidder,
                        null,
                        null,
                        null,
                        accountId,
                        analytics(analyticsEnabled),
                        EventRequest.Format.image,
                        eventsContext));
    }

    /**
     * Returns {@link Events} object based on given params.
     */
    public Events createEvent(String bidId,
                              String bidder,
                              BigDecimal price,
                              String url,
                              String impId,
                              String accountId,
                              boolean analyticsEnabled,
                              EventsContext eventsContext) {

        return Events.of(
                eventUrl(
                        EventRequest.Type.win,
                        bidId,
                        bidder,
                        extendedEvents ? price : null,
                        extendedEvents ? url : null,
                        extendedEvents ? impId : null,
                        accountId,
                        analytics(analyticsEnabled),
                        EventRequest.Format.image,
                        eventsContext),
                eventUrl(
                        EventRequest.Type.imp,
                        bidId,
                        bidder,
                        extendedEvents ? price : null,
                        extendedEvents ? url : null,
                        extendedEvents ? impId : null,
                        accountId,
                        analytics(analyticsEnabled),
                        EventRequest.Format.image,
                        eventsContext));
    }

    /**
     * Returns url for win tracking.
     */
    public String winUrl(String bidId,
                         String bidder,
                         String accountId,
                         boolean analyticsEnabled,
                         EventsContext eventsContext) {

        return eventUrl(
                EventRequest.Type.win,
                bidId,
                bidder,
                null,
                null,
                null,
                accountId,
                analytics(analyticsEnabled),
                EventRequest.Format.image,
                eventsContext);
    }

    /**
     * Returns url for win tracking.
     */
    public String winUrl(String bidId,
                         String bidder,
                         BigDecimal price,
                         String url,
                         String impId,
                         String accountId,
                         boolean analyticsEnabled,
                         EventsContext eventsContext) {

        return eventUrl(
                EventRequest.Type.win,
                bidId,
                bidder,
                extendedEvents ? price : null,
                extendedEvents ? url : null,
                extendedEvents ? impId : null,
                accountId,
                analytics(analyticsEnabled),
                EventRequest.Format.image,
                eventsContext);
    }

    /**
     * Returns url for VAST tracking.
     */
    public String vastUrlTracking(String bidId,
                                  String bidder,
                                  String accountId,
                                  EventsContext eventsContext) {

        return eventUrl(EventRequest.Type.imp,
                bidId,
                bidder,
                null,
                null,
                null,
                accountId,
                null,
                EventRequest.Format.blank,
                eventsContext);
    }

    private String eventUrl(EventRequest.Type type,
                            String bidId,
                            String bidder,
                            BigDecimal price,
                            String url,
                            String impId,
                            String accountId,
                            EventRequest.Analytics analytics,
                            EventRequest.Format format,
                            EventsContext eventsContext) {

        final EventRequest eventRequest = EventRequest.builder()
                .type(type)
                .bidId(bidId)
                .auctionId(eventsContext.getAuctionId())
                .accountId(accountId)
                .bidder(bidder)
                .price(price)
                .url(url)
                .impId(impId)
                .abTestUuid(eventsContext.getState().getAbTestUuid())
                .timestamp(eventsContext.getAuctionTimestamp())
                .format(format)
                .integration(eventsContext.getIntegration())
                .analytics(analytics)
                .build();

        return EventUtil.toUrl(externalUrl, eventRequest);
    }

    private static EventRequest.Analytics analytics(boolean analyticsEnabled) {
        return analyticsEnabled ? null : EventRequest.Analytics.disabled;
    }
}
