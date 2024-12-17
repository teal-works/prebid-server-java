package org.prebid.server.events;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * Represents event request.
 */
@Builder
@Value
public class EventRequest {

    Type type;

    String bidId;

    String auctionId;

    String accountId;

    String bidder;

    Long timestamp;

    Format format;

    String integration;

    Analytics analytics;

    BigDecimal price;

    String url;

    String impId;

    String abTestUuid;

    public enum Type {

        win, imp
    }

    public enum Format {

        blank, image
    }

    public enum Analytics {

        enabled, disabled
    }
}
