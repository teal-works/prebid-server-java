package org.prebid.server.validation;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.sovrn.proto.ExtImpSovrn;
import org.prebid.server.proto.openrtb.ext.request.adform.ExtImpAdform;
import org.prebid.server.proto.openrtb.ext.request.appnexus.ExtImpAppnexus;
import org.prebid.server.proto.openrtb.ext.request.rubicon.ExtImpRubicon;
import org.prebid.server.util.ResourceUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.given;

public class BidderParamValidatorTest extends VertxTest {

    private static final String RUBICON = "rubicon";
    private static final String APPNEXUS = "appnexus";
    private static final String ADFORM = "adform";
    private static final String SOVRN = "sovrn";

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private BidderCatalog bidderCatalog;

    private BidderParamValidator bidderParamValidator;

    @Before
    public void setUp() {
        given(bidderCatalog.names()).willReturn(new HashSet<>(asList(RUBICON, APPNEXUS, ADFORM, SOVRN)));

        bidderParamValidator = BidderParamValidator.create(bidderCatalog, "static/bidder-params");
    }

    @Test
    public void createShouldFailOnNullArguments() {
        assertThatNullPointerException().isThrownBy(() -> BidderParamValidator.create(null, null));
        assertThatNullPointerException().isThrownBy(() -> BidderParamValidator.create(bidderCatalog, null));
    }

    @Test
    public void createShouldFailOnInvalidSchemaPath() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> BidderParamValidator.create(bidderCatalog, "noschema"));
    }

    @Test
    public void createShouldFailOnEmptySchemaFile() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> BidderParamValidator.create(bidderCatalog, "org/prebid/server/validation/schema/empty"));
    }

    @Test
    public void createShouldFailOnInvalidSchemaFile() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> BidderParamValidator.create(bidderCatalog, "org/prebid/server/validation/schema/invalid"));
    }

    @Test
    public void validateShouldNotReturnValidationMessagesWhenRubiconImpExtIsOk() {
        // given
        final ExtImpRubicon ext = ExtImpRubicon.builder().accountId(1).siteId(2).zoneId(3).build();
        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        // when
        final Set<String> messages = bidderParamValidator.validate(RUBICON, node);

        // then
        assertThat(messages).isEmpty();
    }

    @Test
    public void validateShouldReturnValidationMessagesWhenRubiconImpExtNotValid() {
        // given
        final ExtImpRubicon ext = ExtImpRubicon.builder().siteId(2).zoneId(3).build();

        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        final Set<String> messages = bidderParamValidator.validate(RUBICON, node);

        // then
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    public void validateShouldReturnValidationMessagesWhenAppnexusImpExtNotValid() {
        // given
        final ExtImpAppnexus ext = ExtImpAppnexus.builder().member("memberId").build();

        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        // when
        final Set<String> messages = bidderParamValidator.validate(APPNEXUS, node);

        // then
        assertThat(messages.size()).isEqualTo(4);
    }

    @Test
    public void validateShouldNotReturnValidationMessagesWhenAppnexusImpExtIsOk() {
        // given
        final ExtImpAppnexus ext = ExtImpAppnexus.builder().placementId(1).build();

        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        // when
        final Set<String> messages = bidderParamValidator.validate(APPNEXUS, node);

        // then
        assertThat(messages).isEmpty();
    }

    @Test
    public void validateShouldNotReturnValidationMessagesWhenAdformImpExtIsOk() {
        // given
        final ExtImpAdform ext = ExtImpAdform.of(15L);

        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        // when
        final Set<String> messages = bidderParamValidator.validate(ADFORM, node);

        // then
        assertThat(messages).isEmpty();
    }

    @Test
    public void validateShouldReturnValidationMessagesWhenAdformImpExtNotValid() {
        // given
        final JsonNode node = mapper.createObjectNode();

        // when
        final Set<String> messages = bidderParamValidator.validate(ADFORM, node);

        // then
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    public void validateShouldNotReturnValidationMessagesWhenSovrnImpExtIsOk() {
        // given
        final ExtImpSovrn ext = ExtImpSovrn.of("tag", null);

        final JsonNode node = mapper.convertValue(ext, JsonNode.class);

        // when
        final Set<String> messages = bidderParamValidator.validate(SOVRN, node);

        // then
        assertThat(messages).isEmpty();
    }

    @Test
    public void validateShouldReturnValidationMessagesWhenAdformSovrnExtNotValid() {
        // given
        final JsonNode node = mapper.createObjectNode();

        // when
        final Set<String> messages = bidderParamValidator.validate(SOVRN, node);

        // then
        assertThat(messages.size()).isEqualTo(1);
    }

    @Test
    public void schemaShouldReturnSchemasString() throws IOException {
        // given
        given(bidderCatalog.names()).willReturn(new HashSet<>(asList("test-rubicon", "test-appnexus")));

        bidderParamValidator = BidderParamValidator.create(bidderCatalog, "org/prebid/server/validation/schema/valid");

        // when
        final String result = bidderParamValidator.schemas();

        // then
        assertThat(result).isEqualTo(ResourceUtil.readFromClasspath(
                "org/prebid/server/validation/schema//valid/test-schemas.json"));
    }
}
