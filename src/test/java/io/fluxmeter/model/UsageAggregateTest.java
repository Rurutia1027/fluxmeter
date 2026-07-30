package io.fluxmeter.model;

import io.fluxmeter.pricing.PricingCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UsageAggregateTest {

    @BeforeEach
    void loadCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void qwenMaxCostMicro() {
        UsageAggregate agg = new UsageAggregate();
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-qwen");
        event.setModelId("qwen-max");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(0);

        agg.addEvent(event);
        assertEquals(1_600_000L, agg.getCostMicro());
    }

    @Test
    void deduplicatesSameEventId() {
        UsageAggregate agg = new UsageAggregate();
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-1");
        event.setModelId("gpt-4o");
        event.setInputTokens(100);
        event.setOutputTokens(50);

        agg.addEvent(event);
        agg.addEvent(event);

        assertEquals(1, agg.getEventCount());
        assertEquals(100, agg.getInputTokens());
        assertEquals(50, agg.getOutputTokens());
        assertEquals(150, agg.getTotalTokens());
        assertEquals(1, agg.getDeduplicatedCount());
    }

    @Test
    void propagatesTenantIdFromEvent() {
        UsageAggregate agg = new UsageAggregate();
        TokenEvent event = new TokenEvent();
        event.setTenantId("tenant_abc");
        event.setCustomerId("cust_1");
        event.setModelId("gpt-4o");
        event.setInputTokens(10);

        agg.addEvent(event);
        assertEquals("tenant_abc", agg.getTenantId());
    }

    @Test
    void mergeCombinesCounters() {
        UsageAggregate a = new UsageAggregate();
        UsageAggregate b = new UsageAggregate();

        TokenEvent e1 = new TokenEvent();
        e1.setEventId("e1");
        e1.setModelId("gpt-4o");
        e1.setInputTokens(100);
        a.addEvent(e1);

        TokenEvent e2 = new TokenEvent();
        e2.setEventId("e2");
        e2.setModelId("gpt-4o");
        e2.setInputTokens(200);
        b.addEvent(e2);

        a.merge(b);
        assertEquals(2, a.getEventCount());
        assertEquals(300, a.getInputTokens());
    }

    @Test
    void calculateEventCostMicroFlatProviders() {
        // gpt-4o: 1_000_000 * 2.50 + 500_000 * 10.00 = 7_500_000
        assertEquals(7_500_000,
                UsageAggregate.calculateEventCostMicro(openaiGpt4oEvent()));
        // claude-sonnet-4: 1_000_000 * 3.00 + 100_00 * 15.00 = 4_500_000
        assertEquals(4_500_000,
                UsageAggregate.calculateEventCostMicro(anthorpicClaudeSonnet4Event()));
        // gemini-1.5-pro: 1_000_000 * 3.50 + 200_000 * 10.50 = 5_600_000
        assertEquals(5_600_000L,
                UsageAggregate.calculateEventCostMicro(googleGemini15ProEvent()));
    }

    @Test
    void calculateEventCostMicroFlatAllTokenCategories() {
        // gpt-4o + cache_read_multiplier=0.5
        // 1000*2.5 + 500*10 + 200*2.5*0.5 + 50*10 + 100*2.5 = 8500
        assertEquals(8_500L, UsageAggregate.calculateEventCostMicro(openaiGpt4oAllCategoriesEvent()));
        // text-embedding-3-small: 1_000_000 * 0.02 = 20_000
        assertEquals(20_000L,
                UsageAggregate.calculateEventCostMicro(openaiEmbeddingSmallEvent()));
    }

    @Test
    void calculateEventCost_dollarsFromMicro() {
        // 7_500_000 micro = 7.5 dollars
        assertEquals(7_500_000 / 1_000_000.0,
                UsageAggregate.calculateEventCost(openaiGpt4oEvent()), 1e-9);
    }

    @Test
    void normalizeModelId_stripsVersionSuffix() {
        assertEquals("gpt-4o", UsageAggregate.normalizeModelId("gpt-4o-2024-08-06"));
        assertEquals("deepseek-chat", UsageAggregate.normalizeModelId("deepseek-chat-v2"));
    }

    @Test
    void calculateEventCostMicroVolumeTiers() throws Exception {
        reloadTierCatalog();

        TokenEvent oneMillionInput = volumeGpt4oMiniEvent(1_000_000);

        // 9M before -> tier-1 (up_to 10M): 1M * 0.15 = 150_000
        assertEquals(150_000L, UsageAggregate.calculateEventCostMicro(oneMillionInput,
                9 * 1_000_000));

        // 10M before -> tier-2: 1M * 0.12 = 120_000
        assertEquals(120_000, UsageAggregate.calculateEventCostMicro(oneMillionInput,
                10 * 1_000_000));

        // 100M before -> last tier: 1M * 0.10 = 100_000
        assertEquals(100_000L, UsageAggregate.calculateEventCostMicro(oneMillionInput,
                100 * 1_000_000L));
    }

    @Test
    void calculateEventCostMicroVolumeTierBoundaryUsesMillionsFloor() throws Exception {
        reloadTierCatalog();

        TokenEvent event = volumeGpt4oMiniEvent(100_000); // 0.1M tokens
        // 9_999_999 tokens ~ 9M tokens, still tier-1: 0.1 M * 0.15 = 100_000 * 0.15 = 15_000
        assertEquals(15_000L,
                UsageAggregate.calculateEventCostMicro(event, 9_999_999L));
    }

    @Test
    void calculateEventCostMicroGraduatedTiers() throws Exception {
        reloadTierCatalog();

        // all in first tier: input 50K×2.0 + output 50K×4.0 = 300_000
        assertEquals(300_000L, UsageAggregate.calculateEventCostMicro(
                        graduatedClaudeSonnet4Event(50_000, 50_000),
                        0L));

        // monthlyBefore=900K: input 100K @tier1 (input token = 2.0) + output 100K @tier2 (output token = 2.0)
        // => 100_000 * 2.0 + 100_000 * 2.0 = 400_000
        assertEquals(400_000L, UsageAggregate.calculateEventCostMicro(
                        graduatedClaudeSonnet4Event(100_000, 100_000),
                        900_000L));

        // monthlyBefore already past first tier: 200K input @ 1.0 = 200_000
        // monthlyBefore = 1_500_000L
        // => input tokens all in @tier2(input token = 1.0), micro cost = 1.0 * 200_000 = 200_000
        assertEquals(200_000L,
                UsageAggregate.calculateEventCostMicro(
                        graduatedClaudeSonnet4Event(200_000, 0),
                        1_500_000L));
    }

    @Test
    void addEventUsesMonthlyTokensBeforeForVolumeTiers() throws Exception {
        reloadTierCatalog();
        UsageAggregate agg = new UsageAggregate();
        agg.addEvent(volumeGpt4oMiniEvent(1_000_000), 10_000_000L);
        assertEquals(120_000L, agg.getCostMicro());
    }

    // --- event generate functions ---

    /**
     * OpenAI / gpt-4o - catalog: input_per_m = 2.50
     * <p>
     * todo: take consideration of adding a upstream token price subscriber flink job (1h,
     * per day) -> sync to -> cache/db, and let flink main job fetch latest fresh price from
     * official website.
     */
    private TokenEvent openaiGpt4oEvent() {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-openai-gpt4o");
        event.setProvider("openai");
        event.setModelId("gpt-4o");
        event.setCustomerId("cust_openai");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(500_000);
        return event;
    }

    /**
     * Anthropic / claude-sonnet-4 - catalog: input_per_m=3.00, output_per_m=15.00
     * expected micro = 1_000_000 * 3.00 + 100_000 * 15.00 = 4_500_000
     */
    private TokenEvent anthorpicClaudeSonnet4Event() {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-anthropic-sonnet4");
        event.setProvider("anthropic");
        event.setModelId("claude-sonnet-4");
        event.setCustomerId("cust_anthropic");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(100_000);
        return event;
    }

    /**
     * Google / gemini-1.5-pro - catalog: input_per_m=3.50, output_per_m=10.50
     * expected micro = 1_000_000 * 3.50 + 200_000 * 10.50 = 5_600_000
     */
    private TokenEvent googleGemini15ProEvent() {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-google-gemini15pro");
        event.setProvider("google");
        event.setModelId("gemini-1.5-pro");
        event.setCustomerId("cust_google");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(200_000);
        return event;
    }

    /**
     * Flat path: hit input/output/cacheRead/cacheWrite/reasoning in costAtTier
     */
    private TokenEvent openaiGpt4oAllCategoriesEvent() {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-openai-all-cats");
        event.setProvider("openai");
        event.setModelId("gpt-4o");
        event.setCustomerId("cust_openai");
        event.setInputTokens(1_000);
        event.setOutputTokens(500);
        event.setCacheReadTokens(200);
        event.setCacheWriteTokens(100);
        event.setReasoningTokens(50);
        return event;
    }

    private TokenEvent openaiEmbeddingSmallEvent() {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-openai-embed");
        event.setProvider("openai");
        event.setModelId("text-embedding-3-small");
        event.setEmbeddingTokens(1_000_000);
        return event;
    }

    /**
     * Copied from {@link PricingCatalogTest} tier fixtures.
     */
    private void reloadTierCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    /**
     * Volume model from contrib/pricing/tiered-example.json
     */
    private TokenEvent volumeGpt4oMiniEvent(int inputTokens) {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-volume-gpt4o-mini-" + inputTokens);
        event.setProvider("openai");
        event.setModelId("gpt-4o-mini");
        event.setCustomerId("cust_volume");
        event.setInputTokens(inputTokens);
        event.setOutputTokens(0);
        return event;
    }

    /**
     * Graduated model from contrib/pricing/tiered-example.json
     */
    private TokenEvent graduatedClaudeSonnet4Event(int inputTokens, int outputTokens) {
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-grad-sonnet4-" + inputTokens + "-" + outputTokens);
        event.setProvider("anthropic");
        event.setModelId("claude-sonnet-4");
        event.setCustomerId("cust_grad");
        event.setInputTokens(inputTokens);
        event.setOutputTokens(outputTokens);
        return event;
    }
}
