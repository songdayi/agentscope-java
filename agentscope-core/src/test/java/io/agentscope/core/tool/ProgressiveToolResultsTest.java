/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.core.tool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.tool.test.SampleTools;
import io.agentscope.core.util.JsonUtils;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Unit tests for progressive tool results feature.
 *
 * <p>Verifies that when {@code progressiveToolResults=true}, each tool's result is emitted
 * individually as soon as the tool completes, rather than waiting for all tools to finish.
 * Also verifies backward compatibility when the flag is disabled (default).
 */
@Tag("unit")
@DisplayName("Progressive Tool Results Tests")
class ProgressiveToolResultsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private Toolkit progressiveToolkit;
    private Toolkit batchToolkit;

    @BeforeEach
    void setUp() {
        progressiveToolkit =
                new Toolkit(ToolkitConfig.builder().progressiveToolResults(true).build());
        progressiveToolkit.registerTool(new SampleTools());

        batchToolkit = new Toolkit();
        batchToolkit.registerTool(new SampleTools());
    }

    @Test
    @DisplayName("progressiveToolResults should default to false")
    void testProgressiveToolResultsDefaultFalse() {
        ToolkitConfig defaultConfig = ToolkitConfig.defaultConfig();
        assertFalse(
                defaultConfig.isProgressiveToolResults(),
                "Default progressiveToolResults should be false for backward compatibility");
    }

    @Test
    @DisplayName("progressiveToolResults should be true when enabled via builder")
    void testProgressiveToolResultsEnabledViaBuilder() {
        ToolkitConfig config = ToolkitConfig.builder().progressiveToolResults(true).build();
        assertTrue(config.isProgressiveToolResults(), "progressiveToolResults should be true");
    }

    @Test
    @DisplayName("callToolsProgressive should emit each result as it completes")
    void testCallToolsProgressiveEmitsEachResult() {
        Map<String, Object> addInput = Map.of("a", 10, "b", 20);
        Map<String, Object> concatInput = Map.of("str1", "Hello", "str2", "World");

        ToolUseBlock addCall = createToolCall("call-add", "add", addInput);
        ToolUseBlock concatCall = createToolCall("call-concat", "concat", concatInput);

        Flux<ToolResultBlock> flux =
                progressiveToolkit.callToolsProgressive(
                        List.of(addCall, concatCall), null, null, null);

        StepVerifier.create(flux)
                .assertNext(
                        result -> {
                            assertNotNull(result, "First result should not be null");
                            String id = result.getId();
                            assertTrue(
                                    "call-add".equals(id) || "call-concat".equals(id),
                                    "Unexpected tool call id: " + id);
                        })
                .assertNext(
                        result -> {
                            assertNotNull(result, "Second result should not be null");
                            String id = result.getId();
                            assertTrue(
                                    "call-add".equals(id) || "call-concat".equals(id),
                                    "Unexpected tool call id: " + id);
                        })
                .verifyComplete();
    }

    @Test
    @DisplayName("callToolsProgressive should emit correct result content")
    void testCallToolsProgressiveCorrectContent() {
        Map<String, Object> addInput = Map.of("a", 10, "b", 20);
        Map<String, Object> concatInput = Map.of("str1", "Hello", "str2", "World");

        ToolUseBlock addCall = createToolCall("call-add", "add", addInput);
        ToolUseBlock concatCall = createToolCall("call-concat", "concat", concatInput);

        List<ToolResultBlock> results =
                progressiveToolkit
                        .callToolsProgressive(List.of(addCall, concatCall), null, null, null)
                        .collectList()
                        .block(TIMEOUT);

        assertNotNull(results, "Results should not be null");
        assertEquals(2, results.size(), "Should have two results");

        Map<String, ToolResultBlock> byId =
                results.stream().collect(Collectors.toMap(ToolResultBlock::getId, r -> r));

        ToolResultBlock addResult = byId.get("call-add");
        assertNotNull(addResult, "Add result should be present");
        assertEquals("30", extractFirstText(addResult));

        ToolResultBlock concatResult = byId.get("call-concat");
        assertNotNull(concatResult, "Concat result should be present");
        assertEquals("\"HelloWorld\"", extractFirstText(concatResult));
    }

    @Test
    @DisplayName("Progressive should not terminate on single tool error")
    void testProgressivePerToolErrorDoesNotTerminate() {
        Map<String, Object> errorInput = Map.of("message", "test failure");
        Map<String, Object> addInput = Map.of("a", 1, "b", 2);

        ToolUseBlock errorCall = createToolCall("call-error", "error_tool", errorInput);
        ToolUseBlock addCall = createToolCall("call-add", "add", addInput);

        List<ToolResultBlock> results =
                progressiveToolkit
                        .callToolsProgressive(List.of(errorCall, addCall), null, null, null)
                        .collectList()
                        .block(TIMEOUT);

        assertNotNull(results, "Results should not be null");
        assertEquals(2, results.size(), "Both results should be present despite one error");

        Map<String, ToolResultBlock> byId =
                results.stream().collect(Collectors.toMap(ToolResultBlock::getId, r -> r));

        ToolResultBlock errorResult = byId.get("call-error");
        assertNotNull(errorResult, "Error result should be present");
        String errorText = extractFirstText(errorResult);
        assertTrue(errorText.startsWith("Error:"), "Should contain error: " + errorText);

        ToolResultBlock addResult = byId.get("call-add");
        assertNotNull(addResult, "Add result should be present");
        assertEquals("3", extractFirstText(addResult), "Add tool should still succeed");
    }

    @Test
    @DisplayName("Batch and progressive should produce same results")
    void testBatchVsProgressiveSameResults() {
        Map<String, Object> addInput = Map.of("a", 10, "b", 20);
        Map<String, Object> concatInput = Map.of("str1", "Hello", "str2", "World");
        Map<String, Object> noParamInput = Map.of();

        ToolUseBlock addCall = createToolCall("call-add", "add", addInput);
        ToolUseBlock concatCall = createToolCall("call-concat", "concat", concatInput);
        ToolUseBlock noParamCall = createToolCall("call-noparam", "no_param", noParamInput);

        List<ToolResultBlock> batchResults =
                batchToolkit
                        .callTools(List.of(addCall, concatCall, noParamCall), null, null, null)
                        .block(TIMEOUT);

        List<ToolResultBlock> progressiveResults =
                progressiveToolkit
                        .callToolsProgressive(
                                List.of(addCall, concatCall, noParamCall), null, null, null)
                        .collectList()
                        .block(TIMEOUT);

        assertNotNull(batchResults, "Batch results should not be null");
        assertNotNull(progressiveResults, "Progressive results should not be null");
        assertEquals(batchResults.size(), progressiveResults.size(), "Result count should match");

        Map<String, String> batchTexts =
                batchResults.stream()
                        .collect(
                                Collectors.toMap(
                                        ToolResultBlock::getId,
                                        ProgressiveToolResultsTest::extractFirstText));
        Map<String, String> progressiveTexts =
                progressiveResults.stream()
                        .collect(
                                Collectors.toMap(
                                        ToolResultBlock::getId,
                                        ProgressiveToolResultsTest::extractFirstText));

        for (String id : batchTexts.keySet()) {
            assertEquals(
                    batchTexts.get(id),
                    progressiveTexts.get(id),
                    "Content should match for tool call: " + id);
        }
    }

    @Test
    @DisplayName("callToolsProgressive should handle empty tool list")
    void testCallToolsProgressiveEmptyList() {
        Flux<ToolResultBlock> flux =
                progressiveToolkit.callToolsProgressive(List.of(), null, null, null);

        StepVerifier.create(flux).verifyComplete();
    }

    @Test
    @DisplayName("callToolsProgressive should work in sequential mode")
    void testCallToolsProgressiveSequentialMode() {
        Toolkit sequentialToolkit =
                new Toolkit(
                        ToolkitConfig.builder()
                                .parallel(false)
                                .progressiveToolResults(true)
                                .build());
        sequentialToolkit.registerTool(new SampleTools());

        Map<String, Object> addInput = Map.of("a", 10, "b", 20);
        Map<String, Object> concatInput = Map.of("str1", "Hello", "str2", "World");

        ToolUseBlock addCall = createToolCall("call-add", "add", addInput);
        ToolUseBlock concatCall = createToolCall("call-concat", "concat", concatInput);

        List<ToolResultBlock> results =
                sequentialToolkit
                        .callToolsProgressive(List.of(addCall, concatCall), null, null, null)
                        .collectList()
                        .block(TIMEOUT);

        assertNotNull(results, "Results should not be null");
        assertEquals(2, results.size(), "Should have two results");

        assertEquals("call-add", results.get(0).getId(), "First result should be add");
        assertEquals("call-concat", results.get(1).getId(), "Second result should be concat");
    }

    @Test
    @DisplayName("Progressive should emit async tool results individually")
    void testProgressiveAsyncToolsEmittedIndividually() {
        Map<String, Object> addInput = Map.of("a", 5, "b", 10);
        Map<String, Object> concatInput = Map.of("str1", "Test", "str2", "Async");

        ToolUseBlock asyncAddCall = createToolCall("call-async-add", "async_add", addInput);
        ToolUseBlock asyncConcatCall =
                createToolCall("call-async-concat", "async_concat", concatInput);

        Flux<ToolResultBlock> flux =
                progressiveToolkit.callToolsProgressive(
                        List.of(asyncAddCall, asyncConcatCall), null, null, null);

        StepVerifier.create(flux).expectNextCount(1).expectNextCount(1).verifyComplete();
    }

    @Test
    @DisplayName("getConfig should return the toolkit configuration")
    void testGetConfigReturnsCorrectConfig() {
        ToolkitConfig config = progressiveToolkit.getConfig();
        assertNotNull(config, "Config should not be null");
        assertTrue(config.isProgressiveToolResults(), "Config should have progressive enabled");

        ToolkitConfig batchConfig = batchToolkit.getConfig();
        assertNotNull(batchConfig, "Config should not be null");
        assertFalse(
                batchConfig.isProgressiveToolResults(), "Config should have progressive disabled");
    }

    private ToolUseBlock createToolCall(String id, String name, Map<String, Object> input) {
        return ToolUseBlock.builder()
                .id(id)
                .name(name)
                .input(input)
                .content(JsonUtils.getJsonCodec().toJson(input))
                .build();
    }

    private static String extractFirstText(ToolResultBlock response) {
        List<ContentBlock> outputs = response.getOutput();
        if (outputs.isEmpty()) return "";
        return ((TextBlock) outputs.get(0)).getText();
    }
}
