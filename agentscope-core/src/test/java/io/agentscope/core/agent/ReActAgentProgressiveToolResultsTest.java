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
package io.agentscope.core.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.tool.AgentTool;
import io.agentscope.core.tool.ToolCallParam;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.core.tool.ToolkitConfig;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** End-to-end tests for progressive tool results in the ReAct loop. */
class ReActAgentProgressiveToolResultsTest {

    private static final class ScriptedModel extends ChatModelBase {
        private final List<Supplier<Flux<ChatResponse>>> scripts;
        private final AtomicInteger idx = new AtomicInteger(0);

        ScriptedModel(List<Supplier<Flux<ChatResponse>>> scripts) {
            this.scripts = scripts;
        }

        @Override
        public String getModelName() {
            return "scripted";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            int i = idx.getAndIncrement();
            if (i >= scripts.size()) {
                return Flux.just(textResponse(""));
            }
            return scripts.get(i).get();
        }
    }

    private static ChatResponse textResponse(String text) {
        return ChatResponse.builder()
                .content(List.<ContentBlock>of(TextBlock.builder().text(text).build()))
                .build();
    }

    private static ChatResponse multiToolUseResponse(
            String toolId1, String toolName1, String toolId2, String toolName2) {
        return ChatResponse.builder()
                .content(
                        List.<ContentBlock>of(
                                ToolUseBlock.builder()
                                        .id(toolId1)
                                        .name(toolName1)
                                        .input(Map.of("query", "hello"))
                                        .build(),
                                ToolUseBlock.builder()
                                        .id(toolId2)
                                        .name(toolName2)
                                        .input(Map.of("query", "world"))
                                        .build()))
                .build();
    }

    private static ChatResponse singleToolUseResponse(String toolId, String toolName) {
        return ChatResponse.builder()
                .content(
                        List.<ContentBlock>of(
                                ToolUseBlock.builder()
                                        .id(toolId)
                                        .name(toolName)
                                        .input(Map.of("query", "test"))
                                        .build()))
                .build();
    }

    private static final class DelayedEchoTool implements AgentTool {
        private final String name;
        private final long delayMs;

        DelayedEchoTool(String name, long delayMs) {
            this.name = name;
            this.delayMs = delayMs;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return "echoes the input after a delay";
        }

        @Override
        public Map<String, Object> getParameters() {
            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "object");
            Map<String, Object> props = new HashMap<>();
            Map<String, Object> q = new HashMap<>();
            q.put("type", "string");
            props.put("query", q);
            schema.put("properties", props);
            return schema;
        }

        @Override
        public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
            Object q = param.getInput() == null ? "" : param.getInput().get("query");
            if (delayMs <= 0) {
                return Mono.just(ToolResultBlock.text("echo:" + q));
            }
            return Mono.delay(Duration.ofMillis(delayMs))
                    .map(tick -> ToolResultBlock.text("echo:" + q));
        }
    }

    private static final class ErrorTool implements AgentTool {
        @Override
        public String getName() {
            return "error_tool";
        }

        @Override
        public String getDescription() {
            return "always fails";
        }

        @Override
        public Map<String, Object> getParameters() {
            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "object");
            Map<String, Object> props = new HashMap<>();
            Map<String, Object> q = new HashMap<>();
            q.put("type", "string");
            props.put("query", q);
            schema.put("properties", props);
            return schema;
        }

        @Override
        public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
            return Mono.error(new RuntimeException("intentional failure"));
        }
    }

    private static Toolkit progressiveToolkitWith(AgentTool... tools) {
        Toolkit toolkit = new Toolkit(ToolkitConfig.builder().progressiveToolResults(true).build());
        for (AgentTool tool : tools) {
            toolkit.registerAgentTool(tool);
        }
        return toolkit;
    }

    private static Toolkit batchToolkitWith(AgentTool... tools) {
        Toolkit toolkit = new Toolkit();
        for (AgentTool tool : tools) {
            toolkit.registerAgentTool(tool);
        }
        return toolkit;
    }

    private static int indexOf(List<AgentEvent> events, Class<?> cls) {
        for (int i = 0; i < events.size(); i++) {
            if (cls.isInstance(events.get(i))) return i;
        }
        return -1;
    }

    @Test
    void progressiveMultipleToolsAgentCompletes() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc1", "echo_a", "tc2", "echo_b")),
                                () -> Flux.just(textResponse("done"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_a", 0),
                                        new DelayedEchoTool("echo_b", 0)))
                        .build();

        Msg userMsg = Msg.builder().role(MsgRole.USER).textContent("test").build();
        Msg result = agent.call(List.of(userMsg)).block(Duration.ofSeconds(10));

        assertNotNull(result);
        assertEquals(MsgRole.ASSISTANT, result.getRole());
        List<TextBlock> texts = result.getContentBlocks(TextBlock.class);
        assertEquals(1, texts.size());
        assertEquals("done", texts.get(0).getText());
    }

    @Test
    void allToolResultEndsEmittedBeforeAgentEnd() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc1", "echo_a", "tc2", "echo_b")),
                                () -> Flux.just(textResponse("finished"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_a", 0),
                                        new DelayedEchoTool("echo_b", 0)))
                        .build();

        List<AgentEvent> events = agent.streamEvents(List.of()).collectList().block();

        assertNotNull(events);

        int iAgentEnd = indexOf(events, AgentEndEvent.class);
        assertTrue(iAgentEnd >= 0);

        List<ToolResultEndEvent> resultEnds =
                events.stream()
                        .filter(e -> e instanceof ToolResultEndEvent)
                        .map(e -> (ToolResultEndEvent) e)
                        .toList();
        assertEquals(2, resultEnds.size());

        for (ToolResultEndEvent tre : resultEnds) {
            int idx = events.indexOf(tre);
            assertTrue(idx < iAgentEnd, "ToolResultEndEvent should come before AgentEndEvent");
        }
    }

    @Test
    void toolResultStartEmittedBeforeEachEnd() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc1", "echo_a", "tc2", "echo_b")),
                                () -> Flux.just(textResponse("ok"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_a", 0),
                                        new DelayedEchoTool("echo_b", 0)))
                        .build();

        List<AgentEvent> events = agent.streamEvents(List.of()).collectList().block();
        assertNotNull(events);

        List<ToolResultStartEvent> starts =
                events.stream()
                        .filter(e -> e instanceof ToolResultStartEvent)
                        .map(e -> (ToolResultStartEvent) e)
                        .toList();
        List<ToolResultEndEvent> ends =
                events.stream()
                        .filter(e -> e instanceof ToolResultEndEvent)
                        .map(e -> (ToolResultEndEvent) e)
                        .toList();

        assertEquals(2, starts.size());
        assertEquals(2, ends.size());

        for (ToolResultEndEvent end : ends) {
            int endIdx = events.indexOf(end);
            ToolResultStartEvent matchingStart =
                    starts.stream()
                            .filter(s -> s.getToolCallId().equals(end.getToolCallId()))
                            .findFirst()
                            .orElse(null);
            assertNotNull(matchingStart);
            int startIdx = events.indexOf(matchingStart);
            assertTrue(startIdx < endIdx, "ToolResultStart should come before ToolResultEnd");
        }
    }

    @Test
    void toolErrorDoesNotPreventAgentCompletion() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc1", "error_tool", "tc2", "echo_b")),
                                () -> Flux.just(textResponse("recovered"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new ErrorTool(), new DelayedEchoTool("echo_b", 0)))
                        .build();

        Msg userMsg = Msg.builder().role(MsgRole.USER).textContent("test").build();
        Msg result = agent.call(List.of(userMsg)).block(Duration.ofSeconds(10));

        assertNotNull(result);
        assertEquals("recovered", result.getContentBlocks(TextBlock.class).get(0).getText());
    }

    @Test
    void toolResultsPassedToNextIteration() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc1", "echo_a", "tc2", "echo_b")),
                                () -> Flux.just(textResponse("all done"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_a", 0),
                                        new DelayedEchoTool("echo_b", 0)))
                        .build();

        Msg userMsg = Msg.builder().role(MsgRole.USER).textContent("test").build();
        agent.call(List.of(userMsg)).block(Duration.ofSeconds(10));

        List<Msg> context = agent.getAgentState().getContext();
        List<Msg> toolMsgs = context.stream().filter(m -> m.getRole() == MsgRole.TOOL).toList();
        assertTrue(toolMsgs.size() >= 1, "Should have at least one tool result message");

        int totalToolResults =
                toolMsgs.stream()
                        .mapToInt(m -> m.getContentBlocks(ToolResultBlock.class).size())
                        .sum();
        assertEquals(
                2, totalToolResults, "Should have two ToolResultBlocks across all tool messages");
    }

    @Test
    void progressiveAndBatchProduceSameFinalReply() {
        Supplier<Flux<ChatResponse>> firstTurn =
                () -> Flux.just(multiToolUseResponse("tc1", "echo_a", "tc2", "echo_b"));
        Supplier<Flux<ChatResponse>> secondTurn = () -> Flux.just(textResponse("final"));

        ReActAgent progressiveAgent =
                ReActAgent.builder()
                        .name("progressive")
                        .model(new ScriptedModel(List.of(firstTurn, secondTurn)))
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_a", 10),
                                        new DelayedEchoTool("echo_b", 20)))
                        .build();

        ReActAgent batchAgent =
                ReActAgent.builder()
                        .name("batch")
                        .model(new ScriptedModel(List.of(firstTurn, secondTurn)))
                        .toolkit(
                                batchToolkitWith(
                                        new DelayedEchoTool("echo_a", 10),
                                        new DelayedEchoTool("echo_b", 20)))
                        .build();

        Msg userMsg = Msg.builder().role(MsgRole.USER).textContent("test").build();

        Msg progressiveResult =
                progressiveAgent.call(List.of(userMsg)).block(Duration.ofSeconds(10));
        Msg batchResult = batchAgent.call(List.of(userMsg)).block(Duration.ofSeconds(10));

        assertNotNull(progressiveResult);
        assertNotNull(batchResult);
        assertEquals(
                batchResult.getContentBlocks(TextBlock.class).get(0).getText(),
                progressiveResult.getContentBlocks(TextBlock.class).get(0).getText());
    }

    @Test
    void progressiveSingleToolCallCompletes() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () -> Flux.just(singleToolUseResponse("tc1", "echo_a")),
                                () -> Flux.just(textResponse("single done"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(progressiveToolkitWith(new DelayedEchoTool("echo_a", 0)))
                        .build();

        Msg userMsg = Msg.builder().role(MsgRole.USER).textContent("test").build();
        Msg result = agent.call(List.of(userMsg)).block(Duration.ofSeconds(10));

        assertNotNull(result);
        assertEquals("single done", result.getContentBlocks(TextBlock.class).get(0).getText());
    }

    @Test
    void delayedToolsEmitResultsInCompletionOrder() {
        ChatModelBase model =
                new ScriptedModel(
                        List.of(
                                () ->
                                        Flux.just(
                                                multiToolUseResponse(
                                                        "tc-slow",
                                                        "echo_slow",
                                                        "tc-fast",
                                                        "echo_fast")),
                                () -> Flux.just(textResponse("ordered"))));

        ReActAgent agent =
                ReActAgent.builder()
                        .name("progressive-agent")
                        .model(model)
                        .toolkit(
                                progressiveToolkitWith(
                                        new DelayedEchoTool("echo_slow", 200),
                                        new DelayedEchoTool("echo_fast", 10)))
                        .build();

        List<AgentEvent> events = agent.streamEvents(List.of()).collectList().block();
        assertNotNull(events);

        List<ToolResultEndEvent> ends =
                events.stream()
                        .filter(e -> e instanceof ToolResultEndEvent)
                        .map(e -> (ToolResultEndEvent) e)
                        .toList();

        assertEquals(2, ends.size());

        String firstEndId = ends.get(0).getToolCallId();
        String secondEndId = ends.get(1).getToolCallId();
        assertTrue(!firstEndId.equals(secondEndId));
        assertTrue("tc-fast".equals(firstEndId) || "tc-slow".equals(firstEndId));
    }
}
