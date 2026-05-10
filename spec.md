This is the technical specification for **`@nilejs/future`**. This document outlines the hybrid actor model, the memory-mapped communication tiers, and the architectural justifications for your development team.

---

# Technical Specification: @nilejs/future

## 1. Vision & Overview

`@nilejs/future` is a high-performance, system-level actor framework for Bun and Node.js. It facilitates isolated concurrent execution using a **Two-Gear Communication** model, balancing ease of use with zero-copy data transfer.

---

## 2. Architecture Decision Records (ADR)

### ADR 001: Callback-Based Spawning

* **Decision:** Actors are spawned via serialized callbacks rather than external files.
* **Why:** Allows the Supervisor to inject "Resource Manager" proxies and context (`ctx`) automatically. It improves Developer Experience (DX) by keeping logic co-located.
* **Trade-off:** Closures are lost; variables from the parent scope must be passed explicitly via `msg`.

### ADR 002: Two-Gear Communication (Hybrid Tier)

* **Decision:** Split messaging into Tier 1 (Signals) and Tier 2 (Data).
* **Why:** `postMessage` is too slow for large buffers due to serialization tax. SharedArrayBuffer (SAB) is too complex for simple status updates.
* **Strategy:** Tier 1 uses native messaging; Tier 2 uses a Shared Memory "Deposit Box."

### ADR 003: Dedicated $N+1$ Memory Lanes

* **Decision:** Allocate `workerCount + 1` boxes in a fixed-size SAB.
* **Why:** To prevent "Lock Contention." Every worker has a dedicated lane to write to immediately. The `+1` box is a global overflow/swap lane.

---

## 3. Technical Architecture

### Memory Layout (The Shared Bus)

The `SharedArrayBuffer` is divided into a **Control Header** and a **Data Region**.

| Section | Type | Description |
| --- | --- | --- |
| **State Board** | `Int32Array` | Indices tracking Box state: `0` (Clean), `1` (Locked/Writing), `2` (Ready). |
| **Lease Tracker** | `BigInt64Array` | Timestamps of when a lock was acquired to prevent deadlocks. |
| **Data Boxes** | `Uint8Array` | The actual memory segments (Size-classed: SM, MD, LG). |

### The Two-Tier System

1. **Tier 1 (Control):** A PubSub implementation over `worker.postMessage`. Used for `self.send()` and `ctx.resources` (Resource Manager) intents.
2. **Tier 2 (Data):** Atomic-locked memory access. Used for `ctx.deposit()`. Uses `Atomics.compareExchange` for non-blocking lock acquisition.

---

## 4. Implementation Details for Devs

### Lock Acquisition (FIFO)

If a worker requests a lock and no boxes of the requested size are available:

1. The request is pushed to an internal `Queue<Resolver>` in the Main Process.
2. When a box is marked `0` (Clean) by a retriever, the next resolver is triggered.
3. The worker receives the `lock` object containing the `byteOffset` and `length`.

### The Resource Manager (Intent Relay)

Inside the worker callback, `ctx.resources` is a **Proxy**.

* **Call:** `ctx.resources.db.save(data)`
* **Action:** Proxy intercepts the call, packages it as an "Intent Packet," and sends it via Tier 1 to the Supervisor.
* **Return:** Supervisor executes the real DB logic and sends the result back to the worker.

---

## 5. Component Interface (The DSL)

### Supervisor Definition

```typescript
const supervisor = createSupervisor({
  resources: {
    db: { /* Handlers */ }
  },
  memory: {
    poolSize: 10, // Max concurrent boxes
    boxSize: '1mb'
  }
});

```

### Actor Callback Interface

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // Tier 1
  self.send({ status: 'active' });

  // Tier 2
  const lock = await ctx.acquireLock(); 
  ctx.deposit(lock, uint8Data);
  ctx.done(lock);
});

// Main Program
actor.subscribe(msg => {
  // Handles Tier 1 signals and Tier 2 'Done' notifications
});

```

---

## 6. Safety & Lifecycle

* **Heartbeat/Lease:** The Supervisor monitors the **Lease Tracker**. If a box is held for $>5000ms$ (default), the Supervisor kills the worker and clears the lock.
* **Cleanup:** When an actor is terminated, its associated box is immediately cleared (Atomics reset to `0`).
* **Error Handling:** Use `slang-ts` `Result` types for all async operations like `acquireLock`.

---

## 7. Delivery Milestones

1. **Phase 1:** Tier 1 PubSub and Callback Serialization (Bun/Node).
2. **Phase 2:** SAB implementation with Atomic locking and fixed-size boxes.
3. **Phase 3:** Resource Manager Proxy implementation.
4. **Phase 4:** FIFO Queue and Backpressure logic.

---

**Dev Team Note:** Focus on the `Atomics` logic first. If the state-board transitions (`0 -> 1 -> 2 -> 0`) are not rock-solid, the system will leak memory lanes. Use `Atomics.wait` and `Atomics.notify` for the FIFO queuing mechanism.

To make `@nilejs/future` production-ready, the API needs to feel like high-level TypeScript while performing like low-level C. Below are end-user scenarios and the refined specification for the **Heartbeat & Lease Renewal** mechanism.

---

### 1. Scenario: The Real-Time Financial Processor

In this scenario, the actor processes a massive stream of transactions. It uses Tier 1 to report progress and Tier 2 to "deposit" the final reconciled report.

```typescript
import { createSupervisor } from "@nilejs/future";
import { Ok, match } from "slang-ts";

const supervisor = createSupervisor({
  resources: {
    bankApi: { verify: async (id) => /* ... */ }
  }
});

const reconciler = supervisor.spawn(async (self, msg, ctx) => {
  const transactions = msg.batch;
  const results = new Uint8Array(transactions.length);

  for (let i = 0; i < transactions.length; i++) {
    // 1. Heavy logic + Resource Manager usage
    const isValid = await ctx.resources.bankApi.verify(transactions[i].id);
    results[i] = isValid ? 1 : 0;

    // 2. Heartbeat: Reset the lease counter during long loops
    if (i % 100 === 0) ctx.heartbeat(); 
    
    // 3. Tier 1: Small progress update
    self.send({ type: 'PROGRESS', value: i / transactions.length });
  }

  // 4. Tier 2: Deposit the heavy result
  const lock = await ctx.acquireLock({ size: 'md' });
  ctx.deposit(lock, results);
  ctx.done(lock);
});

// Userland Consumption
reconciler.subscribe((msg) => {
  if (msg.type === 'PROGRESS') println(`Progress: ${msg.value * 100}%`);
  if (msg.type === 'DEPOSIT_READY') {
     const data = reconciler.read(msg.address); // Zero-copy read from SAB
     println("Batch reconciled.");
  }
});

```

---

### 2. Scenario: AI Media Transcoder

Actors are perfect for "Burst Memory" tasks. Here, an actor converts an image, resetting the heartbeat during intensive pixel manipulation.

```typescript
const transcoder = supervisor.spawn(async (self, msg, ctx) => {
  const rawImage = await ctx.resources.storage.get(msg.fileId);
  
  // Start high-gear work
  const lock = await ctx.acquireLock({ size: 'lg' });
  
  // Mocking a long pixel-by-pixel transformation
  for (let row = 0; row < rawImage.height; row++) {
    processRow(rawImage, row);
    
    // Explicit Heartbeat: Tells supervisor "I am still alive, don't kill me"
    ctx.heartbeat(); 
  }

  ctx.deposit(lock, rawImage.buffer);
  ctx.done(lock);
});

```

---

### 3. The Heartbeat Logic (Technical Spec Addendum)

The Heartbeat is the "Dead Man's Switch" that prevents a worker from holding a memory box hostage if it hangs.

**ADR 004: Explicit & Implicit Heartbeats**

* **Decision:** Implement a `Lease` system where a lock expires after 5000ms unless a heartbeat is received.
* **Mechanism:**
1. **Implicit:** Every Tier 1 `self.send()` or Tier 2 `ctx.deposit()` automatically updates the `Lease Tracker` (timestamp) in the SAB Header.
2. **Explicit:** `ctx.heartbeat()` allows the dev to manually update the timestamp during high-CPU loops that don't perform I/O.


* **Supervisor Action:** A "Watchdog" interval in the Main Thread checks the `Lease Tracker`. If `Date.now() - last_heartbeat > timeout`:
* **Panic:** The Supervisor terminates the worker thread.
* **Recycle:** The Box state is set to `0` (Clean) and returned to the FIFO queue.



---

### 4. Summary of Tier Semantics

| Action | Tier | Heartbeat Trigger | Result |
| --- | --- | --- | --- |
| `self.send(msg)` | 1 | **Yes (Implicit)** | PubSub event in Main Thread. |
| `ctx.heartbeat()` | 1 | **Yes (Explicit)** | Resets Lease Timer. |
| `ctx.deposit(lock, buf)` | 2 | **Yes (Implicit)** | Writes data to SAB Box. |
| `ctx.done(lock)` | 2 | **N/A** | Finalizes write; notifies Subscriber. |

---

### 5. Final Architecture Overview

This visualizes the separate lanes where:

* **The Green Lane (Tier 1):** Continuous flow of messages and heartbeats.
* **The Blue Lane (Tier 2):** Large cargo deposits protected by the "Lease Tracker" watchdog.

This spec gives the dev team a clear mandate: build a high-performance, resilient message bus where the **Main Thread** is the traffic controller and **Workers** are the heavy-duty haulers.

Great catch. For a library aiming to be a "Future" systems-engine, **Supervision Trees** and **Linking** are what turn a collection of workers into a **Resilient System**.

In Erlang, the philosophy is "Let it crash." In `@nilejs/future`, we implement this by allowing the Supervisor to define exactly what happens when an actor dies—whether it was a `panic()`, a timeout (Lease expiry), or a manual termination.

---

## 1. Supervisor Strategies (The "Restart" Logic)

When you define a `Supervisor`, you can now specify a `strategy`. This determines the fate of the "siblings" when one actor in the group fails.

### ADR 005: Supervision Strategies

* **One-For-One (Default):** If an actor dies, only that actor is restarted. Good for independent tasks (e.g., HTTP request handlers).
* **One-For-All:** If one actor dies, the Supervisor kills and restarts **all** actors in that group. Use this when actors are tightly coupled (e.g., a pipeline where Actor B depends on Actor A's state).
* **Rest-For-One:** If an actor dies, only the actors started **after** it in the sequence are restarted.

```typescript
const billingGroup = supervisor.createGroup({
  strategy: 'one-for-all', // If the 'Tax' actor dies, restart 'Invoice' and 'Email' too
  retry: {
    max: 3,
    backoff: 'exponential'
  }
});

```

---

## 2. Linking Actors (Bi-directional Failure)

Linking allows two actors to be "entwined." If Actor A dies, a "Signal" is automatically sent to Actor B.

### The `ctx.link()` Semantic

A link is bi-directional. It is used to ensure that if a "Manager" actor dies, its "Worker" actors don't stay alive as zombies.

```typescript
// Inside Actor A
const workerActor = await ctx.spawn(workerCallback);
ctx.link(workerActor); 

// If Actor A crashes, the Supervisor automatically kills 'workerActor'.

```

### Monitoring (Unidirectional)

If you don't want a "Suicide Pact" (Linking), you use **Monitoring**. Actor A monitors Actor B. If B dies, A receives a Tier 1 message so it can handle the cleanup gracefully.

```typescript
// Inside Actor A
ctx.monitor(actorB);

self.subscribe((msg) => {
  match(msg, {
    ActorDown: (info) => println(`Actor ${info.id} died of ${info.reason}`),
    _: () => {}
  });
});

```

---

## 3. Revised Spec: The Supervisor Watchdog

The Supervisor now manages three distinct "Health" metrics for every actor:

1. **The Lease (Memory):** Is the actor holding a Tier 2 box too long? (Heartbeat check).
2. **The Exit (Lifecycle):** Did the process/thread finish with a non-zero exit code?
3. **The Link (Topology):** Does this death require me to kill others?

### Internal State Management (Bitmask)

To keep this fast, the Supervisor tracks links in a **Dependency Matrix**. When an actor ID is flagged as `DOWN`, the Supervisor immediately scans the matrix to find and terminate linked PIDs.

---

## 4. End-User Scenario: A Fault-Tolerant Pipeline

Here is how a dev uses these strategies in `@nilejs/future` to build a reliable system.

```typescript
const pipeline = supervisor.createGroup({ strategy: 'rest-for-one' });

// Stage 1: Data Ingest
const ingest = pipeline.spawn(async (self, msg, ctx) => {
  // ...
});

// Stage 2: Transformer (Depends on Ingest)
const transform = pipeline.spawn(async (self, msg, ctx) => {
  // If 'ingest' dies, 'transform' is automatically restarted 
  // because it's further down the 'rest-for-one' chain.
});

```

---

## 5. Summary Table for Dev Team

| Feature | Logic | Trigger |
| --- | --- | --- |
| **Restart Strategy** | `one-for-one`, `all`, `rest` | Process Crash / Unhandled Exception. |
| **Lease Timeout** | Terminate & Recycle Box | `Lease Tracker` > 5000ms. |
| **Linking** | Bi-directional kill | Linked Actor Exit. |
| **Monitoring** | Tier 1 "Down" Message | Monitored Actor Exit. |

### Final Vision for `@nilejs/future`

By adding these Erlang-inspired semantics, your library isn't just "fast JavaScript"—it’s **"Bulletproof JavaScript."** It gives developers the tools to build systems that can heal themselves.

The **Tier 1 (Control)** handles the "Down" signals and monitor messages, while **Tier 2 (Data)** ensures that even if a whole tree of actors is being restarted, the memory boxes are safely cleared and ready for the new instances.

Does this supervision and linking logic complete the "System" part of the spec for your team?

To pitch this effectively, you have to frame it as the solution to the "JavaScript Bottleneck." Most devs feel the pain of Node/Bun freezing during heavy tasks, but they don't know there's a "Low-Level" way out that still feels like "High-Level" TypeScript.

Here is the pitch strategy for general developers and the specific value proposition for the **Agentic Workflow** (AI) boom.

---

## 1. The General Pitch: "The System-Level Actor Model"

**The Hook:** *Stop treating your workers like simple scripts. Start treating them like a distributed system living on a single machine.*

* **The Problem:** Standard Worker Threads are "islands." Passing large data between them involves expensive serialization (JSON.stringify/parse), which kills the very performance you were trying to gain.
* **The Solution:** `@nilejs/future` introduces **Memory-Mapped Concurrency**. We give you a "Two-Gear" engine:
* **Tier 1 (The Radio):** Fast, async signaling via PubSub.
* **Tier 2 (The Cargo):** Zero-copy data transfer via Atomic-locked Shared Memory.


* **The Vibe:** It’s Erlang’s reliability and Rust’s memory efficiency, wrapped in the `slang-ts` functional patterns you already love.

---

## 2. The Agentic Pitch: "The Backbone for AI Swarms"

In the world of **Agentic Workflows**, the "Agent" is often a long-running, unpredictable process that generates a lot of data (tokens, logs, tool-call results). This is where `@nilejs/future` becomes a "Force Multiplier."

### A. The "Thinking" Sandbox

AI agents are notoriously "crashy" or can get stuck in "thought loops."

* **Pitch:** "Don't let a hallucinating agent freeze your API. Wrap it in a `@nilejs/future` Actor. If the agent enters an infinite loop, our **Lease & Heartbeat** system kills the process and recovers the memory automatically. It's self-healing AI infrastructure."

### B. Streaming High-Volume Context

Agents often need to share large "Context Windows" or "Vector Search Results."

* **Pitch:** "Stop copying 10MB of context between your 'Researcher' agent and your 'Writer' agent. Use **Tier 2 Deposits**. The Researcher writes the context to a shared box, and the Writer reads it instantly with zero-copy overhead. Your swarm moves at the speed of RAM, not the speed of the Event Loop."

### C. Supervision Trees for Multi-Agent Orks

In a workflow where Agent B depends on Agent A's output:

* **Pitch:** "Use **Supervision Strategies**. If your 'Data Extractor' fails, use the `rest-for-one` strategy to automatically reset the 'Analyst' and 'Summarizer' agents. It ensures your agentic pipeline stays in a consistent state without manual error-handling boilerplate."

---

## 3. The "Killer Feature" Table for Pitching

| Feature | For Developers | For Agentic Workflows |
| --- | --- | --- |
| **Zero-Copy Tier 2** | No more `JSON.parse` lag. | Instant sharing of RAG context/large prompts. |
| **Lease & Heartbeat** | No more zombie workers. | Automatic termination of looping/stalled LLM calls. |
| **Resource Manager** | Clean, intent-based DB access. | Secure, proxied tool-calling for agents. |
| **Supervision Trees** | Fault-tolerant micro-services. | Resilient multi-agent "Chain of Thought" pipelines. |

---

## 4. The Elevator Pitch

> "Most JavaScript apps are single-lane roads. `@nilejs/future` turns your Bun/Node runtime into a multi-lane highway. By combining **Atomic Shared Memory** with **Erlang-style Supervision**, we let you build AI agent swarms and high-throughput systems that are physically impossible to build with standard JS. It’s not just a library; it’s a runtime upgrade for the Agentic Era."

---

## 5. One Final Scenario: The "AI Researcher"

**The Setup:**

1. **Supervisor:** Manages the system.
2. **Agent A (Searcher):** Scrapes 50 websites (High I/O).
3. **Agent B (Analyst):** Processes the text (High CPU).

**The Workflow:**
Agent A acquires a **Large Box**, deposits 5MB of raw HTML, and calls `ctx.done()`. Agent B is **Linked** to Agent A; if Agent A hits a CAPTCHA and panics, the Supervisor sees the crash, kills the Analyst (to save money/compute), and restarts the whole "Research Task" from a clean state.

**This is the pitch:** You aren't just running code; you're managing a **Life-Cycle.**

The **Resource Manager** is the "Intent Relay" that allows isolated actors to interact with main-thread resources (Databases, File Systems, APIs) without compromising thread safety or performance. In `@nilejs/future`, it operates entirely on **Tier 1 (Control)** but feels like local, synchronous code inside the worker.

---

## 1. The Core Concept: Proxy & Intent

Because the actor runs in a separate thread, it cannot directly touch a Database Connection or a File Handle residing in the main thread. The Resource Manager bridges this using a **Proxy-Handler** pattern.

* **The Proxy:** A virtual object injected into the actor's `ctx.resources`.
* **The Intent:** When the actor calls a method on the proxy, it generates an "Intent Packet."
* **The Relay:** Tier 1 carries this packet to the Supervisor, which executes the real logic and sends the result back.

---

## 2. Technical Workflow

1. **Definition:** You define resources in the `createSupervisor` config.
2. **Mapping:** The Supervisor maps method names to actual functions.
3. **Call:** The actor calls `await ctx.resources.db.save(user)`.
4. **Serialization:** The Proxy converts arguments into a serializable Tier 1 message.
5. **Execution:** The Main Thread receives the message, runs `db.save(user)`, and captures the return value.
6. **Resolution:** The Supervisor sends the return value back to the worker, resolving the worker's original `await`.

---

## 3. The Specification for Developers

### Resource Definition

Resources must be plain objects containing methods. Each method can be async.

```typescript
const supervisor = createSupervisor({
  resources: {
    // Standard key-value mapping
    postgres: {
      query: async (sql, params) => pool.query(sql, params),
      getStatus: () => pool.state
    }
  }
});

```

### The Worker Proxy

Inside the worker, the `ctx.resources` object is a **Recursive Proxy**. It traps all property access.

```typescript
// Internal Logic (Pseudo-code for the library)
const resourceProxy = new Proxy({}, {
  get: (target, resourceName) => {
    return new Proxy({}, {
      get: (target, methodName) => {
        return (...args) => {
          // Tier 1 Intent Packet
          return self.callSupervisor('RESOURCE_INTENT', {
            resourceName,
            methodName,
            args
          });
        };
      }
    });
  }
});

```

---

## 4. Key Features & Constraints

### A. Non-Blocking but Awaitable

While the actor waits for the resource response, the worker thread is suspended at the `await` point, allowing other micro-tasks in the worker's event loop (like Tier 1 PubSub listeners) to run.

### B. Serialization Limits

Arguments and return values must be **Cloneable** (JSON-compatible or `Uint8Array`). You cannot pass a function or a class instance back and forth through the Resource Manager.

* *Solution:* For large data, the Resource Manager should return a **Tier 2 Address** instead of the actual data.

### C. Resource Scoping

The Supervisor can optionally "filter" resources per actor. You might grant the `AuthActor` access to the `users` resource but not the `billing` resource.

---

## 5. Scenario: Intent-Based Tool Calling

In an agentic workflow, the Resource Manager acts as the "Tool Belt" for the AI.

```typescript
// Main Thread
const supervisor = createSupervisor({
  resources: {
    searchTool: {
      find: async (q) => { 
        println("Agent is searching..."); 
        return await google.search(q); 
      }
    }
  }
});

// Worker (The Agent)
const agent = supervisor.spawn(async (self, msg, ctx) => {
  // Agent decides it needs to search
  const info = await ctx.resources.searchTool.find("Slang-TS release date");
  
  // Use the info to build a response
  self.send(Ok(`Found info: ${info}`));
});

```

## 6. Implementation Checklist for the Team

* [ ] **Proxy Implementation:** Create the nested `Proxy` for `ctx.resources` to capture resource/method calls.
* [ ] **Message ID Tracking:** Ensure every Resource Intent has a unique ID so the worker can match the response back to the correct `await`.
* [ ] **Error Wrapping:** If a resource method throws in the Main Thread, the error must be serialized, sent back, and re-thrown (or returned as an `Err`) inside the worker.
* [ ] **Performance:** For high-frequency resource calls, implement "Batching" where multiple intents are bundled into a single Tier 1 message.
