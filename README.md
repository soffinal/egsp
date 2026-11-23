# Execution Gating Stream Protocol (EGSP) Specification

[![DOI](https://zenodo.org/badge/1469113696.svg)](https://doi.org/10.5281/zenodo.24476810)

**Version:** 0.5.0  
**Status:** Draft Specification

The **Execution Gating Stream Protocol (EGSP)** is a language-agnostic behavioral contract for asynchronous and synchronous stream execution. Unlike reactive streaming protocols that enforce bidirectional negotiation (where a `Consumer` forces a `Consumable` to slow down), EGSP establishes an **Inward-Facing Isolation Barrier** inside the `Consumer` itself.

Under EGSP, any `Consumable`—whether hot or cold—may emit values at its own native velocity. Backpressure isolation is achieved not by choking the `Consumable`, but by strictly gating the **execution context** of the `Consumer`'s processing handler through an explicit single-step credit counter and FIFO buffer.

---

## Conformance & Normative Language

The key words **"MUST"**, **"MUST NOT"**, **"REQUIRED"**, **"SHALL"**, **"SHALL NOT"**, **"SHOULD"**, **"SHOULD NOT"**, **"RECOMMENDED"**, **"MAY"**, and **"OPTIONAL"** in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html) and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174.html) when, and only when, they appear in all capitals.

> _Note on Pseudo-Code: All code blocks in this specification use TypeScript syntax strictly as non-normative, language-agnostic pseudo-code to illustrate contracts and control flow. Conformant implementations MAY use any idiomatic constructs and method names appropriate to their target language._

---

## 1. Scope & Convergence Principle

### 1.1 Scope

This specification defines the minimal behavioral contract between a **`Consumable`** and a **`Consumer`**:

- A `Consumable` providing a factory method (`consume`) that creates and returns a `Consumer` bound to a caller-supplied `Handler`—and, if the `Consumable` is cold/pull-driven, binds itself to the `Consumer`'s demand polling hook (`options.next`).
- A `Consumer` providing three core operations: **ingestion** (`push`), **single-step execution gating** (`next`), and **termination** (`terminate` with `"complete"` or `"abort"`), backed by a credit counter and lossless FIFO buffering.

### 1.2 Non-Goals & Implementation Freedom

This specification intentionally leaves the following to individual implementations and higher-level libraries:

- Fluent composition DSLs (`pipe`), standard operators (`map`, `filter`, `debounce`, `latest`), or domain-specific backpressure policies (dropping, throttling, windowing).
- Exact method naming conventions where target-language idioms differ (e.g., `snake_case` vs. `camelCase`).

### 1.3 The Convergence Principle (Source Agnosticism)

EGSP is **pull/push and hot/cold agnostic** at the `Consumable` boundary, yet **strictly demand-driven** at the `Consumer` execution boundary:

- **Push & Hot `Consumable`s:** Create a `Consumer` and emit values autonomously via `consumer.push(value)` at their own native rate. The `Consumer` absorbs bursts into its FIFO buffer and meters execution one `.next()` credit at a time.
- **Pull & Cold `Consumable`s:** Create a `Consumer` bound to its polling hook (`options.next`), remaining dormant until the `Consumer` signals demand, producing and pushing data only when credit is requested and the buffer is empty.

Because both models converge at the `Consumer` gate, downstream handlers and operators never need to know whether their input is synchronous or asynchronous, hot or cold, or push- or pull-based.

---

## 2. Core Contracts

### 2.1 Entities

- **`Consumable<VALUE>`:** A capability factory that creates a `Consumer<VALUE>` bound to a caller's `Handler`, wiring its own production trigger to the `Consumer`'s polling hook if necessary.
- **`Consumer<VALUE>`:** The execution coordinator and isolation barrier. It owns the credit counter, the FIFO buffer, the handler invocation context, and lifecycle termination.
- **`Handler`:** A callback invoked with `(consumer, value)` whenever execution credit is available and a value is ready.
- **`Polling Hook` (`options.next`/`ConsumerOptions`):** An optional callback registered on a `Consumer` that signals a cold or pull-driven `Consumable` when the buffer is empty and credit is available to yield more data.
- **`Credit`:** A non-negative integer authorization counter inside a `Consumer`. One unit of credit authorizes **at most one** execution of `Handler`.
- **`TerminateReason`:** A terminal intent discriminator, strictly either `"complete"` (graceful wind-down) or `"abort"` (immediate stop).

### 2.2 Abstract Signatures (Non-Normative)

```ts
type TerminateReason = "complete" | "abort";

interface Consumable<VALUE> {
  consume(
    handler: (consumer: Consumer<VALUE>, value: VALUE) => void,
    options?: ConsumerOptions<VALUE>,
  ): Consumer<VALUE>;
}

interface ConsumerOptions<VALUE> {
  next?: (consumer: Consumer<VALUE>) => void;
}

interface Consumer<VALUE> {
  push(value: VALUE): void;
  next(): void;
  terminate(reason: TerminateReason): void;
}
```

---

## 3. Normative Rules

### 3.1 `Consumable` Rules

- **Rule 1.1 (Consumer Factory):** Calling `consumable.consume(handler, options)` MUST create and return a valid `Consumer` instance bound to `handler` (and forwarding any caller-provided `options`).
- **Rule 1.2 (Polling Hook Binding):** If the `Consumable` produces values on demand (cold/pull-driven), it MUST bind its production trigger to the created `Consumer`'s polling hook (`options.next`) so it yields data only when demand is signaled. Hot/push-driven `Consumable`s MAY omit binding `options.next`.
- **Rule 1.3 (Unconstrained Push):** A `Consumable` MAY invoke `consumer.push(value)` at any frequency without waiting for `consumer.next()`.

---

### 3.2 `Consumer` Ingestion (`push`)

- **Rule 2.1 (Armed at Zero):** A newly created `Consumer` MUST initialize with `credit = 0` and an empty buffer. No handler execution MAY occur until `next()` is called.
- **Rule 2.2 (Zero-Allocation Fast Path):** When `push(value)` is called while active, if `credit > 0` and the FIFO buffer is empty, the `Consumer` MUST NOT enqueue `value` into a buffer; it MUST invoke `handler(consumer, value)` inline and decrement `credit` by `1`.
- **Rule 2.3 (Lossless FIFO Buffering):** When `push(value)` is called while active, if `credit == 0` or the FIFO buffer is non-empty, the `Consumer` MUST append `value` to its FIFO buffer without dropping data and return immediately without blocking the caller.
- **Rule 2.4 (Closed Ingestion After Termination):** Once `terminate("complete")` or `terminate("abort")` has been called, any subsequent call to `push(value)` MUST act as a no-op and MUST NOT buffer `value` or invoke `handler`.

---

### 3.3 `Consumer` Credit Metering (`next`)

- **Rule 3.1 (Single-Unit Credit Grant):** Calling `next()` while active or draining MUST increment `credit` by `1`.
- **Rule 3.2 (Credit Accumulation & Polling Signal):** If the FIFO buffer is empty when `next()` is called, `credit` MUST remain stored (`credit > 0`) to authorize the next arriving `push(value)` inline, and if the `Consumer` is active (not draining), it MUST invoke `options.next(consumer)` to signal polling demand to a cold `Consumable`.
- **Rule 3.3 (Single-Step Dequeue):** If the FIFO buffer is non-empty when `next()` is called, the `Consumer` MUST dequeue **at most one** item per unit of credit, invoke `handler(consumer, value)`, and decrement `credit` by `1`. It MUST NOT flush remaining buffered items without corresponding `next()` calls.
- **Rule 3.4 (Re-Entrant Stack Safety):** If `handler` calls `consumer.next()` synchronously before returning, the `Consumer` MUST NOT recurse onto the call stack; the incremented `credit` MUST be processed iteratively once the active `handler` invocation returns.

---

### 3.4 Lifecycle State Machine & Termination (`terminate`)

```text
                        ┌──────────┐
                        │  Active  │
                        └────┬─────┘
                             │
               ┌─────────────┴─────────────┐
               │ terminate("complete")     │ terminate("abort")
               ▼                           ▼
         ┌───────────┐               ┌───────────┐
         │   Drain   │               │   Abort   │
         └─────┬─────┘               └───────────┘
               │                           ▲
         ┌─────┴─────────────────────┐     │
         │                           │     │ terminate("abort")
         ▼ (Buffer Empties)          ▼     │ (Emergency Stop)
   ┌───────────┐               ┌───────────┘
   │ Complete  │               │
   └───────────┘               └────────────────────────────
```

- **Rule 4.1 (Graceful Completion — Empty Buffer):** Calling `terminate("complete")` while active and the FIFO buffer is empty MUST transition immediately to `"complete"` and neutralize the `Consumer` (Rule 4.4).
- **Rule 4.2 (Graceful Drain — Non-Empty Buffer):** Calling `terminate("complete")` while active and the FIFO buffer contains items MUST close ingestion (`push` becomes a no-op) and enter `"drain"` mode, allowing `next()` to continue draining buffered items one by one until empty, at which point the `Consumer` MUST transition to `"complete"` (Rule 4.4).
- **Rule 4.3 (Immediate Abort):** Calling `terminate("abort")` while active or draining MUST immediately discard all buffered items without invoking `handler`, transition to `"abort"`, and neutralize the `Consumer` (Rule 4.4).
- **Rule 4.4 (Post-Termination Neutralization):** Upon reaching `"complete"` or `"abort"`, all subsequent calls to `push`, `next`, and `terminate` MUST act as idempotent no-ops, and references to `handler`, buffer storage, and options SHOULD be released for memory reclamation.
- **Rule 4.5 (Scoped Resource Disposal):** In languages providing deterministic scope-exit constructs (such as RAII destructors, `defer` blocks, context managers, or disposable interfaces), synchronous scope exit SHOULD invoke `terminate("abort")`, and asynchronous scope exit SHOULD await completion of `terminate("complete")`.

---

## 4. Reference Pseudo-Code (Non-Normative)

```ts
function push(value: VALUE): void {
  if (this.status != "active") return;

  if (this.credit > 0 && this.buffer.isEmpty()) {
    this.handler(this, value);
    this.credit -= 1;
  } else {
    this.buffer.enqueue(value);
  }
}

function next(): void {
  if (this.status == "complete" || this.status == "abort") return;

  this.credit += 1;

  if (this.buffer.isEmpty()) {
    if (this.status == "active") {
      this.options.next?.(this);
    }
    return;
  }

  // Re-entrancy guard: outer loop is already draining credit
  if (this.credit > 1) return;

  while (this.credit > 0) {
    if (this.buffer.isEmpty()) {
      if (this.status == "drain") {
        this.terminate("complete");
      } else {
        this.options.next?.(this);
      }
      break;
    }

    const value = this.buffer.dequeue();
    this.handler(this, value);
    this.credit -= 1;
  }

  if (this.buffer.isEmpty() && this.status == "drain") {
    this.terminate("complete");
  }
}
```

---

## 5. Conformance Checklist

An implementation in any programming language is **EGSP-Conformant** if and only if it passes all of the following criteria:

- [ ] **Zero-Credit Arming (Rule 2.1):** Creating a `Consumer` does not execute `handler` until `next()` is called.
- [ ] **Cold Polling Signal (Rules 1.2 & 3.2):** Calling `next()` on an empty buffer invokes `options.next` so cold `Consumable`s bound to it yield on demand.
- [ ] **Zero-Allocation Inline Path (Rule 2.2):** Pushing a value when `credit > 0` and the buffer is empty executes `handler` inline without buffering.
- [ ] **Lossless FIFO Buffering (Rule 2.3):** Pushing values when `credit == 0` or the FIFO buffer is non-empty buffers all values in strict FIFO order without dropping data or blocking the caller.
- [ ] **Single-Step Pacing (Rule 3.3):** Each `next()` call dequeues and processes at most one value, pausing until `next()` is called again.
- [ ] **Re-Entrant Stack Safety (Rule 3.4):** Calling `consumer.next()` synchronously inside `handler` across thousands of buffered items executes iteratively in $O(1)$ stack depth.
- [ ] **Graceful Drain vs. Immediate Abort (Rules 4.1 – 4.4):** `terminate("complete")` drains buffered items at consumer pace before neutralizing, while `terminate("abort")` discards buffered items immediately and makes all methods safe no-ops.
