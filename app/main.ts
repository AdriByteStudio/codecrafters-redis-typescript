import * as net from "net";
import * as fs from "fs";
import * as path from "path";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Parse a RESP array of bulk strings (e.g. *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n).
// Returns null if the buffer doesn't yet contain a complete command.
function parseCommand(
   buffer: Buffer
): { command: string; args: Buffer[]; consumed: number } | null {
   if (buffer.length < 1 || buffer[0] !== 0x2a) {
      return null; // not an array
   }

   const lineEnd = buffer.indexOf("\r\n");
   if (lineEnd === -1) {
      return null; // incomplete header
   }

   const count = parseInt(buffer.subarray(1, lineEnd).toString(), 10);
   let offset = lineEnd + 2;
   const elements: Buffer[] = [];

   for (let i = 0; i < count; i++) {
      if (buffer.length < offset + 1 || buffer[offset] !== 0x24) {
         return null; // not a bulk string
      }

      const lenEnd = buffer.indexOf("\r\n", offset);
      if (lenEnd === -1) {
         return null; // incomplete length line
      }

      const len = parseInt(buffer.subarray(offset + 1, lenEnd).toString(), 10);
      offset = lenEnd + 2;

      if (buffer.length < offset + len + 2) {
         return null; // incomplete payload
      }

      elements.push(buffer.subarray(offset, offset + len));
      offset += len + 2;
   }

   return { command: elements[0].toString(), args: elements.slice(1), consumed: offset };
}

// Encode a value as a RESP bulk string (e.g. $3\r\nbar\r\n).
function bulkString(value: Buffer): Buffer {
   return Buffer.concat([Buffer.from(`$${value.length}\r\n`), value, Buffer.from("\r\n")]);
}

// An empty RDB file, used for full resynchronization. Sent as
// $<length>\r\n<binary contents> (no trailing CRLF).
const emptyRdb = Buffer.from(
   "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2",
   "hex"
);

// Sockets of connected replicas (those that completed the handshake). Write
// commands are propagated to all of them over their replication connection.
const replicas = new Set<net.Socket>();

// Track the per-replica ACK offset (the last REPLCONF ACK offset received).
const replicaAckOffset = new Map<net.Socket, number>();

// Master's replication offset: total bytes of all commands propagated to replicas.
let masterReplOffset = 0;

// Encode a command (name + args) as a RESP array and send it to every replica.
// Also advances the master's replication offset by the total payload size.
function propagate(command: string, args: Buffer[]): void {
   if (replicas.size === 0) {
      return;
   }
   const parts: Buffer[] = [Buffer.from(`*${args.length + 1}\r\n`), bulkString(Buffer.from(command))];
   for (const arg of args) {
      parts.push(bulkString(arg));
   }
   const payload = Buffer.concat(parts);
   masterReplOffset += payload.length;
   for (const replica of replicas) {
      replica.write(payload);
   }
}

// Path to the active incremental AOF file (set at startup when AOF is enabled).
let activeAofPath: string | null = null;

// Global map of channel name -> set of subscriber sockets. Used by PUBLISH to
// count (and later deliver to) clients subscribed to a channel.
const channelSubscribers = new Map<string, Set<net.Socket>>();

// Append a write command to the active AOF file in RESP format. When
// appendfsync is "always", flush to disk before returning so the write is
// durable before the client receives a response.
function appendToAof(command: string, args: Buffer[]): void {
   if (activeAofPath === null) {
      return;
   }
   // Redis writes commands in uppercase in the AOF file.
   const cmd = command.toUpperCase();
   const parts: Buffer[] = [Buffer.from(`*${args.length + 1}\r\n`), bulkString(Buffer.from(cmd))];
   for (const arg of args) {
      parts.push(bulkString(arg));
   }
   const payload = Buffer.concat(parts);
   const fd = fs.openSync(activeAofPath, "a");
   fs.writeSync(fd, payload);
   if (configAppendfsync === "always") {
      fs.fsyncSync(fd);
   }
   fs.closeSync(fd);
}

// In-memory key-value store, shared across all connections.
// Each entry holds the value and an optional expiry timestamp (ms since epoch).
const store = new Map<string, { value: Buffer; expiresAt: number | null }>();

// In-memory list store, shared across all connections.
const lists = new Map<string, Buffer[]>();

// In-memory stream store, shared across all connections. Each entry holds its
// ID and the field-value pairs (as alternating Buffers).
interface StreamEntry {
   id: string;
   fields: Buffer[];
}
const streams = new Map<string, StreamEntry[]>();

// Global write version counter and per-key version tracking. Every time any
// key is written, writeVersion increments and that key's entry is updated.
// On WATCH, the connection snapshots current versions. On EXEC, versions are
// compared to detect writes that happened between WATCH and EXEC.
let writeVersion = 0;
const keyVersions = new Map<string, number>();

// Clients blocked on BLPOP, keyed by list name. Each queue is FIFO so the
// client that has been waiting the longest is served first. Each entry also
// holds the timeout timer so it can be cleared when the client is served.
const blockedClients = new Map<
   string,
   { socket: net.Socket; timer: ReturnType<typeof setTimeout> | null }[]
>();

// If a client is waiting on `key` and the list has an element, pop one element
// and send it to the longest-waiting client. Returns true if a client was served.
function serveBlockedClient(key: string): boolean {
   const queue = blockedClients.get(key);
   if (queue === undefined || queue.length === 0) {
      return false;
   }

   const list = lists.get(key);
   if (list === undefined || list.length === 0) {
      return false;
   }

   const entry = queue.shift()!;
   if (queue.length === 0) {
      blockedClients.delete(key);
   }

   if (entry.timer !== null) {
      clearTimeout(entry.timer);
   }

   const element = list.shift()!;
   if (list.length === 0) {
      lists.delete(key);
   }

   entry.socket.write(
      Buffer.concat([Buffer.from("*2\r\n"), bulkString(Buffer.from(key)), bulkString(element)])
   );
   return true;
}

// Returns the value for a key, or undefined if the key is missing or expired.
function getValue(key: string): Buffer | undefined {
   const entry = store.get(key);
   if (entry === undefined) {
      return undefined;
   }
   if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      store.delete(key);
      return undefined;
   }
   return entry.value;
}

// Builds the RESP array body for XREAD: for each key, all entries with an ID
// strictly greater than the corresponding "after" ID. Streams with no new
// entries are omitted, matching XREAD's semantics.
function readStreamsAfter(
   keys: string[],
   ids: string[]
): { streamParts: Buffer[]; matchedStreams: number } {
   const streamParts: Buffer[] = [];
   let matchedStreams = 0;

   for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const [afterMsStr, afterSeqStr] = ids[i].split("-");
      const afterMs = parseInt(afterMsStr, 10);
      const afterSeq = afterSeqStr === undefined ? 0 : parseInt(afterSeqStr, 10);

      const stream = streams.get(key) ?? [];
      const entries = stream.filter((entry) => {
         const [entryMsStr, entrySeqStr] = entry.id.split("-");
         const entryMs = parseInt(entryMsStr, 10);
         const entrySeq = parseInt(entrySeqStr, 10);
         return entryMs > afterMs || (entryMs === afterMs && entrySeq > afterSeq);
      });

      if (entries.length === 0) {
         continue;
      }
      matchedStreams++;

      streamParts.push(Buffer.from("*2\r\n"));
      streamParts.push(bulkString(Buffer.from(key)));
      streamParts.push(Buffer.from(`*${entries.length}\r\n`));
      for (const entry of entries) {
         streamParts.push(Buffer.from("*2\r\n"));
         streamParts.push(bulkString(Buffer.from(entry.id)));
         streamParts.push(Buffer.from(`*${entry.fields.length}\r\n`));
         for (const field of entry.fields) {
            streamParts.push(bulkString(field));
         }
      }
   }

   return { streamParts, matchedStreams };
}

interface XreadWaiter {
   socket: net.Socket;
   keys: string[];
   ids: string[];
   timer: ReturnType<typeof setTimeout> | null;
   served: boolean;
}

// Clients blocked on XREAD BLOCK, keyed by each stream key they're waiting on.
// A waiter may appear under multiple keys if it specified multiple streams.
const blockedXreadClients = new Map<string, XreadWaiter[]>();

// Removes a waiter from every stream key's queue it was registered under.
function removeXreadWaiter(waiter: XreadWaiter): void {
   for (const key of waiter.keys) {
      const queue = blockedXreadClients.get(key);
      if (queue === undefined) {
         continue;
      }
      const index = queue.indexOf(waiter);
      if (index !== -1) {
         queue.splice(index, 1);
      }
      if (queue.length === 0) {
         blockedXreadClients.delete(key);
      }
   }
}

// If any client is blocked on XREAD waiting for `key`, check whether it now has
// new data across all of its requested streams and, if so, unblock it.
function serveBlockedXreadClients(key: string): void {
   const queue = blockedXreadClients.get(key);
   if (queue === undefined) {
      return;
   }

   for (const waiter of [...queue]) {
      if (waiter.served) {
         continue;
      }
      const { streamParts, matchedStreams } = readStreamsAfter(waiter.keys, waiter.ids);
      if (matchedStreams === 0) {
         continue;
      }

      waiter.served = true;
      if (waiter.timer !== null) {
         clearTimeout(waiter.timer);
      }
      removeXreadWaiter(waiter);

      const parts: Buffer[] = [Buffer.from(`*${matchedStreams}\r\n`), ...streamParts];
      waiter.socket.write(Buffer.concat(parts));
   }
}

// Per-connection state needed to execute commands. Shared between the server's
// client connections and the replica's master connection (which processes
// propagated commands silently).
interface ExecContext {
   connection: net.Socket;
   send: (data: string | Buffer) => void;
   inTransaction: boolean;
   queuedCommands: { command: string; args: Buffer[] }[];
   watchedKeys: Map<string, number>;
   responseSink: Buffer[] | null;
   subscriptions: Set<string>;
   subscribed: boolean;
}

// Executes a single command against the shared stores. `ctx.send` is used to
// write the response; for a replica processing propagated commands, send is a
// no-op so nothing is written back to the master.
function executeCommand(ctx: ExecContext, command: string, args: Buffer[]): void {
   const { connection, send } = ctx;

   // In subscribed mode, only a subset of commands is allowed. Reject any
   // other command with an error. (PING is allowed but handled separately.)
   if (ctx.subscribed) {
      const allowed = new Set(["subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit"]);
      if (!allowed.has(command)) {
         send(`-ERR Can't execute '${command}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n`);
         return;
      }
   }

   if (command === "ping") {
      if (ctx.subscribed) {
         // In subscribed mode, PING responds with a RESP array ["pong", ""].
         send(Buffer.concat([Buffer.from("*2\r\n"), bulkString(Buffer.from("pong")), bulkString(Buffer.from(""))]));
      } else {
         send("+PONG\r\n");
      }
   } else if (command === "echo") {
      send(bulkString(args[0]));
   } else if (command === "replconf") {
      // Used during the replication handshake. For now, just acknowledge.
      send("+OK\r\n");
   } else if (command === "psync") {
      // Respond with a full resynchronization using the master's replid,
      // then send the empty RDB file. This connection is now a replica.
      send(`+FULLRESYNC ${masterReplid} 0\r\n`);
      send(Buffer.concat([Buffer.from(`$${emptyRdb.length}\r\n`), emptyRdb]));
      replicas.add(connection);
   } else if (command === "info") {
      // Only the replication section is needed for now.
      const section = args[0]?.toString().toLowerCase();
      if (section === "replication") {
         send(
            bulkString(
               Buffer.from(
                  `role:${role}\nmaster_replid:${masterReplid}\nmaster_repl_offset:${masterReplOffset}`
               )
            )
         );
      } else {
         send(bulkString(Buffer.from("")));
      }
   } else if (command === "multi") {
      ctx.inTransaction = true;
      send("+OK\r\n");
   } else if (command === "exec") {
      if (!ctx.inTransaction) {
         send("-ERR EXEC without MULTI\r\n");
      } else {
         ctx.inTransaction = false;
         // Check if any watched key was modified since WATCH.
         let aborted = false;
         if (ctx.watchedKeys.size > 0) {
            for (const [key, versionAtWatch] of ctx.watchedKeys) {
               const currentVersion = keyVersions.get(key) ?? 0;
               if (currentVersion !== versionAtWatch) {
                  aborted = true;
                  break;
               }
            }
         }
         // Clear watch state regardless of outcome.
         ctx.watchedKeys.clear();
         if (aborted) {
            ctx.queuedCommands.length = 0;
            send("*-1\r\n");
         } else {
            const queued = ctx.queuedCommands.splice(0, ctx.queuedCommands.length);
            const results: Buffer[] = [];
            for (const queuedCommand of queued) {
               const previousSink = ctx.responseSink;
               ctx.responseSink = [];
               executeCommand(ctx, queuedCommand.command, queuedCommand.args);
               results.push(...ctx.responseSink);
               ctx.responseSink = previousSink;
            }
            send(Buffer.concat([Buffer.from(`*${queued.length}\r\n`), ...results]));
         }
      }
   } else if (command === "discard") {
      if (!ctx.inTransaction) {
         send("-ERR DISCARD without MULTI\r\n");
      } else {
         ctx.inTransaction = false;
         ctx.queuedCommands.length = 0;
         ctx.watchedKeys.clear();
         send("+OK\r\n");
      }
   } else if (command === "set") {
      const key = args[0].toString();
      const value = args[1];

      // Parse optional expiry options: EX <seconds> or PX <milliseconds>.
      let expiresAt: number | null = null;
      for (let i = 2; i + 1 < args.length; i += 2) {
         const option = args[i].toString().toLowerCase();
         const amount = parseInt(args[i + 1].toString(), 10);
         if (option === "ex") {
            expiresAt = Date.now() + amount * 1000;
         } else if (option === "px") {
            expiresAt = Date.now() + amount;
         }
      }

      store.set(key, { value, expiresAt });
      writeVersion++;
      keyVersions.set(key, writeVersion);
      propagate("set", args);
      appendToAof("set", args);
      send("+OK\r\n");
   } else if (command === "get") {
      const value = getValue(args[0].toString());
      send(value === undefined ? "$-1\r\n" : bulkString(value));
   } else if (command === "incr") {
      const key = args[0].toString();
      const value = getValue(key);
      const current = value === undefined ? 0 : parseInt(value.toString(), 10);

      if (value !== undefined && Number.isNaN(current)) {
         send("-ERR value is not an integer or out of range\r\n");
      } else {
         const incremented = current + 1;
         store.set(key, { value: Buffer.from(incremented.toString()), expiresAt: null });
         writeVersion++;
         keyVersions.set(key, writeVersion);
         propagate("incr", args);
         appendToAof("incr", args);
         send(`:${incremented}\r\n`);
      }
   } else if (command === "type") {
      const key = args[0].toString();
      if (getValue(key) !== undefined) {
         send("+string\r\n");
      } else if (lists.has(key)) {
         send("+list\r\n");
      } else if (streams.has(key)) {
         send("+stream\r\n");
      } else {
         send("+none\r\n");
      }
   } else if (command === "xadd") {
      const key = args[0].toString();
      const rawId = args[1].toString();
      const fields = args.slice(2);

      const stream = streams.get(key);
      const last = stream?.[stream.length - 1];

      // Resolve a fully auto-generated ID ("*") or a partially
      // auto-generated sequence number (e.g. "5-*") into concrete numbers.
      let ms: number;
      let seq: number;
      if (rawId === "*") {
         ms = Date.now();
         if (last !== undefined) {
            const [lastMsStr, lastSeqStr] = last.id.split("-");
            const lastMs = parseInt(lastMsStr, 10);
            const lastSeq = parseInt(lastSeqStr, 10);
            seq = lastMs === ms ? lastSeq + 1 : 0;
         } else {
            seq = 0;
         }
      } else {
         const [msStr, seqStr] = rawId.split("-");
         ms = parseInt(msStr, 10);
         if (seqStr === "*") {
            if (last !== undefined) {
               const [lastMsStr, lastSeqStr] = last.id.split("-");
               const lastMs = parseInt(lastMsStr, 10);
               const lastSeq = parseInt(lastSeqStr, 10);
               seq = lastMs === ms ? lastSeq + 1 : 0;
            } else {
               seq = ms === 0 ? 1 : 0;
            }
         } else {
            seq = parseInt(seqStr, 10);
         }
      }
      const id = `${ms}-${seq}`;

      // 0-0 is always invalid, regardless of stream state.
      if (ms === 0 && seq === 0) {
         send("-ERR The ID specified in XADD must be greater than 0-0\r\n");
      } else if (last === undefined) {
         // Stream is empty (0-0 already rejected above).
         const newStream = stream ?? [];
         newStream.push({ id, fields });
         streams.set(key, newStream);
         writeVersion++;
         keyVersions.set(key, writeVersion);
         propagate("xadd", args);
         appendToAof("xadd", args);
         send(bulkString(Buffer.from(id)));
         serveBlockedXreadClients(key);
      } else {
         const [lastMsStr, lastSeqStr] = last.id.split("-");
         const lastMs = parseInt(lastMsStr, 10);
         const lastSeq = parseInt(lastSeqStr, 10);

         if (ms > lastMs || (ms === lastMs && seq > lastSeq)) {
            const newStream = stream ?? [];
            newStream.push({ id, fields });
            streams.set(key, newStream);
            writeVersion++;
            keyVersions.set(key, writeVersion);
            propagate("xadd", args);
            appendToAof("xadd", args);
            send(bulkString(Buffer.from(id)));
            serveBlockedXreadClients(key);
         } else {
            send(
               "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
            );
         }
      }
   } else if (command === "xrange") {
      const key = args[0].toString();
      const startArg = args[1].toString();
      const endArg = args[2].toString();

      const parseRangeId = (arg: string, defaultSeq: number): [number, number] => {
         const [msPart, seqPart] = arg.split("-");
         const ms = parseInt(msPart, 10);
         const seq = seqPart === undefined ? defaultSeq : parseInt(seqPart, 10);
         return [ms, seq];
      };

      const [startMs, startSeq] = startArg === "-" ? [0, 0] : parseRangeId(startArg, 0);
      const [endMs, endSeq] =
         endArg === "+"
            ? [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]
            : parseRangeId(endArg, Number.MAX_SAFE_INTEGER);

      const stream = streams.get(key) ?? [];
      const entries = stream.filter((entry) => {
         const [entryMsStr, entrySeqStr] = entry.id.split("-");
         const entryMs = parseInt(entryMsStr, 10);
         const entrySeq = parseInt(entrySeqStr, 10);

         const afterStart = entryMs > startMs || (entryMs === startMs && entrySeq >= startSeq);
         const beforeEnd = entryMs < endMs || (entryMs === endMs && entrySeq <= endSeq);
         return afterStart && beforeEnd;
      });

      const parts: Buffer[] = [Buffer.from(`*${entries.length}\r\n`)];
      for (const entry of entries) {
         parts.push(Buffer.from("*2\r\n"));
         parts.push(bulkString(Buffer.from(entry.id)));
         parts.push(Buffer.from(`*${entry.fields.length}\r\n`));
         for (const field of entry.fields) {
            parts.push(bulkString(field));
         }
      }
      send(Buffer.concat(parts));
   } else if (command === "xread") {
      // Syntax: XREAD [BLOCK <ms>] STREAMS <key1> [key2 ...] <id1> [id2 ...]
      let blockTimeout: number | null = null;
      if (args[0].toString().toLowerCase() === "block") {
         blockTimeout = parseInt(args[1].toString(), 10);
      }

      const streamsIndex = args.findIndex((arg) => arg.toString().toLowerCase() === "streams");
      const keysAndIds = args.slice(streamsIndex + 1);
      const numStreams = keysAndIds.length / 2;
      const keys = keysAndIds.slice(0, numStreams).map((k) => k.toString());
      // "$" means "the last ID currently in the stream", resolved once
      // up front so later blocking checks compare against a fixed point.
      const ids = keysAndIds.slice(numStreams).map((id, i) => {
         const raw = id.toString();
         if (raw !== "$") {
            return raw;
         }
         const stream = streams.get(keys[i]);
         return stream === undefined || stream.length === 0 ? "0-0" : stream[stream.length - 1].id;
      });

      const { streamParts, matchedStreams } = readStreamsAfter(keys, ids);

      if (matchedStreams > 0 || blockTimeout === null) {
         const parts: Buffer[] = [Buffer.from(`*${matchedStreams}\r\n`), ...streamParts];
         send(Buffer.concat(parts));
      } else {
         // No data yet: block this client until a new entry is added to
         // one of the requested streams, or the timeout expires.
         const waiter: XreadWaiter = { socket: connection, keys, ids, timer: null, served: false };
         if (blockTimeout > 0) {
            waiter.timer = setTimeout(() => {
               waiter.served = true;
               removeXreadWaiter(waiter);
               connection.write("*-1\r\n");
            }, blockTimeout);
         }
         for (const key of keys) {
            const queue = blockedXreadClients.get(key) ?? [];
            queue.push(waiter);
            blockedXreadClients.set(key, queue);
         }
      }
   } else if (command === "rpush") {
      const key = args[0].toString();
      const list = lists.get(key) ?? [];
      for (const element of args.slice(1)) {
         list.push(element);
      }
      lists.set(key, list);
      writeVersion++;
      keyVersions.set(key, writeVersion);
      propagate("rpush", args);
      appendToAof("rpush", args);
      send(`:${list.length}\r\n`);
      while (serveBlockedClient(key)) {
         // Keep serving waiting clients while elements remain.
      }
   } else if (command === "lpush") {
      const key = args[0].toString();
      const list = lists.get(key) ?? [];
      for (const element of args.slice(1)) {
         list.unshift(element);
      }
      lists.set(key, list);
      writeVersion++;
      keyVersions.set(key, writeVersion);
      propagate("lpush", args);
      appendToAof("lpush", args);
      send(`:${list.length}\r\n`);
      while (serveBlockedClient(key)) {
         // Keep serving waiting clients while elements remain.
      }
   } else if (command === "blpop") {
      const key = args[0].toString();
      const list = lists.get(key);

      if (list !== undefined && list.length > 0) {
         const element = list.shift()!;
         if (list.length === 0) {
            lists.delete(key);
         }
         writeVersion++;
         keyVersions.set(key, writeVersion);
         propagate("blpop", args);
         appendToAof("blpop", args);
         send(
            Buffer.concat([Buffer.from("*2\r\n"), bulkString(Buffer.from(key)), bulkString(element)])
         );
      } else {
         // List is empty: block this client until an element is pushed
         // or the timeout (in seconds) expires.
         const timeout = parseFloat(args[1].toString());
         let timer: ReturnType<typeof setTimeout> | null = null;
         if (timeout > 0) {
            timer = setTimeout(() => {
               const queue = blockedClients.get(key);
               if (queue !== undefined) {
                  const index = queue.findIndex((e) => e.socket === connection);
                  if (index !== -1) {
                     queue.splice(index, 1);
                     if (queue.length === 0) {
                        blockedClients.delete(key);
                     }
                  }
               }
               connection.write("*-1\r\n");
            }, timeout * 1000);
         }
         const queue = blockedClients.get(key) ?? [];
         queue.push({ socket: connection, timer });
         blockedClients.set(key, queue);
      }
   } else if (command === "llen") {
      const list = lists.get(args[0].toString());
      send(`:${list?.length ?? 0}\r\n`);
   } else if (command === "lpop") {
      const key = args[0].toString();
      const list = lists.get(key);
      if (list === undefined || list.length === 0) {
         send("$-1\r\n");
      } else if (args.length > 1) {
         const count = parseInt(args[1].toString(), 10);
         const removed = list.splice(0, count);
         if (list.length === 0) {
            lists.delete(key);
         }
         writeVersion++;
         keyVersions.set(key, writeVersion);
         propagate("lpop", args);
         appendToAof("lpop", args);
         const parts: Buffer[] = [Buffer.from(`*${removed.length}\r\n`)];
         for (const element of removed) {
            parts.push(bulkString(element));
         }
         send(Buffer.concat(parts));
      } else {
         const element = list.shift()!;
         if (list.length === 0) {
            lists.delete(key);
         }
         writeVersion++;
         keyVersions.set(key, writeVersion);
         propagate("lpop", args);
         appendToAof("lpop", args);
         send(bulkString(element));
      }
   } else if (command === "lrange") {
      const key = args[0].toString();
      let start = parseInt(args[1].toString(), 10);
      let stop = parseInt(args[2].toString(), 10);
      const list = lists.get(key);

      if (list !== undefined) {
         // Normalize negative indexes: -1 is the last element, etc.
         if (start < 0) {
            start = Math.max(list.length + start, 0);
         }
         if (stop < 0) {
            stop = Math.max(list.length + stop, 0);
         }
      }

      if (list === undefined || start >= list.length || start > stop) {
         send("*0\r\n");
      } else {
         const end = Math.min(stop, list.length - 1);
         const parts: Buffer[] = [Buffer.from(`*${end - start + 1}\r\n`)];
         for (let i = start; i <= end; i++) {
            parts.push(bulkString(list[i]));
         }
         send(Buffer.concat(parts));
      }
   } else if (command === "watch") {
      if (ctx.inTransaction) {
         send("-ERR WATCH inside MULTI is not allowed\r\n");
      } else {
         for (const arg of args) {
            const key = arg.toString();
            ctx.watchedKeys.set(key, keyVersions.get(key) ?? 0);
         }
         send("+OK\r\n");
      }
   } else if (command === "unwatch") {
      ctx.watchedKeys.clear();
      send("+OK\r\n");
   } else if (command === "keys") {
      const pattern = args[0]?.toString();
      if (pattern === "*") {
         const keys = [...store.keys()];
         if (keys.length === 0) {
            send("*0\r\n");
         } else {
            const parts = keys.map((k) => bulkString(Buffer.from(k)));
            send(Buffer.concat([Buffer.from(`*${keys.length}\r\n`), ...parts]));
         }
      } else {
         send("*0\r\n");
      }
   } else if (command === "config") {
      const sub = args[0]?.toString().toLowerCase();
      if (sub === "get") {
         const param = args[1]?.toString().toLowerCase();
         const configMap: Record<string, string> = {
            dir: configDir,
            dbfilename: configDbfilename,
            appendonly: configAppendonly,
            appenddirname: configAppenddirname,
            appendfilename: configAppendfilename,
            appendfsync: configAppendfsync,
         };
         const value = configMap[param] ?? "";
         send(Buffer.concat([Buffer.from("*2\r\n"), bulkString(Buffer.from(param ?? "")), bulkString(Buffer.from(value))]));
      } else {
         send("*0\r\n");
      }
   } else if (command === "wait") {
      const numReplicas = parseInt(args[0].toString(), 10);
      const timeout = parseInt(args[1].toString(), 10);

      // If no replicas are connected, return 0 immediately.
      if (replicas.size === 0) {
         send(":0\r\n");
         return;
      }

      // If no write commands have been propagated since the last WAIT (i.e.
      // masterReplOffset is 0), all replicas are trivially in sync.
      if (masterReplOffset === 0) {
         send(`:${replicas.size}\r\n`);
         return;
      }

      // Send REPLCONF GETACK * to every connected replica.
      const getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
      for (const replica of replicas) {
         replica.write(getack);
      }

      // Count how many replicas have acknowledged up to the current offset.
      const countAcked = (): number => {
         let n = 0;
         for (const replica of replicas) {
            const acked = replicaAckOffset.get(replica) ?? 0;
            if (acked >= masterReplOffset) {
               n++;
            }
         }
         return n;
      };

      // If timeout is 0, return immediately.
      if (timeout === 0) {
         send(`:${countAcked()}\r\n`);
         return;
      }

      // Poll every 10ms until we have enough acks or the timeout expires.
      const start = Date.now();
      const timer = setInterval(() => {
         const acked = countAcked();
         if (acked >= numReplicas || Date.now() - start >= timeout) {
            clearInterval(timer);
            send(`:${acked}\r\n`);
         }
      }, 10);
   } else if (command === "subscribe") {
      const channel = args[0].toString();
      ctx.subscriptions.add(channel);
      ctx.subscribed = true;
      // Register this connection as a subscriber of the channel globally.
      const subs = channelSubscribers.get(channel) ?? new Set<net.Socket>();
      subs.add(connection);
      channelSubscribers.set(channel, subs);
      const count = ctx.subscriptions.size;
      const resp = Buffer.concat([
         Buffer.from(`*3\r\n`),
         bulkString(Buffer.from("subscribe")),
         bulkString(Buffer.from(channel)),
         Buffer.from(`:${count}\r\n`),
      ]);
      send(resp);
   } else if (command === "publish") {
      const channel = args[0].toString();
      const message = args[1];
      const subs = channelSubscribers.get(channel);
      // Deliver the message to every subscribed client.
      if (subs) {
         const payload = Buffer.concat([
            Buffer.from("*3\r\n"),
            bulkString(Buffer.from("message")),
            bulkString(Buffer.from(channel)),
            bulkString(message),
         ]);
         for (const sub of subs) {
            sub.write(payload);
         }
      }
      send(`:${subs?.size ?? 0}\r\n`);
   }
}

const server: net.Server = net.createServer((connection: net.Socket) => {
   let buffer = Buffer.alloc(0);

   // Per-connection execution context shared with the standalone
   // executeCommand function.
   const ctx: ExecContext = {
      connection,
      send: (data: string | Buffer): void => {
         const buf = typeof data === "string" ? Buffer.from(data) : data;
         if (ctx.responseSink !== null) {
            ctx.responseSink.push(buf);
         } else {
            connection.write(buf);
         }
      },
      inTransaction: false,
      queuedCommands: [],
      watchedKeys: new Map<string, number>(),
      responseSink: null,
      subscriptions: new Set(),
      subscribed: false,
   };

   connection.on("data", (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      while (true) {
         const parsed = parseCommand(buffer);
         if (parsed === null) {
            break; // wait for more data
         }

         buffer = buffer.subarray(parsed.consumed);
         const command = parsed.command.toLowerCase();

         // A replica may send REPLCONF ACK <offset> at any time after the
         // handshake. Record the offset and don't process it as a command.
         if (
            command === "replconf" &&
            parsed.args[0]?.toString().toLowerCase() === "ack" &&
            replicas.has(connection)
         ) {
            const offset = parseInt(parsed.args[1]?.toString() ?? "0", 10);
            replicaAckOffset.set(connection, offset);
            continue;
         }

         if (
            ctx.inTransaction &&
            command !== "multi" &&
            command !== "exec" &&
            command !== "discard" &&
            command !== "watch" &&
            command !== "unwatch"
         ) {
            ctx.queuedCommands.push({ command, args: parsed.args });
            connection.write("+QUEUED\r\n");
            continue;
         }

         executeCommand(ctx, command, parsed.args);
      }
   });

   connection.on("close", () => {
      // Remove this connection from the replica set if it was one.
      replicas.delete(connection);
      replicaAckOffset.delete(connection);

      // Remove this connection from any BLPOP wait queues.
      for (const [key, queue] of blockedClients) {
         const index = queue.findIndex((e) => e.socket === connection);
         if (index !== -1) {
            if (queue[index].timer !== null) {
               clearTimeout(queue[index].timer);
            }
            queue.splice(index, 1);
            if (queue.length === 0) {
               blockedClients.delete(key);
            }
         }
      }
   });
});

// Parse the --port flag (defaults to 6379).
const portArgIndex = process.argv.indexOf("--port");
const port = portArgIndex !== -1 ? parseInt(process.argv[portArgIndex + 1], 10) : 6379;

// Determine the server role: master by default, slave if --replicaof is given.
const role = process.argv.includes("--replicaof") ? "slave" : "master";

// RDB configuration parameters.
const dirArgIndex = process.argv.indexOf("--dir");
const configDir = dirArgIndex !== -1 ? process.argv[dirArgIndex + 1] : process.cwd();
const dbfilenameArgIndex = process.argv.indexOf("--dbfilename");
const configDbfilename = dbfilenameArgIndex !== -1 ? process.argv[dbfilenameArgIndex + 1] : "dump.rdb";

// AOF configuration parameters (with defaults).
const appendonlyArgIndex = process.argv.indexOf("--appendonly");
const configAppendonly = appendonlyArgIndex !== -1 ? process.argv[appendonlyArgIndex + 1] : "no";
const appenddirnameArgIndex = process.argv.indexOf("--appenddirname");
const configAppenddirname = appenddirnameArgIndex !== -1 ? process.argv[appenddirnameArgIndex + 1] : "appendonlydir";
const appendfilenameArgIndex = process.argv.indexOf("--appendfilename");
const configAppendfilename = appendfilenameArgIndex !== -1 ? process.argv[appendfilenameArgIndex + 1] : "appendonly.aof";
const appendfsyncArgIndex = process.argv.indexOf("--appendfsync");
const configAppendfsync = appendfsyncArgIndex !== -1 ? process.argv[appendfsyncArgIndex + 1] : "everysec";

// Replication ID for the master. The ID is a 40-char pseudo-random string.
const masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

// ---------- RDB parser ----------

// Read a length-encoded value from buf at offset. Returns [value, newOffset].
function readLength(buf: Buffer, offset: number): [number, number] {
   const first = buf[offset];
   const type = (first >> 6) & 0x03;
   if (type === 0) {
      return [first & 0x3f, offset + 1];
   } else if (type === 1) {
      const second = buf[offset + 1];
      return [((first & 0x3f) << 8) | second, offset + 2];
   } else if (type === 2) {
      const val = (buf[offset + 1] << 24) | (buf[offset + 2] << 16) | (buf[offset + 3] << 8) | buf[offset + 4];
      return [val >>> 0, offset + 5];
   } else {
      // 0b11 — special string encoding
      return [first & 0x3f, offset + 1];
   }
}

// Read a string-encoded value (length + raw bytes, or integer encoding).
function readStringEncoded(buf: Buffer, offset: number): [Buffer, number] {
   const first = buf[offset];
   const type = (first >> 6) & 0x03;
   if (type === 0) {
      const len = first & 0x3f;
      return [buf.subarray(offset + 1, offset + 1 + len), offset + 1 + len];
   } else if (type === 1) {
      const second = buf[offset + 1];
      const len = ((first & 0x3f) << 8) | second;
      return [buf.subarray(offset + 2, offset + 2 + len), offset + 2 + len];
   } else if (type === 2) {
      const len = (buf[offset + 1] << 24) | (buf[offset + 2] << 16) | (buf[offset + 3] << 8) | buf[offset + 4];
      return [buf.subarray(offset + 5, offset + 5 + (len >>> 0)), offset + 5 + (len >>> 0)];
   } else {
      // Special encodings (0xC0..0xC3)
      const specialType = first & 0x3f;
      if (specialType === 0) {
         // 8-bit integer
         return [Buffer.from(buf[offset + 1].toString()), offset + 2];
      } else if (specialType === 1) {
         // 16-bit integer, little-endian
         const val = buf[offset + 1] | (buf[offset + 2] << 8);
         return [Buffer.from(val.toString()), offset + 3];
      } else if (specialType === 2) {
         // 32-bit integer, little-endian
         const val = buf[offset + 1] | (buf[offset + 2] << 8) | (buf[offset + 3] << 16) | (buf[offset + 4] << 24);
         return [Buffer.from((val >>> 0).toString()), offset + 5];
      } else {
         // LZF compressed — skip
         throw new Error("LZF compressed strings not supported");
      }
   }
}

// Load key-value pairs from an RDB file into the global store.
function loadRdb(filePath: string): void {
   if (!fs.existsSync(filePath)) {
      return; // treat as empty database
   }
   const buf = fs.readFileSync(filePath);
   let offset = 0;

   // 1. Header: "REDIS0011" (9 bytes)
   offset = 9;

   while (offset < buf.length) {
      const byte = buf[offset];

      // Metadata subsection (0xFA)
      if (byte === 0xFA) {
         offset++;
         // Skip metadata name (string encoded)
         [, offset] = readStringEncoded(buf, offset);
         // Skip metadata value (string encoded)
         [, offset] = readStringEncoded(buf, offset);
         continue;
      }

      // Database subsection (0xFE)
      if (byte === 0xFE) {
         offset++;
         // Skip DB index (size encoded)
         [, offset] = readLength(buf, offset);
         continue;
      }

      // Hash table size info (0xFB)
      if (byte === 0xFB) {
         offset++;
         // Skip hash table size (size encoded)
         [, offset] = readLength(buf, offset);
         // Skip expiry hash table size (size encoded)
         [, offset] = readLength(buf, offset);
         continue;
      }

      // End of file (0xFF)
      if (byte === 0xFF) {
         break;
      }

      // Expire in milliseconds (0xFC)
      let expiresAtMs: number | null = null;
      if (byte === 0xFC) {
         offset++;
         expiresAtMs = Number(buf.readBigUInt64LE(offset));
         offset += 8;
      }

      // Expire in seconds (0xFD)
      let expiresAtSec: number | null = null;
      if (offset < buf.length && buf[offset] === 0xFD) {
         offset++;
         expiresAtSec = buf.readUInt32LE(offset);
         offset += 4;
      }

      // Value type flag
      const valueType = buf[offset];
      offset++;

      // Key (string encoded)
      const [key, off1] = readStringEncoded(buf, offset);
      offset = off1;

      // Value (string encoded for type 0)
      const [value, off2] = readStringEncoded(buf, offset);
      offset = off2;

      const expiresAt = expiresAtMs !== null ? expiresAtMs : expiresAtSec !== null ? expiresAtSec * 1000 : null;
      store.set(key.toString(), { value, expiresAt });
   }
}

// Load the RDB file on startup.
const rdbPath = path.join(configDir, configDbfilename);
loadRdb(rdbPath);

// If AOF persistence is enabled, create the append-only directory and the
// first incremental AOF file at startup, and read the manifest to determine
// the active incremental file to write to. If the manifest already exists
// (e.g. from a previous run), read incremental files and replay their commands
// to restore the database state.
if (configAppendonly === "yes") {
   const appendDir = path.join(configDir, configAppenddirname);
   fs.mkdirSync(appendDir, { recursive: true });
   const manifestPath = path.join(appendDir, `${configAppendfilename}.manifest`);

   if (fs.existsSync(manifestPath)) {
      // ---- Replay phase: read existing AOF files listed in the manifest ----
      const manifestContent = fs.readFileSync(manifestPath, "utf8");
      const incrAofPaths: string[] = [];
      for (const line of manifestContent.split("\n")) {
         const tokens = line.trim().split(/\s+/);
         // Format: file <name> seq <n> type i
         if (tokens.length >= 6 && tokens[0] === "file" && tokens[5] === "i") {
            incrAofPaths.push(path.join(appendDir, tokens[1]));
         }
      }

      // Replay commands from each incremental AOF file in sequence order.
      // We reuse the existing RESP parser (parseCommand) which expects a
      // complete RESP array in the buffer. Each AOF file may contain
      // multiple commands concatenated together, so we iterate through
      // the buffer consuming one command at a time.
      const replayCtx: ExecContext = {
         connection: null as unknown as net.Socket,
         send: () => {},
         inTransaction: false,
         queuedCommands: [],
         watchedKeys: new Map(),
         responseSink: null,
         subscriptions: new Set(),
         subscribed: false,
      };

      for (const aofFilePath of incrAofPaths) {
         if (!fs.existsSync(aofFilePath)) continue;
         const aofData = fs.readFileSync(aofFilePath);
         let offset = 0;
         while (offset < aofData.length) {
            const slice = aofData.subarray(offset);
            const parsed = parseCommand(slice);
            if (parsed === null) break; // incomplete or no more commands
            const cmd = parsed.command.toLowerCase();
            executeCommand(replayCtx, cmd, parsed.args);
            offset += parsed.consumed;
         }
      }

      // ---- Active AOF file setup for new writes ----
      // Use the last incremental file from the manifest as the active AOF.
      if (incrAofPaths.length > 0) {
         activeAofPath = incrAofPaths[incrAofPaths.length - 1];
      } else {
         // No incremental files found; create a default one.
         const defaultIncrPath = path.join(appendDir, `${configAppendfilename}.1.incr.aof`);
         fs.writeFileSync(defaultIncrPath, "");
         activeAofPath = defaultIncrPath;
      }
   } else {
      // ---- First run: create the initial AOF file and manifest ----
      const incrAofPath = path.join(appendDir, `${configAppendfilename}.1.incr.aof`);
      fs.writeFileSync(incrAofPath, "");
      fs.writeFileSync(manifestPath, `file ${configAppendfilename}.1.incr.aof seq 1 type i\n`);
      activeAofPath = incrAofPath;
   }
}

server.listen(port, "127.0.0.1");

// If running as a replica, connect to the master and start the handshake.
if (role === "slave") {
   const replicaofIndex = process.argv.indexOf("--replicaof");
   const [masterHost, masterPort] = process.argv[replicaofIndex + 1].split(" ");

   const masterConnection = net.createConnection({
      host: masterHost,
      port: parseInt(masterPort, 10),
   });

   masterConnection.on("connect", () => {
      // Step 1: send PING as a RESP array.
      masterConnection.write("*1\r\n$4\r\nPING\r\n");
   });

   // Buffer incoming data from the master to parse RESP responses.
   let masterBuffer = Buffer.alloc(0);
   let handshakeStep = 1; // 1 = waiting for PONG, 2 = waiting for first REPLCONF OK, 3 = waiting for second REPLCONF OK, 4 = waiting for FULLRESYNC + RDB

   // Once the handshake completes, the replica processes propagated commands
   // from the master. It must apply them to its own state without sending any
   // response back to the master.
   const replicaCtx: ExecContext = {
      connection: masterConnection,
      send: () => {
         // No-op: the replica never replies to the master.
      },
      inTransaction: false,
      queuedCommands: [],
      watchedKeys: new Map<string, number>(),
      responseSink: null,
      subscriptions: new Set(),
      subscribed: false,
   };

   // State for reading the RDB file after FULLRESYNC.
   let rdbRemaining: number | null = null; // bytes of RDB still to consume
   let rdbLength: number | null = null; // declared RDB length
   let fullresyncReceived = false; // true once we've consumed the +FULLRESYNC line

   // Running byte offset of all commands processed from the master. Used in
   // REPLCONF ACK responses so the master knows how far along the replica is.
   let replicaOffset = 0;

   masterConnection.on("data", (data: Buffer) => {
      masterBuffer = Buffer.concat([masterBuffer, data]);

      if (handshakeStep <= 3) {
         // Handshake: wait for a complete simple string response (+...\r\n).
         const lineEnd = masterBuffer.indexOf("\r\n");
         if (lineEnd === -1) {
            return;
         }
         const response = masterBuffer.subarray(0, lineEnd).toString();
         masterBuffer = masterBuffer.subarray(lineEnd + 2);

         if (handshakeStep === 1 && response === "+PONG") {
            // Step 2a: REPLCONF listening-port <PORT>
            const portStr = port.toString();
            masterConnection.write(
               `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${portStr.length}\r\n${portStr}\r\n`
            );
            handshakeStep = 2;
         } else if (handshakeStep === 2 && response === "+OK") {
            // Step 2b: REPLCONF capa psync2
            masterConnection.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
            handshakeStep = 3;
         } else if (handshakeStep === 3 && response === "+OK") {
            // Step 3: PSYNC ? -1
            masterConnection.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
            handshakeStep = 4;
         }
         return;
      }

      // Handshake complete. The master now sends:
      //   +FULLRESYNC <replid> <offset>\r\n
      //   $<length>\r\n<binary RDB contents>
      // followed by propagated commands (RESP arrays).

      // Read the FULLRESYNC line if we haven't yet.
      if (rdbLength === null) {
         if (!fullresyncReceived) {
            const lineEnd = masterBuffer.indexOf("\r\n");
            if (lineEnd === -1) {
               return;
            }
            const line = masterBuffer.subarray(0, lineEnd).toString();
            masterBuffer = masterBuffer.subarray(lineEnd + 2);
            if (!line.startsWith("+FULLRESYNC")) {
               // Unexpected; ignore.
               return;
            }
            fullresyncReceived = true;
         }
         // Next comes the RDB bulk string header: $<length>\r\n
         const dollar = masterBuffer.indexOf("\r\n");
         if (dollar === -1) {
            return;
         }
         const header = masterBuffer.subarray(0, dollar).toString();
         if (!header.startsWith("$")) {
            return;
         }
         rdbLength = parseInt(header.slice(1), 10);
         masterBuffer = masterBuffer.subarray(dollar + 2);
         rdbRemaining = rdbLength;
      }

      // Consume the RDB file bytes.
      if (rdbRemaining !== null && rdbRemaining > 0) {
         const toConsume = Math.min(rdbRemaining, masterBuffer.length);
         masterBuffer = masterBuffer.subarray(toConsume);
         rdbRemaining -= toConsume;
         if (rdbRemaining > 0) {
            return; // wait for more RDB bytes
         }
      }

      // RDB fully consumed. Process any propagated commands in the buffer.
      // Track the total bytes of every command received from the master so we
      // can report the offset in REPLCONF ACK responses.
      while (true) {
         const parsed = parseCommand(masterBuffer);
         if (parsed === null) {
            break; // wait for more data
         }
         masterBuffer = masterBuffer.subarray(parsed.consumed);
         const command = parsed.command.toLowerCase();

         // The only command the replica responds to is REPLCONF GETACK.
         // Reply with the current offset (which does NOT include this GETACK
         // command), then add this command's bytes to the running total.
         if (command === "replconf" && parsed.args[0]?.toString().toLowerCase() === "getack") {
            const ackResp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${replicaOffset.toString().length}\r\n${replicaOffset}\r\n`;
            masterConnection.write(ackResp);
            replicaOffset += parsed.consumed;
            continue;
         }

         executeCommand(replicaCtx, command, parsed.args);
         replicaOffset += parsed.consumed;
      }
   });
}
