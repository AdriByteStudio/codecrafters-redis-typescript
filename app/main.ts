import * as net from "net";

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

const server: net.Server = net.createServer((connection: net.Socket) => {
   let buffer = Buffer.alloc(0);
   let inTransaction = false;
   const queuedCommands: { command: string; args: Buffer[] }[] = [];
   const watchedKeys = new Map<string, number>(); // key -> version at WATCH time

   // When set, command replies are collected here (used while replaying a
   // transaction's queued commands) instead of being written to the socket.
   let responseSink: Buffer[] | null = null;
   const send = (data: string | Buffer): void => {
      const buf = typeof data === "string" ? Buffer.from(data) : data;
      if (responseSink !== null) {
         responseSink.push(buf);
      } else {
         connection.write(buf);
      }
   };

   function executeCommand(command: string, args: Buffer[]): void {
      if (command === "ping") {
         send("+PONG\r\n");
      } else if (command === "echo") {
         send(bulkString(args[0]));
      } else if (command === "multi") {
         inTransaction = true;
         send("+OK\r\n");
      } else if (command === "exec") {
         if (!inTransaction) {
            send("-ERR EXEC without MULTI\r\n");
         } else {
            inTransaction = false;
            // Check if any watched key was modified since WATCH.
            let aborted = false;
            if (watchedKeys.size > 0) {
               for (const [key, versionAtWatch] of watchedKeys) {
                  const currentVersion = keyVersions.get(key) ?? 0;
                  if (currentVersion !== versionAtWatch) {
                     aborted = true;
                     break;
                  }
               }
            }
            // Clear watch state regardless of outcome.
            watchedKeys.clear();
            if (aborted) {
               queuedCommands.length = 0;
               send("*-1\r\n");
            } else {
               const queued = queuedCommands.splice(0, queuedCommands.length);
               const results: Buffer[] = [];
               for (const queuedCommand of queued) {
                  const previousSink = responseSink;
                  responseSink = [];
                  executeCommand(queuedCommand.command, queuedCommand.args);
                  results.push(...responseSink);
                  responseSink = previousSink;
               }
               send(Buffer.concat([Buffer.from(`*${queued.length}\r\n`), ...results]));
            }
         }
      } else if (command === "discard") {
         if (!inTransaction) {
            send("-ERR DISCARD without MULTI\r\n");
         } else {
            inTransaction = false;
            queuedCommands.length = 0;
            watchedKeys.clear();
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
         if (inTransaction) {
            send("-ERR WATCH inside MULTI is not allowed\r\n");
         } else {
            for (const arg of args) {
               const key = arg.toString();
               watchedKeys.set(key, keyVersions.get(key) ?? 0);
            }
            send("+OK\r\n");
         }
      }
   }

   connection.on("data", (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      while (true) {
         const parsed = parseCommand(buffer);
         if (parsed === null) {
            break; // wait for more data
         }

         buffer = buffer.subarray(parsed.consumed);
         const command = parsed.command.toLowerCase();

         if (inTransaction && command !== "multi" && command !== "exec" && command !== "discard" && command !== "watch") {
            queuedCommands.push({ command, args: parsed.args });
            connection.write("+QUEUED\r\n");
            continue;
         }

         executeCommand(command, parsed.args);
      }
   });

   connection.on("close", () => {
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

server.listen(6379, "127.0.0.1");
