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

const server: net.Server = net.createServer((connection: net.Socket) => {
   let buffer = Buffer.alloc(0);

   connection.on("data", (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      while (true) {
         const parsed = parseCommand(buffer);
         if (parsed === null) {
            break; // wait for more data
         }

         buffer = buffer.subarray(parsed.consumed);
         const command = parsed.command.toLowerCase();

         if (command === "ping") {
            connection.write("+PONG\r\n");
         } else if (command === "echo") {
            connection.write(bulkString(parsed.args[0]));
         } else if (command === "set") {
            const key = parsed.args[0].toString();
            const value = parsed.args[1];

            // Parse optional expiry options: EX <seconds> or PX <milliseconds>.
            let expiresAt: number | null = null;
            for (let i = 2; i + 1 < parsed.args.length; i += 2) {
               const option = parsed.args[i].toString().toLowerCase();
               const amount = parseInt(parsed.args[i + 1].toString(), 10);
               if (option === "ex") {
                  expiresAt = Date.now() + amount * 1000;
               } else if (option === "px") {
                  expiresAt = Date.now() + amount;
               }
            }

            store.set(key, { value, expiresAt });
            connection.write("+OK\r\n");
         } else if (command === "get") {
            const value = getValue(parsed.args[0].toString());
            connection.write(value === undefined ? "$-1\r\n" : bulkString(value));
         } else if (command === "type") {
            const key = parsed.args[0].toString();
            if (getValue(key) !== undefined) {
               connection.write("+string\r\n");
            } else if (lists.has(key)) {
               connection.write("+list\r\n");
            } else if (streams.has(key)) {
               connection.write("+stream\r\n");
            } else {
               connection.write("+none\r\n");
            }
         } else if (command === "xadd") {
            const key = parsed.args[0].toString();
            const rawId = parsed.args[1].toString();
            const fields = parsed.args.slice(2);

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
               connection.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
            } else if (last === undefined) {
               // Stream is empty (0-0 already rejected above).
               const newStream = stream ?? [];
               newStream.push({ id, fields });
               streams.set(key, newStream);
               connection.write(bulkString(Buffer.from(id)));
            } else {
               const [lastMsStr, lastSeqStr] = last.id.split("-");
               const lastMs = parseInt(lastMsStr, 10);
               const lastSeq = parseInt(lastSeqStr, 10);

               if (ms > lastMs || (ms === lastMs && seq > lastSeq)) {
                  const newStream = stream ?? [];
                  newStream.push({ id, fields });
                  streams.set(key, newStream);
                  connection.write(bulkString(Buffer.from(id)));
               } else {
                  connection.write(
                     "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
                  );
               }
            }
         } else if (command === "xrange") {
            const key = parsed.args[0].toString();
            const startArg = parsed.args[1].toString();
            const endArg = parsed.args[2].toString();

            const parseRangeId = (arg: string, defaultSeq: number): [number, number] => {
               const [msPart, seqPart] = arg.split("-");
               const ms = parseInt(msPart, 10);
               const seq = seqPart === undefined ? defaultSeq : parseInt(seqPart, 10);
               return [ms, seq];
            };

            const [startMs, startSeq] =
               startArg === "-" ? [0, 0] : parseRangeId(startArg, 0);
            const [endMs, endSeq] = parseRangeId(endArg, Number.MAX_SAFE_INTEGER);

            const stream = streams.get(key) ?? [];
            const entries = stream.filter((entry) => {
               const [entryMsStr, entrySeqStr] = entry.id.split("-");
               const entryMs = parseInt(entryMsStr, 10);
               const entrySeq = parseInt(entrySeqStr, 10);

               const afterStart =
                  entryMs > startMs || (entryMs === startMs && entrySeq >= startSeq);
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
            connection.write(Buffer.concat(parts));
         } else if (command === "rpush") {
            const key = parsed.args[0].toString();
            const list = lists.get(key) ?? [];
            for (const element of parsed.args.slice(1)) {
               list.push(element);
            }
            lists.set(key, list);
            connection.write(`:${list.length}\r\n`);
            while (serveBlockedClient(key)) {
               // Keep serving waiting clients while elements remain.
            }
         } else if (command === "lpush") {
            const key = parsed.args[0].toString();
            const list = lists.get(key) ?? [];
            for (const element of parsed.args.slice(1)) {
               list.unshift(element);
            }
            lists.set(key, list);
            connection.write(`:${list.length}\r\n`);
            while (serveBlockedClient(key)) {
               // Keep serving waiting clients while elements remain.
            }
         } else if (command === "blpop") {
            const key = parsed.args[0].toString();
            const list = lists.get(key);

            if (list !== undefined && list.length > 0) {
               const element = list.shift()!;
               if (list.length === 0) {
                  lists.delete(key);
               }
               connection.write(
                  Buffer.concat([
                     Buffer.from("*2\r\n"),
                     bulkString(Buffer.from(key)),
                     bulkString(element),
                  ])
               );
            } else {
               // List is empty: block this client until an element is pushed
               // or the timeout (in seconds) expires.
               const timeout = parseFloat(parsed.args[1].toString());
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
            const list = lists.get(parsed.args[0].toString());
            connection.write(`:${list?.length ?? 0}\r\n`);
         } else if (command === "lpop") {
            const key = parsed.args[0].toString();
            const list = lists.get(key);
            if (list === undefined || list.length === 0) {
               connection.write("$-1\r\n");
            } else if (parsed.args.length > 1) {
               const count = parseInt(parsed.args[1].toString(), 10);
               const removed = list.splice(0, count);
               if (list.length === 0) {
                  lists.delete(key);
               }
               const parts: Buffer[] = [Buffer.from(`*${removed.length}\r\n`)];
               for (const element of removed) {
                  parts.push(bulkString(element));
               }
               connection.write(Buffer.concat(parts));
            } else {
               const element = list.shift()!;
               if (list.length === 0) {
                  lists.delete(key);
               }
               connection.write(bulkString(element));
            }
         } else if (command === "lrange") {
            const key = parsed.args[0].toString();
            let start = parseInt(parsed.args[1].toString(), 10);
            let stop = parseInt(parsed.args[2].toString(), 10);
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
               connection.write("*0\r\n");
            } else {
               const end = Math.min(stop, list.length - 1);
               const parts: Buffer[] = [Buffer.from(`*${end - start + 1}\r\n`)];
               for (let i = start; i <= end; i++) {
                  parts.push(bulkString(list[i]));
               }
               connection.write(Buffer.concat(parts));
            }
         }
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
