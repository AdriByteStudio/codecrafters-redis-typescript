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
         } else if (command === "rpush") {
            const key = parsed.args[0].toString();
            const list = lists.get(key) ?? [];
            for (const element of parsed.args.slice(1)) {
               list.push(element);
            }
            lists.set(key, list);
            connection.write(`:${list.length}\r\n`);
         }
      }
   });
});

server.listen(6379, "127.0.0.1");
