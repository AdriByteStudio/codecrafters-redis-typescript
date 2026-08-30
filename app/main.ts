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
            const arg = parsed.args[0];
            connection.write(
               Buffer.concat([Buffer.from(`$${arg.length}\r\n`), arg, Buffer.from("\r\n")])
            );
         }
      }
   });
});

server.listen(6379, "127.0.0.1");
