import amqp from "amqplib";

export class RabbitClient {
  constructor(url) {
    this.url = url || "amqp://rabbitmq:5672";
    this.connection = null;
    this.channel = null;
  }

  async connect(retries = 10) {
    while (retries > 0) {
      try {
        console.log("Connecting to RabbitMQ...");
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();

        this.connection.on("error", (err) => {
          console.error("RabbitMQ connection error:", err.message);
        });

        this.connection.on("close", () => {
          console.warn("RabbitMQ connection closed");
        });

        console.log("Connected to RabbitMQ");
        return;
      } catch (err) {
        console.error("RabbitMQ connection failed:", err.message);
        retries--;
        console.log(`Retrying in 3s... (${retries} attempts left)`);
        await new Promise((r) => setTimeout(r, 3000));
      }
    }
    throw new Error("Could not connect to RabbitMQ after multiple attempts");
  }

  async assertExchange(exchange, type = "topic", options = { durable: true }) {
    if (!this.channel) throw new Error("Channel not initialized");
    await this.channel.assertExchange(exchange, type, options);
    console.log(`Exchange "${exchange}" asserted (${type})`);
  }

  publish(exchange, routingKey, message) {
    if (!this.channel) throw new Error("Channel not initialized");

    const payload = Buffer.from(JSON.stringify(message));
    this.channel.publish(exchange, routingKey, payload, { persistent: true });

    console.log(`Sent to exchange "${exchange}" (routingKey: "${routingKey}")`);
  }

  async assertAndBindQueue(queue, exchange, routingKey) {
    if (!this.channel) throw new Error("Channel not initialized");
    await this.channel.assertQueue(queue, { durable: true });
    await this.channel.bindQueue(queue, exchange, routingKey);
    console.log(
      `Bound queue "${queue}" -> exchange "${exchange}" (${routingKey})`
    );
  }

  async consume(queue, handler) {
    if (!this.channel) throw new Error("Channel not initialized");
    this.channel.consume(
      queue,
      async (msg) => {
        if (!msg) return;
        try {
          const content = JSON.parse(msg.content.toString());
          await handler(content, msg, queue);

          this.channel.ack(msg);
        } catch (err) {
          console.error("Message handler error:", err);
          this.channel.nack(msg, false, true);
        }
      },
      { noAck: false }
    );
    console.log(`Listening on queue "${queue}"`);
  }
}
