import {
  Channel,
  ConsumeMessage,
} from 'amqplib';
import { Logger } from 'winston';

import { winstonLogger } from '@ajackti/jobber-shared';
import { config } from '@notifications/config';

import { createConnection } from './connection';

const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, `emailConsumer`, 'debug');

async function consumeAuthEmailMessages(channel: Channel): Promise<void> {
  try {
    if (!channel) {
      channel = (await createConnection()) as Channel;
    }
    const exchangeName = 'jobber-email-notification';
    const routingKey = 'auth-email';
    const queueName = 'auth-email-queue';

    await channel.assertExchange(exchangeName, 'direct');
    const jobberQueue = await channel.assertQueue(queueName, { durable: true, autoDelete: false });
    await channel.bindQueue(jobberQueue.queue, exchangeName, routingKey);

    channel.consume(jobberQueue.queue, async (msg: ConsumeMessage | null) => {
      if (msg) {
        try {
          const content = msg.content.toString();
          if (isValidJSON(content)) {
            const parsedMessage = JSON.parse(content);
            console.log('parsedMessage:', parsedMessage);
            // send emails
            // acknowledge
            channel.ack(msg!);
          }
        } catch (parseError) {
          console.error('Failed to parse message:', msg.content.toString(), parseError);
          // Optionally, handle the invalid message, such as moving it to a dead-letter queue
        }
      }
    });
  } catch (error) {
    log.log('error', 'NotificationService EmailConsumer consumeAuthEmailMessages() method error:', error);
  }
}

async function consumeOrderEmailMessages(channel: Channel): Promise<void> {
  try {
    if (!channel) {
      channel = (await createConnection()) as Channel;
    }
    const exchangeName = 'jobber-order-notification';
    const routingKey = 'order-email';
    const queueName = 'order-email-queue';

    await channel.assertExchange(exchangeName, 'direct');
    const jobberQueue = await channel.assertQueue(queueName, { durable: true, autoDelete: false });
    await channel.bindQueue(jobberQueue.queue, exchangeName, routingKey);

    channel.consume(jobberQueue.queue, async (msg: ConsumeMessage | null) => {
      if (msg) {
        try {
          const content = msg.content.toString();
          if (isValidJSON(content)) {
            const parsedMessage = JSON.parse(content);
            console.log('parsedMessage:', parsedMessage);
            // send emails
            // acknowledge
            channel.ack(msg!);
          }
        } catch (parseError) {
          console.error('Failed to parse message:', msg.content.toString(), parseError);
          // Optionally, handle the invalid message, such as moving it to a dead-letter queue
        }
      }
    });
  } catch (error) {
    log.log('error', 'NotificationService EmailConsumer consumeOrderEmailMessages() method error:', error);
  }
}

function isValidJSON(message: string) {
  try {
    JSON.parse(message);
    return true;
  } catch (e) {
    return false;
  }
}

export { consumeAuthEmailMessages, consumeOrderEmailMessages };
