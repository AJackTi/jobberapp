import 'express-async-errors';

import { Channel } from 'amqplib';
import { Application } from 'express';
import http from 'http';
import { Logger } from 'winston';

import { winstonLogger } from '@ajackti/jobber-shared';
import { config } from '@notifications/config';
import { checkConnection } from '@notifications/elasticsearch';
import { createConnection } from '@notifications/queues/connection';
import {
  consumeAuthEmailMessages,
  consumeOrderEmailMessages,
} from '@notifications/queues/email.consumer';
import { healthRoutes } from '@notifications/routes';

const SERVER_PORT = 4001;
const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, `notificationServer`, 'debug');

export function start(app: Application): void {
  startServer(app);
  app.use('', healthRoutes);
  startQueues();
  startElasticSearch();
}

async function startQueues(): Promise<void> {
  const emailChannel: Channel = (await createConnection()) as Channel;
  await consumeAuthEmailMessages(emailChannel);
  await consumeOrderEmailMessages(emailChannel);
  await emailChannel.assertExchange('jobber-email-notification', 'direct');
  const message = JSON.stringify({ name: 'jobber', service: 'auth notification service' });
  emailChannel.publish('jobber-email-notification', 'auth-email', Buffer.from(message));

  await emailChannel.assertExchange('jobber-order-notification', 'direct');
  const message1 = JSON.stringify({ name: 'jobber', service: 'order notification service' });
  emailChannel.publish('jobber-order-notification', 'order-email', Buffer.from(message1));
}

function startElasticSearch(): void {
  checkConnection();
}

function startServer(app: Application): void {
  try {
    const httpServer: http.Server = new http.Server(app);
    log.info(`Worker with process id of ${process.pid} on notification server has started`);
    httpServer.listen(SERVER_PORT, () => {
      log.info(`Notification server listening on port ${SERVER_PORT}`);
    });
  } catch (error) {
    log.log('error', 'NotificationService startServer() method:', error);
  }
}
