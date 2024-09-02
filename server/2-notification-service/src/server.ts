import 'express-async-errors';

import { Application } from 'express';
import http from 'http';
import { Logger } from 'winston';

import { winstonLogger } from '@ajackti/jobber-shared';
import { config } from '@notifications/config';

import { checkConnection } from './elasticsearch';
import { healthRoutes } from './routes';

const SERVER_PORT = 4001;
const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, `notificationServer`, 'debug');

export function start(app: Application): void {
  startServer(app);
  app.use('', healthRoutes);
  startQueues();
  startElasticSearch();
}

async function startQueues(): Promise<void> {}

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
