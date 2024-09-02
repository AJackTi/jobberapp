import express, { Express } from 'express';
import { Logger } from 'winston';

import { winstonLogger } from '@ajackti/jobber-shared';
import { config } from '@notifications/config';
import { start } from '@notifications/server';

const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, `notificationServer`, 'debug');

function initialize(): void {
  const app: Express = express();
  start(app);
  log.info('Notification Service Initialized');
}

initialize();
