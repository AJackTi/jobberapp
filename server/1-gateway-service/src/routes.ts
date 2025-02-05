import { Application } from 'express';

import { healthRoutes } from '@gateway/routes/health';

export const appRoutes = (app: Application): void => {
  app.use('', healthRoutes.routes());
};
