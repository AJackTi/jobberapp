import express, { Router } from 'express';

import { Health } from '@gateway/controllers/health';

class HealthRoutes {
  private router: Router;

  constructor() {
    this.router = express.Router();
  }

  public routes(): Router {
    this.router.get('/gateway-health', Health.prototype.health);
    return this.router;
  }
}

export const healthRoutes: HealthRoutes = new HealthRoutes();
