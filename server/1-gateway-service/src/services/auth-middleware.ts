import {
  NextFunction,
  Request,
  Response
} from 'express';
import JWT from 'jsonwebtoken';

import {
  BadRequestError,
  IAuthPayload,
  NotAuthorizedError
} from '@ajackti/jobber-shared';
import { config } from '@gateway/config';

class AuthMiddleware {
  public verifyUser(req: Request, res: Response, next: NextFunction): void {
    if (!req.session?.jwt) {
      throw new NotAuthorizedError('Token is not available. Please login again.', 'GatewayService verifyUser() method error');
    }

    try {
      const payload: IAuthPayload = JWT.verify(req.session?.jwt, `${config.JWT_TOKEN}`) as IAuthPayload;
      req.currentUser = payload;
    } catch (error) {
      throw new NotAuthorizedError(
        'Token is not available. Please login again.',
        'GatewayService verifyUser() method invalid session error'
      );
    }
    next();
  }

  public checkAuthentication(req: Request, _res: Response, next: NextFunction): void {
    if (req!.currentUser) {
      throw new BadRequestError('Authentication is required to access this route.', 'GatewayService checkAuthentication() method error');
    }
    next();
  }
}

export const authMiddleware: AuthMiddleware = new AuthMiddleware();
