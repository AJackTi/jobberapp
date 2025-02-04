import { Logger } from 'winston';

import { IEmailLocals, winstonLogger } from '@ajackti/jobber-shared';
import { config } from '@notifications/config';
import { emailTemplates } from '@notifications/helpers';

const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, 'mailTransport', 'debug');

async function sendEmail(template: string, receiveEmail: string, locals: IEmailLocals): Promise<void> {
  try {
    emailTemplates(template, receiveEmail, locals);
    log.info('Email send successfully.');
  } catch (error) {
    log.log('error', 'NotificationService MailTransport sendEmail() method error:', error);
  }
}

export { sendEmail };
