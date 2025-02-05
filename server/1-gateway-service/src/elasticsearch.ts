import { Logger } from 'winston';

import { winstonLogger } from '@ajackti/jobber-shared';
import { Client } from '@elastic/elasticsearch';
import { ClusterHealthResponse } from '@elastic/elasticsearch/lib/api/types';
import { config } from '@gateway/config';

const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, `gatewayElasticConnection`, 'debug');

class ElasticSearch {
  private elasticSearchClient: Client;

  constructor() {
    console.log('config.ELASTIC_SEARCH_URL', `${config.ELASTIC_SEARCH_URL}`);
    this.elasticSearchClient = new Client({
      node: `${config.ELASTIC_SEARCH_URL}`
    });
  }

  public async checkConnection(): Promise<void> {
    let isConnected = false;
    while (!isConnected) {
      log.info('GatewayService Connecting to ElasticSearch');
      try {
        const health: ClusterHealthResponse = await this.elasticSearchClient.cluster.health();
        log.info('GatewayService ElasticSearch health status - ', health.status);
        isConnected = true;
      } catch (error) {
        log.error('Connection to ElasticSearch failed. Retrying...');
        log.log('error', 'GatewayService checkConnection() method error:', error);
      }
    }
  }
}

export const elasticSearch: ElasticSearch = new ElasticSearch();
