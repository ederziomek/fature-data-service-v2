const { Pool } = require('pg');
const ETLConfig = require('../config');
const logger = require('../../utils/logger');

class DataExtractor {
  constructor() {
    this.sourcePool = null;
    this.isConnected = false;
    this.connectionRetries = 0;
    this.maxRetries = ETLConfig.general.maxRetries;
  }

  // Conectar ao banco de origem
  async connect() {
    if (this.isConnected && this.sourcePool) {
      return this.sourcePool;
    }

    try {
      this.sourcePool = new Pool({
        host: ETLConfig.source.host,
        port: ETLConfig.source.port,
        database: ETLConfig.source.database,
        user: ETLConfig.source.username,
        password: ETLConfig.source.password,
        ssl: ETLConfig.source.ssl,
        connectionTimeoutMillis: ETLConfig.source.connectionTimeout,
        min: ETLConfig.source.pool.min,
        max: ETLConfig.source.pool.max,
        idleTimeoutMillis: 30000,
        query_timeout: 60000
      });

      // Testar conexão
      const client = await this.sourcePool.connect();
      await client.query('SELECT 1');
      client.release();

      this.isConnected = true;
      this.connectionRetries = 0;
      
      logger.info('🔗 ETL Extractor: Conectado ao banco de origem', {
        host: ETLConfig.source.host,
        database: ETLConfig.source.database
      });

      return this.sourcePool;

    } catch (error) {
      this.connectionRetries++;
      logger.error('❌ ETL Extractor: Erro ao conectar ao banco de origem', {
        error: error.message,
        attempt: this.connectionRetries,
        maxRetries: this.maxRetries
      });

      if (this.connectionRetries < this.maxRetries) {
        await this.delay(ETLConfig.general.retryDelay);
        return this.connect();
      }

      throw new Error(`Falha ao conectar ao banco de origem após ${this.maxRetries} tentativas`);
    }
  }

  // Extrair dados de uma tabela específica
  async extractTable(tableName, options = {}) {
    const {
      batchSize = ETLConfig.general.batchSize,
      offset = 0,
      incrementalField = null,
      lastSyncTime = null,
      filters = {},
      orderBy = null
    } = options;

    try {
      await this.connect();

      const mapping = ETLConfig.mappings[tableName];
      if (!mapping || !mapping.enabled) {
        throw new Error(`Tabela ${tableName} não configurada ou desabilitada`);
      }

      // Construir query
      const query = this.buildExtractQuery(mapping, {
        batchSize,
        offset,
        incrementalField,
        lastSyncTime,
        filters,
        orderBy
      });

      logger.info(`📤 ETL Extractor: Extraindo dados de ${mapping.sourceTable}`, {
        batchSize,
        offset,
        incrementalField,
        lastSyncTime
      });

      const startTime = Date.now();
      const result = await this.sourcePool.query(query.text, query.values);
      const extractTime = Date.now() - startTime;

      logger.info(`✅ ETL Extractor: Dados extraídos com sucesso`, {
        table: mapping.sourceTable,
        records: result.rows.length,
        extractTime: `${extractTime}ms`
      });

      return {
        success: true,
        data: result.rows,
        metadata: {
          table: mapping.sourceTable,
          recordCount: result.rows.length,
          extractTime,
          batchSize,
          offset,
          hasMore: result.rows.length === batchSize
        }
      };

    } catch (error) {
      logger.error(`❌ ETL Extractor: Erro ao extrair dados de ${tableName}`, {
        error: error.message,
        stack: error.stack
      });

      return {
        success: false,
        error: error.message,
        metadata: {
          table: tableName,
          recordCount: 0,
          extractTime: 0
        }
      };
    }
  }

  // Extrair dados incrementais
  async extractIncremental(tableName, lastSyncTime) {
    const mapping = ETLConfig.mappings[tableName];
    if (!mapping || !mapping.incrementalField) {
      throw new Error(`Tabela ${tableName} não suporta extração incremental`);
    }

    return this.extractTable(tableName, {
      incrementalField: mapping.incrementalField,
      lastSyncTime,
      orderBy: mapping.incrementalField
    });
  }

  // Extrair dados completos em lotes
  async extractFull(tableName, onBatch = null) {
    const mapping = ETLConfig.mappings[tableName];
    if (!mapping) {
      throw new Error(`Tabela ${tableName} não configurada`);
    }

    let offset = 0;
    let hasMore = true;
    let totalRecords = 0;
    const batchSize = ETLConfig.general.batchSize;

    logger.info(`📤 ETL Extractor: Iniciando extração completa de ${mapping.sourceTable}`);

    while (hasMore) {
      const result = await this.extractTable(tableName, {
        batchSize,
        offset,
        orderBy: mapping.primaryKey
      });

      if (!result.success) {
        throw new Error(`Erro na extração: ${result.error}`);
      }

      totalRecords += result.data.length;
      hasMore = result.metadata.hasMore;

      // Callback para processar lote
      if (onBatch && typeof onBatch === 'function') {
        await onBatch(result.data, {
          batch: Math.floor(offset / batchSize) + 1,
          offset,
          totalRecords
        });
      }

      offset += batchSize;

      // Log de progresso
      if (offset % (batchSize * 10) === 0) {
        logger.info(`📊 ETL Extractor: Progresso da extração`, {
          table: mapping.sourceTable,
          recordsProcessed: totalRecords,
          currentOffset: offset
        });
      }
    }

    logger.info(`✅ ETL Extractor: Extração completa finalizada`, {
      table: mapping.sourceTable,
      totalRecords
    });

    return {
      success: true,
      totalRecords,
      batches: Math.ceil(totalRecords / batchSize)
    };
  }

  // Construir query de extração
  buildExtractQuery(mapping, options) {
    const {
      batchSize,
      offset,
      incrementalField,
      lastSyncTime,
      filters,
      orderBy
    } = options;

    let query = `SELECT * FROM ${mapping.sourceTable}`;
    const values = [];
    const conditions = [];
    let paramIndex = 1;

    // Filtros de configuração
    if (mapping.filters) {
      Object.entries(mapping.filters).forEach(([field, condition]) => {
        if (Array.isArray(condition)) {
          const placeholders = condition.map(() => `$${paramIndex++}`).join(', ');
          conditions.push(`${field} IN (${placeholders})`);
          values.push(...condition);
        } else if (typeof condition === 'object') {
          Object.entries(condition).forEach(([operator, value]) => {
            conditions.push(`${field} ${operator} $${paramIndex++}`);
            values.push(value);
          });
        } else {
          conditions.push(`${field} = $${paramIndex++}`);
          values.push(condition);
        }
      });
    }

    // Filtros adicionais
    if (filters && Object.keys(filters).length > 0) {
      Object.entries(filters).forEach(([field, value]) => {
        conditions.push(`${field} = $${paramIndex++}`);
        values.push(value);
      });
    }

    // Filtro incremental
    if (incrementalField && lastSyncTime) {
      conditions.push(`${incrementalField} > $${paramIndex++}`);
      values.push(lastSyncTime);
    }

    // Adicionar condições WHERE
    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }

    // Ordenação
    if (orderBy) {
      query += ` ORDER BY ${orderBy}`;
    }

    // Paginação
    if (batchSize) {
      query += ` LIMIT $${paramIndex++}`;
      values.push(batchSize);
    }

    if (offset) {
      query += ` OFFSET $${paramIndex++}`;
      values.push(offset);
    }

    return { text: query, values };
  }

  // Obter metadados da tabela
  async getTableMetadata(tableName) {
    try {
      await this.connect();

      const mapping = ETLConfig.mappings[tableName];
      if (!mapping) {
        throw new Error(`Tabela ${tableName} não configurada`);
      }

      // Query para obter informações da tabela
      const metadataQuery = `
        SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default
        FROM information_schema.columns 
        WHERE table_name = $1 
        ORDER BY ordinal_position
      `;

      const result = await this.sourcePool.query(metadataQuery, [mapping.sourceTable]);

      // Query para obter contagem total
      const countQuery = `SELECT COUNT(*) as total FROM ${mapping.sourceTable}`;
      const countResult = await this.sourcePool.query(countQuery);

      return {
        tableName: mapping.sourceTable,
        columns: result.rows,
        totalRecords: parseInt(countResult.rows[0].total),
        primaryKey: mapping.primaryKey,
        incrementalField: mapping.incrementalField
      };

    } catch (error) {
      logger.error(`❌ ETL Extractor: Erro ao obter metadados de ${tableName}`, {
        error: error.message
      });
      throw error;
    }
  }

  // Verificar última sincronização
  async getLastSyncTime(tableName) {
    try {
      // Esta informação virá do banco de destino
      // Por enquanto, retornar null para sincronização completa
      return null;
    } catch (error) {
      logger.error(`❌ ETL Extractor: Erro ao obter última sincronização de ${tableName}`, {
        error: error.message
      });
      return null;
    }
  }

  // Utilitário para delay
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Fechar conexões
  async disconnect() {
    if (this.sourcePool) {
      await this.sourcePool.end();
      this.isConnected = false;
      logger.info('📴 ETL Extractor: Desconectado do banco de origem');
    }
  }

  // Obter estatísticas de conexão
  getConnectionStats() {
    if (!this.sourcePool) {
      return { connected: false };
    }

    return {
      connected: this.isConnected,
      totalCount: this.sourcePool.totalCount,
      idleCount: this.sourcePool.idleCount,
      waitingCount: this.sourcePool.waitingCount
    };
  }
}

module.exports = DataExtractor;

