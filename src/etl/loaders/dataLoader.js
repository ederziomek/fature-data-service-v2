const { Pool } = require('pg');
const ETLConfig = require('../config');
const logger = require('../../utils/logger');

class DataLoader {
  constructor() {
    this.targetPool = null;
    this.isConnected = false;
    this.connectionRetries = 0;
    this.maxRetries = ETLConfig.general.maxRetries;
    this.loadStats = {
      recordsLoaded: 0,
      recordsSkipped: 0,
      recordsUpdated: 0,
      recordsInserted: 0,
      errors: []
    };
  }

  // Conectar ao banco de destino (Affiliate Service)
  async connect() {
    if (this.isConnected && this.targetPool) {
      return this.targetPool;
    }

    try {
      this.targetPool = new Pool({
        host: ETLConfig.target.host,
        port: ETLConfig.target.port,
        database: ETLConfig.target.database,
        user: ETLConfig.target.username,
        password: ETLConfig.target.password,
        ssl: ETLConfig.target.ssl,
        connectionTimeoutMillis: ETLConfig.target.connectionTimeout,
        min: ETLConfig.target.pool.min,
        max: ETLConfig.target.pool.max,
        idleTimeoutMillis: 30000,
        query_timeout: 120000
      });

      // Testar conexão
      const client = await this.targetPool.connect();
      await client.query('SELECT 1');
      client.release();

      this.isConnected = true;
      this.connectionRetries = 0;
      
      logger.info('🔗 ETL Loader: Conectado ao banco de destino', {
        host: ETLConfig.target.host,
        database: ETLConfig.target.database
      });

      return this.targetPool;

    } catch (error) {
      this.connectionRetries++;
      logger.error('❌ ETL Loader: Erro ao conectar ao banco de destino', {
        error: error.message,
        attempt: this.connectionRetries,
        maxRetries: this.maxRetries
      });

      if (this.connectionRetries < this.maxRetries) {
        await this.delay(ETLConfig.general.retryDelay);
        return this.connect();
      }

      throw new Error(`Falha ao conectar ao banco de destino após ${this.maxRetries} tentativas`);
    }
  }

  // Carregar dados transformados
  async loadTable(tableName, transformedData) {
    const mapping = ETLConfig.mappings[tableName];
    if (!mapping) {
      throw new Error(`Mapeamento não encontrado para tabela ${tableName}`);
    }

    logger.info(`📥 ETL Loader: Iniciando carregamento de ${tableName}`, {
      recordCount: transformedData.length,
      targetTable: mapping.targetTable
    });

    const startTime = Date.now();
    let recordsProcessed = 0;

    try {
      await this.connect();

      // Processar em lotes para melhor performance
      const batchSize = ETLConfig.general.batchSize;
      const batches = this.chunkArray(transformedData, batchSize);

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        
        logger.info(`📦 ETL Loader: Processando lote ${i + 1}/${batches.length}`, {
          batchSize: batch.length
        });

        await this.loadBatch(mapping, batch);
        recordsProcessed += batch.length;

        // Log de progresso
        if ((i + 1) % 10 === 0) {
          logger.info(`📊 ETL Loader: Progresso do carregamento`, {
            table: mapping.targetTable,
            batchesProcessed: i + 1,
            totalBatches: batches.length,
            recordsProcessed
          });
        }
      }

      const loadTime = Date.now() - startTime;

      logger.info(`✅ ETL Loader: Carregamento concluído`, {
        table: mapping.targetTable,
        recordsProcessed,
        recordsLoaded: this.loadStats.recordsLoaded,
        recordsUpdated: this.loadStats.recordsUpdated,
        recordsInserted: this.loadStats.recordsInserted,
        recordsSkipped: this.loadStats.recordsSkipped,
        loadTime: `${loadTime}ms`
      });

      return {
        success: true,
        stats: {
          ...this.loadStats,
          recordsProcessed,
          loadTime
        }
      };

    } catch (error) {
      logger.error(`❌ ETL Loader: Erro ao carregar dados de ${tableName}`, {
        error: error.message,
        stack: error.stack
      });

      return {
        success: false,
        error: error.message,
        stats: this.loadStats
      };
    }
  }

  // Carregar um lote de dados
  async loadBatch(mapping, batch) {
    const client = await this.targetPool.connect();
    
    try {
      await client.query('BEGIN');

      for (const record of batch) {
        await this.loadRecord(client, mapping, record);
      }

      await client.query('COMMIT');

    } catch (error) {
      await client.query('ROLLBACK');
      logger.error(`❌ ETL Loader: Erro no lote`, {
        table: mapping.targetTable,
        error: error.message
      });
      throw error;
    } finally {
      client.release();
    }
  }

  // Carregar um registro individual
  async loadRecord(client, mapping, record) {
    try {
      // Remover metadados ETL antes de inserir
      const { _etl_metadata, _unique_fields, ...cleanRecord } = record;

      // Verificar se o registro já existe
      const existingRecord = await this.findExistingRecord(
        client, 
        mapping, 
        cleanRecord
      );

      if (existingRecord) {
        // Atualizar registro existente
        await this.updateRecord(client, mapping, cleanRecord, existingRecord);
        this.loadStats.recordsUpdated++;
      } else {
        // Inserir novo registro
        await this.insertRecord(client, mapping, cleanRecord);
        this.loadStats.recordsInserted++;
      }

      this.loadStats.recordsLoaded++;

    } catch (error) {
      this.loadStats.errors.push({
        table: mapping.targetTable,
        record: record,
        error: error.message,
        timestamp: new Date().toISOString()
      });

      logger.error(`❌ ETL Loader: Erro ao carregar registro`, {
        table: mapping.targetTable,
        error: error.message,
        record: record
      });

      // Decidir se deve continuar ou falhar
      if (error.code === '23505') { // Violação de unique constraint
        this.loadStats.recordsSkipped++;
        logger.warn(`⚠️ ETL Loader: Registro duplicado ignorado`, {
          table: mapping.targetTable,
          record: record
        });
      } else {
        throw error; // Re-throw outros erros
      }
    }
  }

  // Verificar se registro já existe
  async findExistingRecord(client, mapping, record) {
    // Usar external_user_id ou external_transaction_id como chave
    let keyField = 'external_user_id';
    if (mapping.targetTable === 'referrals') {
      keyField = 'external_transaction_id';
    } else if (mapping.targetTable === 'bet_activities') {
      keyField = 'external_bet_id';
    }

    if (!record[keyField]) {
      return null;
    }

    const query = `SELECT id FROM ${mapping.targetTable} WHERE ${keyField} = $1`;
    const result = await client.query(query, [record[keyField]]);
    
    return result.rows.length > 0 ? result.rows[0] : null;
  }

  // Inserir novo registro
  async insertRecord(client, mapping, record) {
    const fields = Object.keys(record);
    const values = Object.values(record);
    const placeholders = fields.map((_, index) => `$${index + 1}`).join(', ');

    const query = `
      INSERT INTO ${mapping.targetTable} (${fields.join(', ')})
      VALUES (${placeholders})
      RETURNING id
    `;

    const result = await client.query(query, values);
    
    logger.debug(`📝 ETL Loader: Registro inserido`, {
      table: mapping.targetTable,
      id: result.rows[0].id
    });

    return result.rows[0];
  }

  // Atualizar registro existente
  async updateRecord(client, mapping, record, existingRecord) {
    const fields = Object.keys(record);
    const values = Object.values(record);
    
    // Construir SET clause
    const setClause = fields.map((field, index) => `${field} = $${index + 1}`).join(', ');
    
    const query = `
      UPDATE ${mapping.targetTable} 
      SET ${setClause}, updated_at = NOW()
      WHERE id = $${fields.length + 1}
      RETURNING id
    `;

    const result = await client.query(query, [...values, existingRecord.id]);
    
    logger.debug(`📝 ETL Loader: Registro atualizado`, {
      table: mapping.targetTable,
      id: result.rows[0].id
    });

    return result.rows[0];
  }

  // Carregar dados específicos de afiliados
  async loadAffiliates(affiliatesData) {
    return this.loadTable('users', affiliatesData);
  }

  // Carregar dados específicos de referrals
  async loadReferrals(referralsData) {
    return this.loadTable('transactions', referralsData);
  }

  // Carregar dados específicos de atividades de apostas
  async loadBetActivities(betData) {
    return this.loadTable('bets', betData);
  }

  // Executar operações de limpeza
  async cleanup() {
    try {
      await this.connect();

      // Limpar registros órfãos
      await this.cleanupOrphanRecords();

      // Atualizar estatísticas
      await this.updateTableStatistics();

      // Limpar logs antigos
      await this.cleanupOldLogs();

      logger.info('🧹 ETL Loader: Limpeza concluída');

    } catch (error) {
      logger.error('❌ ETL Loader: Erro na limpeza', {
        error: error.message
      });
    }
  }

  // Limpar registros órfãos
  async cleanupOrphanRecords() {
    const client = await this.targetPool.connect();
    
    try {
      // Limpar referrals sem afiliados
      const orphanReferrals = await client.query(`
        DELETE FROM referrals 
        WHERE referred_user_id NOT IN (
          SELECT external_user_id FROM affiliates WHERE external_user_id IS NOT NULL
        )
      `);

      logger.info('🧹 ETL Loader: Registros órfãos removidos', {
        orphanReferrals: orphanReferrals.rowCount
      });

    } finally {
      client.release();
    }
  }

  // Atualizar estatísticas das tabelas
  async updateTableStatistics() {
    const client = await this.targetPool.connect();
    
    try {
      // Atualizar estatísticas do PostgreSQL
      await client.query('ANALYZE affiliates');
      await client.query('ANALYZE referrals');
      await client.query('ANALYZE bet_activities');

      logger.info('📊 ETL Loader: Estatísticas das tabelas atualizadas');

    } finally {
      client.release();
    }
  }

  // Limpar logs antigos
  async cleanupOldLogs() {
    const client = await this.targetPool.connect();
    const retentionDays = ETLConfig.monitoring.logRetention.days;
    
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      const result = await client.query(`
        DELETE FROM etl_logs 
        WHERE created_at < $1
      `, [cutoffDate]);

      logger.info('🧹 ETL Loader: Logs antigos removidos', {
        removedLogs: result.rowCount,
        retentionDays
      });

    } finally {
      client.release();
    }
  }

  // Utilitários
  chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Obter estatísticas de carregamento
  getStats() {
    return { ...this.loadStats };
  }

  // Resetar estatísticas
  resetStats() {
    this.loadStats = {
      recordsLoaded: 0,
      recordsSkipped: 0,
      recordsUpdated: 0,
      recordsInserted: 0,
      errors: []
    };
  }

  // Fechar conexões
  async disconnect() {
    if (this.targetPool) {
      await this.targetPool.end();
      this.isConnected = false;
      logger.info('📴 ETL Loader: Desconectado do banco de destino');
    }
  }

  // Obter estatísticas de conexão
  getConnectionStats() {
    if (!this.targetPool) {
      return { connected: false };
    }

    return {
      connected: this.isConnected,
      totalCount: this.targetPool.totalCount,
      idleCount: this.targetPool.idleCount,
      waitingCount: this.targetPool.waitingCount
    };
  }
}

module.exports = DataLoader;

