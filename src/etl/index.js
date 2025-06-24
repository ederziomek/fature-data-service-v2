const ETLConfig = require('./config');
const DataExtractor = require('./extractors/dataExtractor');
const DataTransformer = require('./transformers/dataTransformer');
const DataLoader = require('./loaders/dataLoader');
const ETLScheduler = require('./schedulers/etlScheduler');
const logger = require('../utils/logger');

class ETLManager {
  constructor() {
    this.extractor = new DataExtractor();
    this.transformer = new DataTransformer();
    this.loader = new DataLoader();
    this.scheduler = new ETLScheduler();
    
    this.isInitialized = false;
    this.isRunning = false;
    
    this.stats = {
      totalSyncs: 0,
      successfulSyncs: 0,
      failedSyncs: 0,
      lastSync: null,
      startTime: null
    };
  }

  // Inicializar ETL Manager
  async initialize() {
    if (this.isInitialized) {
      logger.warn('⚠️ ETL Manager: Já inicializado');
      return;
    }

    logger.info('🚀 ETL Manager: Inicializando sistema ETL');

    try {
      // Verificar se ETL está habilitado
      if (!ETLConfig.general.enabled) {
        logger.info('📴 ETL Manager: Sistema ETL desabilitado');
        return;
      }

      // Testar conexões
      await this.testConnections();

      // Inicializar agendador se habilitado
      if (ETLConfig.schedule.enabled) {
        await this.scheduler.initialize();
      }

      this.isInitialized = true;
      this.isRunning = true;
      this.stats.startTime = new Date().toISOString();

      logger.info('✅ ETL Manager: Sistema ETL inicializado com sucesso', {
        enabled: ETLConfig.general.enabled,
        scheduleEnabled: ETLConfig.schedule.enabled,
        tablesConfigured: Object.keys(ETLConfig.mappings).length
      });

    } catch (error) {
      logger.error('❌ ETL Manager: Erro ao inicializar', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  // Testar conexões com bancos de dados
  async testConnections() {
    logger.info('🔍 ETL Manager: Testando conexões');

    try {
      // Testar conexão com banco de origem
      await this.extractor.connect();
      logger.info('✅ ETL Manager: Conexão com banco de origem OK');

      // Testar conexão com banco de destino
      await this.loader.connect();
      logger.info('✅ ETL Manager: Conexão com banco de destino OK');

    } catch (error) {
      logger.error('❌ ETL Manager: Erro ao testar conexões', {
        error: error.message
      });
      throw error;
    }
  }

  // Executar sincronização completa
  async runFullSync(options = {}) {
    const { 
      tables = null, 
      skipValidation = false,
      onProgress = null 
    } = options;

    const syncId = `full_${Date.now()}`;
    this.stats.totalSyncs++;

    logger.info('🚀 ETL Manager: Iniciando sincronização completa', {
      syncId,
      tables: tables || 'todas'
    });

    try {
      const startTime = Date.now();
      const results = {};

      // Determinar tabelas a sincronizar
      const tablesToSync = tables || Object.keys(ETLConfig.mappings).filter(
        table => ETLConfig.mappings[table].enabled
      );

      let processedTables = 0;

      for (const tableName of tablesToSync) {
        logger.info(`📊 ETL Manager: Sincronizando ${tableName}`, {
          progress: `${processedTables + 1}/${tablesToSync.length}`
        });

        const tableResult = await this.syncTable(tableName, 'full', { skipValidation });
        results[tableName] = tableResult;

        processedTables++;

        // Callback de progresso
        if (onProgress && typeof onProgress === 'function') {
          onProgress({
            table: tableName,
            progress: processedTables / tablesToSync.length,
            result: tableResult
          });
        }

        // Delay entre tabelas
        await this.delay(3000);
      }

      const totalTime = Date.now() - startTime;
      const summary = this.summarizeResults(results);

      this.stats.successfulSyncs++;
      this.stats.lastSync = new Date().toISOString();

      logger.info('✅ ETL Manager: Sincronização completa concluída', {
        syncId,
        totalTime: `${totalTime}ms`,
        summary
      });

      return {
        success: true,
        syncId,
        type: 'full',
        results,
        summary,
        totalTime
      };

    } catch (error) {
      this.stats.failedSyncs++;
      logger.error('❌ ETL Manager: Erro na sincronização completa', {
        syncId,
        error: error.message
      });

      return {
        success: false,
        syncId,
        type: 'full',
        error: error.message
      };
    }
  }

  // Executar sincronização incremental
  async runIncrementalSync(options = {}) {
    const { 
      tables = null,
      since = null,
      onProgress = null 
    } = options;

    const syncId = `incremental_${Date.now()}`;
    this.stats.totalSyncs++;

    logger.info('🚀 ETL Manager: Iniciando sincronização incremental', {
      syncId,
      tables: tables || 'todas',
      since
    });

    try {
      const startTime = Date.now();
      const results = {};

      // Determinar tabelas a sincronizar (apenas com suporte incremental)
      const tablesToSync = (tables || Object.keys(ETLConfig.mappings)).filter(
        table => ETLConfig.mappings[table].enabled && 
                ETLConfig.mappings[table].incrementalField
      );

      let processedTables = 0;

      for (const tableName of tablesToSync) {
        logger.info(`📊 ETL Manager: Sincronização incremental de ${tableName}`, {
          progress: `${processedTables + 1}/${tablesToSync.length}`
        });

        const tableResult = await this.syncTable(tableName, 'incremental', { since });
        results[tableName] = tableResult;

        processedTables++;

        // Callback de progresso
        if (onProgress && typeof onProgress === 'function') {
          onProgress({
            table: tableName,
            progress: processedTables / tablesToSync.length,
            result: tableResult
          });
        }

        // Delay menor para sincronização incremental
        await this.delay(1000);
      }

      const totalTime = Date.now() - startTime;
      const summary = this.summarizeResults(results);

      this.stats.successfulSyncs++;
      this.stats.lastSync = new Date().toISOString();

      logger.info('✅ ETL Manager: Sincronização incremental concluída', {
        syncId,
        totalTime: `${totalTime}ms`,
        summary
      });

      return {
        success: true,
        syncId,
        type: 'incremental',
        results,
        summary,
        totalTime
      };

    } catch (error) {
      this.stats.failedSyncs++;
      logger.error('❌ ETL Manager: Erro na sincronização incremental', {
        syncId,
        error: error.message
      });

      return {
        success: false,
        syncId,
        type: 'incremental',
        error: error.message
      };
    }
  }

  // Sincronizar uma tabela específica
  async syncTable(tableName, syncType = 'incremental', options = {}) {
    const { skipValidation = false, since = null } = options;

    logger.info(`🔄 ETL Manager: Sincronizando tabela ${tableName}`, {
      syncType,
      skipValidation
    });

    try {
      const startTime = Date.now();

      // 1. Extração
      let extractResult;
      if (syncType === 'incremental') {
        const lastSyncTime = since || await this.getLastSyncTime(tableName);
        extractResult = await this.extractor.extractIncremental(tableName, lastSyncTime);
      } else {
        // Para sincronização completa, processar em lotes
        const batchResults = [];
        await this.extractor.extractFull(tableName, async (data, metadata) => {
          const batchResult = await this.processBatch(tableName, data, skipValidation);
          batchResults.push(batchResult);
        });

        return {
          success: true,
          syncType,
          batches: batchResults,
          totalTime: Date.now() - startTime
        };
      }

      if (!extractResult.success) {
        throw new Error(`Erro na extração: ${extractResult.error}`);
      }

      if (extractResult.data.length === 0) {
        return {
          success: true,
          syncType,
          message: 'Nenhum dado novo para sincronizar',
          recordCount: 0,
          totalTime: Date.now() - startTime
        };
      }

      // Processar dados extraídos
      const result = await this.processBatch(tableName, extractResult.data, skipValidation);
      result.totalTime = Date.now() - startTime;
      result.syncType = syncType;

      return result;

    } catch (error) {
      logger.error(`❌ ETL Manager: Erro ao sincronizar ${tableName}`, {
        error: error.message,
        syncType
      });

      return {
        success: false,
        syncType,
        error: error.message
      };
    }
  }

  // Processar um lote de dados
  async processBatch(tableName, data, skipValidation = false) {
    try {
      // 2. Transformação
      const transformResult = await this.transformer.transformTable(tableName, data);
      
      if (!transformResult.success) {
        throw new Error('Erro na transformação dos dados');
      }

      if (transformResult.transformedData.length === 0) {
        return {
          success: true,
          message: 'Nenhum dado válido após transformação',
          extract: { recordCount: data.length },
          transform: transformResult.stats,
          load: { recordsLoaded: 0 }
        };
      }

      // 3. Carregamento
      const loadResult = await this.loader.loadTable(tableName, transformResult.transformedData);
      
      if (!loadResult.success) {
        throw new Error(`Erro no carregamento: ${loadResult.error}`);
      }

      return {
        success: true,
        extract: { recordCount: data.length },
        transform: transformResult.stats,
        load: loadResult.stats
      };

    } catch (error) {
      logger.error(`❌ ETL Manager: Erro ao processar lote de ${tableName}`, {
        error: error.message
      });

      return {
        success: false,
        error: error.message
      };
    }
  }

  // Obter informações das tabelas configuradas
  getTablesInfo() {
    return Object.entries(ETLConfig.mappings).map(([tableName, mapping]) => ({
      name: tableName,
      sourceTable: mapping.sourceTable,
      targetTable: mapping.targetTable,
      enabled: mapping.enabled,
      supportsIncremental: !!mapping.incrementalField,
      incrementalField: mapping.incrementalField,
      primaryKey: mapping.primaryKey
    }));
  }

  // Obter status do sistema ETL
  getStatus() {
    return {
      isInitialized: this.isInitialized,
      isRunning: this.isRunning,
      config: {
        enabled: ETLConfig.general.enabled,
        batchSize: ETLConfig.general.batchSize,
        maxRetries: ETLConfig.general.maxRetries,
        scheduleEnabled: ETLConfig.schedule.enabled
      },
      connections: {
        extractor: this.extractor.getConnectionStats(),
        loader: this.loader.getConnectionStats()
      },
      scheduler: this.scheduler.getStatus(),
      stats: this.stats,
      tables: this.getTablesInfo()
    };
  }

  // Obter métricas detalhadas
  getMetrics() {
    return {
      ...this.stats,
      extractorStats: this.extractor.getConnectionStats(),
      transformerStats: this.transformer.getStats(),
      loaderStats: this.loader.getStats(),
      schedulerStats: this.scheduler.getStatus().stats
    };
  }

  // Executar limpeza
  async runCleanup() {
    logger.info('🧹 ETL Manager: Executando limpeza');

    try {
      await this.loader.cleanup();
      logger.info('✅ ETL Manager: Limpeza concluída');
      return { success: true };
    } catch (error) {
      logger.error('❌ ETL Manager: Erro na limpeza', {
        error: error.message
      });
      return { success: false, error: error.message };
    }
  }

  // Parar sistema ETL
  async stop() {
    logger.info('📴 ETL Manager: Parando sistema ETL');

    try {
      // Parar agendador
      if (this.scheduler.isRunning) {
        await this.scheduler.stop();
      }

      // Fechar conexões
      await this.extractor.disconnect();
      await this.loader.disconnect();

      this.isRunning = false;
      logger.info('✅ ETL Manager: Sistema ETL parado');

    } catch (error) {
      logger.error('❌ ETL Manager: Erro ao parar sistema ETL', {
        error: error.message
      });
    }
  }

  // Utilitários
  async getLastSyncTime(tableName) {
    // Por enquanto, usar timestamp padrão
    // Em produção, isso viria de uma tabela de controle
    const defaultTime = new Date();
    defaultTime.setHours(defaultTime.getHours() - 1);
    return defaultTime.toISOString();
  }

  summarizeResults(results) {
    const summary = {
      totalTables: Object.keys(results).length,
      successfulTables: 0,
      failedTables: 0,
      totalRecordsProcessed: 0,
      totalRecordsLoaded: 0
    };

    Object.values(results).forEach(result => {
      if (result.success) {
        summary.successfulTables++;
        if (result.extract) {
          summary.totalRecordsProcessed += result.extract.recordCount || 0;
        }
        if (result.load) {
          summary.totalRecordsLoaded += result.load.recordsLoaded || 0;
        }
      } else {
        summary.failedTables++;
      }
    });

    return summary;
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Singleton instance
const etlManager = new ETLManager();

module.exports = {
  ETLManager,
  etlManager,
  ETLConfig,
  DataExtractor,
  DataTransformer,
  DataLoader,
  ETLScheduler
};

