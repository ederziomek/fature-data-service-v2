const cron = require('node-cron');
const ETLConfig = require('../config');
const DataExtractor = require('../extractors/dataExtractor');
const DataTransformer = require('../transformers/dataTransformer');
const DataLoader = require('../loaders/dataLoader');
const logger = require('../../utils/logger');

class ETLScheduler {
  constructor() {
    this.extractor = new DataExtractor();
    this.transformer = new DataTransformer();
    this.loader = new DataLoader();
    
    this.jobs = new Map();
    this.isRunning = false;
    this.currentJobs = new Set();
    
    this.stats = {
      totalJobs: 0,
      successfulJobs: 0,
      failedJobs: 0,
      lastFullSync: null,
      lastIncrementalSync: null,
      lastCleanup: null
    };
  }

  // Inicializar agendador
  async initialize() {
    if (!ETLConfig.schedule.enabled) {
      logger.info('📅 ETL Scheduler: Agendamento desabilitado');
      return;
    }

    logger.info('📅 ETL Scheduler: Inicializando agendamentos');

    try {
      // Agendar sincronização completa
      if (ETLConfig.schedule.fullSync.enabled) {
        this.scheduleFullSync();
      }

      // Agendar sincronização incremental
      if (ETLConfig.schedule.incrementalSync.enabled) {
        this.scheduleIncrementalSync();
      }

      // Agendar limpeza
      if (ETLConfig.schedule.cleanup.enabled) {
        this.scheduleCleanup();
      }

      this.isRunning = true;
      logger.info('✅ ETL Scheduler: Agendamentos configurados com sucesso');

    } catch (error) {
      logger.error('❌ ETL Scheduler: Erro ao inicializar', {
        error: error.message
      });
      throw error;
    }
  }

  // Agendar sincronização completa
  scheduleFullSync() {
    const cronExpression = ETLConfig.schedule.fullSync.cron;
    
    const job = cron.schedule(cronExpression, async () => {
      await this.executeFullSync();
    }, {
      scheduled: false,
      timezone: 'America/Sao_Paulo'
    });

    this.jobs.set('fullSync', job);
    job.start();

    logger.info('📅 ETL Scheduler: Sincronização completa agendada', {
      cron: cronExpression,
      timezone: 'America/Sao_Paulo'
    });
  }

  // Agendar sincronização incremental
  scheduleIncrementalSync() {
    const cronExpression = ETLConfig.schedule.incrementalSync.cron;
    
    const job = cron.schedule(cronExpression, async () => {
      await this.executeIncrementalSync();
    }, {
      scheduled: false,
      timezone: 'America/Sao_Paulo'
    });

    this.jobs.set('incrementalSync', job);
    job.start();

    logger.info('📅 ETL Scheduler: Sincronização incremental agendada', {
      cron: cronExpression,
      timezone: 'America/Sao_Paulo'
    });
  }

  // Agendar limpeza
  scheduleCleanup() {
    const cronExpression = ETLConfig.schedule.cleanup.cron;
    
    const job = cron.schedule(cronExpression, async () => {
      await this.executeCleanup();
    }, {
      scheduled: false,
      timezone: 'America/Sao_Paulo'
    });

    this.jobs.set('cleanup', job);
    job.start();

    logger.info('📅 ETL Scheduler: Limpeza agendada', {
      cron: cronExpression,
      timezone: 'America/Sao_Paulo'
    });
  }

  // Executar sincronização completa
  async executeFullSync() {
    const jobId = `fullSync_${Date.now()}`;
    
    if (this.currentJobs.has('fullSync')) {
      logger.warn('⚠️ ETL Scheduler: Sincronização completa já em execução');
      return;
    }

    this.currentJobs.add('fullSync');
    this.stats.totalJobs++;

    logger.info('🚀 ETL Scheduler: Iniciando sincronização completa', { jobId });

    try {
      const startTime = Date.now();
      const results = {};

      // Sincronizar todas as tabelas configuradas
      const tables = Object.keys(ETLConfig.mappings).filter(
        table => ETLConfig.mappings[table].enabled
      );

      for (const tableName of tables) {
        logger.info(`📊 ETL Scheduler: Sincronizando tabela ${tableName}`);
        
        const tableResult = await this.syncTable(tableName, 'full');
        results[tableName] = tableResult;

        // Delay entre tabelas para não sobrecarregar
        await this.delay(5000);
      }

      const totalTime = Date.now() - startTime;
      this.stats.successfulJobs++;
      this.stats.lastFullSync = new Date().toISOString();

      logger.info('✅ ETL Scheduler: Sincronização completa concluída', {
        jobId,
        totalTime: `${totalTime}ms`,
        results: this.summarizeResults(results)
      });

      // Executar limpeza após sincronização completa
      await this.loader.cleanup();

    } catch (error) {
      this.stats.failedJobs++;
      logger.error('❌ ETL Scheduler: Erro na sincronização completa', {
        jobId,
        error: error.message,
        stack: error.stack
      });
    } finally {
      this.currentJobs.delete('fullSync');
    }
  }

  // Executar sincronização incremental
  async executeIncrementalSync() {
    const jobId = `incrementalSync_${Date.now()}`;
    
    if (this.currentJobs.has('incrementalSync')) {
      logger.warn('⚠️ ETL Scheduler: Sincronização incremental já em execução');
      return;
    }

    this.currentJobs.add('incrementalSync');
    this.stats.totalJobs++;

    logger.info('🚀 ETL Scheduler: Iniciando sincronização incremental', { jobId });

    try {
      const startTime = Date.now();
      const results = {};

      // Sincronizar apenas tabelas com suporte incremental
      const tables = Object.keys(ETLConfig.mappings).filter(
        table => ETLConfig.mappings[table].enabled && 
                ETLConfig.mappings[table].incrementalField
      );

      for (const tableName of tables) {
        logger.info(`📊 ETL Scheduler: Sincronização incremental de ${tableName}`);
        
        const tableResult = await this.syncTable(tableName, 'incremental');
        results[tableName] = tableResult;

        // Delay menor entre tabelas na sincronização incremental
        await this.delay(2000);
      }

      const totalTime = Date.now() - startTime;
      this.stats.successfulJobs++;
      this.stats.lastIncrementalSync = new Date().toISOString();

      logger.info('✅ ETL Scheduler: Sincronização incremental concluída', {
        jobId,
        totalTime: `${totalTime}ms`,
        results: this.summarizeResults(results)
      });

    } catch (error) {
      this.stats.failedJobs++;
      logger.error('❌ ETL Scheduler: Erro na sincronização incremental', {
        jobId,
        error: error.message,
        stack: error.stack
      });
    } finally {
      this.currentJobs.delete('incrementalSync');
    }
  }

  // Executar limpeza
  async executeCleanup() {
    const jobId = `cleanup_${Date.now()}`;
    
    if (this.currentJobs.has('cleanup')) {
      logger.warn('⚠️ ETL Scheduler: Limpeza já em execução');
      return;
    }

    this.currentJobs.add('cleanup');
    this.stats.totalJobs++;

    logger.info('🧹 ETL Scheduler: Iniciando limpeza', { jobId });

    try {
      const startTime = Date.now();

      // Executar limpeza
      await this.loader.cleanup();

      const totalTime = Date.now() - startTime;
      this.stats.successfulJobs++;
      this.stats.lastCleanup = new Date().toISOString();

      logger.info('✅ ETL Scheduler: Limpeza concluída', {
        jobId,
        totalTime: `${totalTime}ms`
      });

    } catch (error) {
      this.stats.failedJobs++;
      logger.error('❌ ETL Scheduler: Erro na limpeza', {
        jobId,
        error: error.message
      });
    } finally {
      this.currentJobs.delete('cleanup');
    }
  }

  // Sincronizar uma tabela específica
  async syncTable(tableName, syncType = 'full') {
    try {
      let extractResult;

      if (syncType === 'incremental') {
        // Obter última sincronização
        const lastSyncTime = await this.getLastSyncTime(tableName);
        extractResult = await this.extractor.extractIncremental(tableName, lastSyncTime);
      } else {
        // Extração completa em lotes
        const results = [];
        await this.extractor.extractFull(tableName, async (data, metadata) => {
          // Transformar dados
          const transformResult = await this.transformer.transformTable(tableName, data);
          
          if (transformResult.success && transformResult.transformedData.length > 0) {
            // Carregar dados
            const loadResult = await this.loader.loadTable(tableName, transformResult.transformedData);
            results.push({
              batch: metadata.batch,
              extract: { recordCount: data.length },
              transform: transformResult.stats,
              load: loadResult.stats
            });
          }
        });

        return {
          success: true,
          syncType,
          batches: results,
          totalRecords: results.reduce((sum, batch) => sum + batch.extract.recordCount, 0)
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
          recordCount: 0
        };
      }

      // Transformar dados
      const transformResult = await this.transformer.transformTable(tableName, extractResult.data);
      
      if (!transformResult.success) {
        throw new Error('Erro na transformação dos dados');
      }

      // Carregar dados
      const loadResult = await this.loader.loadTable(tableName, transformResult.transformedData);
      
      if (!loadResult.success) {
        throw new Error(`Erro no carregamento: ${loadResult.error}`);
      }

      // Atualizar timestamp da última sincronização
      await this.updateLastSyncTime(tableName);

      return {
        success: true,
        syncType,
        extract: extractResult.metadata,
        transform: transformResult.stats,
        load: loadResult.stats
      };

    } catch (error) {
      logger.error(`❌ ETL Scheduler: Erro ao sincronizar tabela ${tableName}`, {
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

  // Obter timestamp da última sincronização
  async getLastSyncTime(tableName) {
    try {
      // Por enquanto, usar um valor padrão
      // Em produção, isso viria de uma tabela de controle
      const defaultTime = new Date();
      defaultTime.setHours(defaultTime.getHours() - 1); // Última hora
      return defaultTime.toISOString();
    } catch (error) {
      logger.error(`❌ ETL Scheduler: Erro ao obter última sincronização de ${tableName}`, {
        error: error.message
      });
      return null;
    }
  }

  // Atualizar timestamp da última sincronização
  async updateLastSyncTime(tableName) {
    try {
      // Por enquanto, apenas log
      // Em produção, isso seria salvo em uma tabela de controle
      logger.info(`📝 ETL Scheduler: Última sincronização atualizada para ${tableName}`, {
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error(`❌ ETL Scheduler: Erro ao atualizar última sincronização de ${tableName}`, {
        error: error.message
      });
    }
  }

  // Executar sincronização manual
  async runManualSync(tableName = null, syncType = 'incremental') {
    const jobId = `manual_${syncType}_${Date.now()}`;
    
    logger.info('🔧 ETL Scheduler: Executando sincronização manual', {
      jobId,
      tableName,
      syncType
    });

    try {
      if (tableName) {
        // Sincronizar tabela específica
        const result = await this.syncTable(tableName, syncType);
        return {
          success: true,
          jobId,
          table: tableName,
          result
        };
      } else {
        // Sincronizar todas as tabelas
        if (syncType === 'full') {
          await this.executeFullSync();
        } else {
          await this.executeIncrementalSync();
        }
        
        return {
          success: true,
          jobId,
          message: `Sincronização ${syncType} executada`
        };
      }

    } catch (error) {
      logger.error('❌ ETL Scheduler: Erro na sincronização manual', {
        jobId,
        error: error.message
      });

      return {
        success: false,
        jobId,
        error: error.message
      };
    }
  }

  // Resumir resultados
  summarizeResults(results) {
    const summary = {
      totalTables: Object.keys(results).length,
      successfulTables: 0,
      failedTables: 0,
      totalRecords: 0
    };

    Object.values(results).forEach(result => {
      if (result.success) {
        summary.successfulTables++;
        if (result.load && result.load.recordsLoaded) {
          summary.totalRecords += result.load.recordsLoaded;
        }
      } else {
        summary.failedTables++;
      }
    });

    return summary;
  }

  // Parar agendador
  async stop() {
    logger.info('📅 ETL Scheduler: Parando agendamentos');

    // Parar todos os jobs
    this.jobs.forEach((job, name) => {
      job.stop();
      logger.info(`📅 ETL Scheduler: Job ${name} parado`);
    });

    // Aguardar jobs em execução terminarem
    while (this.currentJobs.size > 0) {
      logger.info('⏳ ETL Scheduler: Aguardando jobs em execução terminarem', {
        runningJobs: Array.from(this.currentJobs)
      });
      await this.delay(5000);
    }

    // Fechar conexões
    await this.extractor.disconnect();
    await this.loader.disconnect();

    this.isRunning = false;
    logger.info('✅ ETL Scheduler: Agendador parado');
  }

  // Obter status do agendador
  getStatus() {
    return {
      isRunning: this.isRunning,
      scheduledJobs: Array.from(this.jobs.keys()),
      currentJobs: Array.from(this.currentJobs),
      stats: this.stats,
      config: {
        fullSyncEnabled: ETLConfig.schedule.fullSync.enabled,
        fullSyncCron: ETLConfig.schedule.fullSync.cron,
        incrementalSyncEnabled: ETLConfig.schedule.incrementalSync.enabled,
        incrementalSyncCron: ETLConfig.schedule.incrementalSync.cron,
        cleanupEnabled: ETLConfig.schedule.cleanup.enabled,
        cleanupCron: ETLConfig.schedule.cleanup.cron
      }
    };
  }

  // Utilitário para delay
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = ETLScheduler;

