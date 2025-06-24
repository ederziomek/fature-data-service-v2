const express = require('express');
const router = express.Router();
const { etlManager } = require('../etl');
const logger = require('../utils/logger');

// Middleware de validação para ETL
const validateETLEnabled = (req, res, next) => {
  if (!etlManager.isInitialized) {
    return res.status(503).json({
      success: false,
      error: 'ETL não inicializado',
      message: 'O sistema ETL não foi inicializado ou está desabilitado'
    });
  }
  next();
};

// GET /api/v1/etl/status - Status do sistema ETL
router.get('/status', (req, res) => {
  try {
    const status = etlManager.getStatus();
    
    res.json({
      success: true,
      data: status,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    logger.error('❌ ETL API: Erro ao obter status', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível obter o status do ETL'
    });
  }
});

// GET /api/v1/etl/metrics - Métricas detalhadas
router.get('/metrics', validateETLEnabled, (req, res) => {
  try {
    const metrics = etlManager.getMetrics();
    
    res.json({
      success: true,
      data: metrics,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    logger.error('❌ ETL API: Erro ao obter métricas', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível obter as métricas do ETL'
    });
  }
});

// GET /api/v1/etl/tables - Informações das tabelas configuradas
router.get('/tables', validateETLEnabled, (req, res) => {
  try {
    const tables = etlManager.getTablesInfo();
    
    res.json({
      success: true,
      data: {
        tables,
        totalTables: tables.length,
        enabledTables: tables.filter(t => t.enabled).length,
        incrementalTables: tables.filter(t => t.supportsIncremental).length
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    logger.error('❌ ETL API: Erro ao obter informações das tabelas', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível obter informações das tabelas'
    });
  }
});

// POST /api/v1/etl/sync/full - Executar sincronização completa
router.post('/sync/full', validateETLEnabled, async (req, res) => {
  try {
    const { tables, skipValidation = false } = req.body;

    logger.info('🚀 ETL API: Iniciando sincronização completa via API', {
      tables: tables || 'todas',
      skipValidation,
      requestedBy: req.ip
    });

    // Executar sincronização de forma assíncrona
    const syncPromise = etlManager.runFullSync({
      tables,
      skipValidation,
      onProgress: (progress) => {
        logger.info('📊 ETL API: Progresso da sincronização', progress);
      }
    });

    // Responder imediatamente com ID do job
    const jobId = `full_sync_${Date.now()}`;
    
    res.status(202).json({
      success: true,
      message: 'Sincronização completa iniciada',
      jobId,
      estimatedTime: '5-30 minutos',
      checkStatusAt: `/api/v1/etl/status`,
      timestamp: new Date().toISOString()
    });

    // Aguardar conclusão em background
    try {
      const result = await syncPromise;
      logger.info('✅ ETL API: Sincronização completa concluída', {
        jobId,
        success: result.success,
        summary: result.summary
      });
    } catch (error) {
      logger.error('❌ ETL API: Erro na sincronização completa', {
        jobId,
        error: error.message
      });
    }

  } catch (error) {
    logger.error('❌ ETL API: Erro ao iniciar sincronização completa', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível iniciar a sincronização completa'
    });
  }
});

// POST /api/v1/etl/sync/incremental - Executar sincronização incremental
router.post('/sync/incremental', validateETLEnabled, async (req, res) => {
  try {
    const { tables, since } = req.body;

    logger.info('🚀 ETL API: Iniciando sincronização incremental via API', {
      tables: tables || 'todas',
      since,
      requestedBy: req.ip
    });

    // Executar sincronização
    const result = await etlManager.runIncrementalSync({
      tables,
      since
    });

    if (result.success) {
      res.json({
        success: true,
        message: 'Sincronização incremental concluída',
        data: result,
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        error: 'Erro na sincronização',
        message: result.error,
        timestamp: new Date().toISOString()
      });
    }

  } catch (error) {
    logger.error('❌ ETL API: Erro na sincronização incremental', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível executar a sincronização incremental'
    });
  }
});

// POST /api/v1/etl/sync/table/:tableName - Sincronizar tabela específica
router.post('/sync/table/:tableName', validateETLEnabled, async (req, res) => {
  try {
    const { tableName } = req.params;
    const { syncType = 'incremental', skipValidation = false, since } = req.body;

    // Validar se a tabela existe na configuração
    const tables = etlManager.getTablesInfo();
    const tableInfo = tables.find(t => t.name === tableName);
    
    if (!tableInfo) {
      return res.status(404).json({
        success: false,
        error: 'Tabela não encontrada',
        message: `A tabela '${tableName}' não está configurada`,
        availableTables: tables.map(t => t.name)
      });
    }

    if (!tableInfo.enabled) {
      return res.status(400).json({
        success: false,
        error: 'Tabela desabilitada',
        message: `A tabela '${tableName}' está desabilitada`
      });
    }

    if (syncType === 'incremental' && !tableInfo.supportsIncremental) {
      return res.status(400).json({
        success: false,
        error: 'Sincronização incremental não suportada',
        message: `A tabela '${tableName}' não suporta sincronização incremental`
      });
    }

    logger.info(`🚀 ETL API: Sincronizando tabela ${tableName}`, {
      syncType,
      skipValidation,
      since,
      requestedBy: req.ip
    });

    // Executar sincronização da tabela
    const result = await etlManager.syncTable(tableName, syncType, {
      skipValidation,
      since
    });

    if (result.success) {
      res.json({
        success: true,
        message: `Sincronização de ${tableName} concluída`,
        data: result,
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        error: 'Erro na sincronização',
        message: result.error,
        table: tableName,
        timestamp: new Date().toISOString()
      });
    }

  } catch (error) {
    logger.error(`❌ ETL API: Erro ao sincronizar tabela ${req.params.tableName}`, {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível sincronizar a tabela'
    });
  }
});

// POST /api/v1/etl/cleanup - Executar limpeza
router.post('/cleanup', validateETLEnabled, async (req, res) => {
  try {
    logger.info('🧹 ETL API: Executando limpeza via API', {
      requestedBy: req.ip
    });

    const result = await etlManager.runCleanup();

    if (result.success) {
      res.json({
        success: true,
        message: 'Limpeza executada com sucesso',
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        error: 'Erro na limpeza',
        message: result.error,
        timestamp: new Date().toISOString()
      });
    }

  } catch (error) {
    logger.error('❌ ETL API: Erro na limpeza', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível executar a limpeza'
    });
  }
});

// GET /api/v1/etl/scheduler/status - Status do agendador
router.get('/scheduler/status', validateETLEnabled, (req, res) => {
  try {
    const schedulerStatus = etlManager.scheduler.getStatus();
    
    res.json({
      success: true,
      data: schedulerStatus,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    logger.error('❌ ETL API: Erro ao obter status do agendador', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível obter o status do agendador'
    });
  }
});

// POST /api/v1/etl/scheduler/manual - Executar job manual do agendador
router.post('/scheduler/manual', validateETLEnabled, async (req, res) => {
  try {
    const { tableName, syncType = 'incremental' } = req.body;

    logger.info('🔧 ETL API: Executando sincronização manual via agendador', {
      tableName,
      syncType,
      requestedBy: req.ip
    });

    const result = await etlManager.scheduler.runManualSync(tableName, syncType);

    if (result.success) {
      res.json({
        success: true,
        message: 'Sincronização manual executada',
        data: result,
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        error: 'Erro na sincronização manual',
        message: result.error,
        timestamp: new Date().toISOString()
      });
    }

  } catch (error) {
    logger.error('❌ ETL API: Erro na sincronização manual', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: 'Erro interno',
      message: 'Não foi possível executar a sincronização manual'
    });
  }
});

// GET /api/v1/etl/docs - Documentação da API ETL
router.get('/docs', (req, res) => {
  res.json({
    title: 'Fature ETL API Documentation',
    version: '1.0.0',
    description: 'API para controle do sistema ETL de sincronização de dados',
    baseUrl: `${req.protocol}://${req.get('host')}/api/v1/etl`,
    
    endpoints: {
      'GET /status': {
        description: 'Obter status geral do sistema ETL',
        response: 'Status, configurações e estatísticas'
      },
      'GET /metrics': {
        description: 'Obter métricas detalhadas do ETL',
        response: 'Métricas de performance e estatísticas'
      },
      'GET /tables': {
        description: 'Listar tabelas configuradas',
        response: 'Lista de tabelas com configurações'
      },
      'POST /sync/full': {
        description: 'Executar sincronização completa',
        body: {
          tables: 'Array de nomes de tabelas (opcional)',
          skipValidation: 'Boolean para pular validações (opcional)'
        },
        response: 'Job ID e status da sincronização'
      },
      'POST /sync/incremental': {
        description: 'Executar sincronização incremental',
        body: {
          tables: 'Array de nomes de tabelas (opcional)',
          since: 'Timestamp para sincronização desde (opcional)'
        },
        response: 'Resultado da sincronização'
      },
      'POST /sync/table/:tableName': {
        description: 'Sincronizar tabela específica',
        params: {
          tableName: 'Nome da tabela a sincronizar'
        },
        body: {
          syncType: 'Tipo de sincronização (full|incremental)',
          skipValidation: 'Boolean para pular validações (opcional)',
          since: 'Timestamp para sincronização desde (opcional)'
        },
        response: 'Resultado da sincronização da tabela'
      },
      'POST /cleanup': {
        description: 'Executar limpeza do sistema',
        response: 'Status da limpeza'
      },
      'GET /scheduler/status': {
        description: 'Obter status do agendador',
        response: 'Status e configurações do agendador'
      },
      'POST /scheduler/manual': {
        description: 'Executar sincronização manual via agendador',
        body: {
          tableName: 'Nome da tabela (opcional)',
          syncType: 'Tipo de sincronização (full|incremental)'
        },
        response: 'Resultado da sincronização manual'
      }
    },

    authentication: {
      type: 'API Key',
      header: 'X-API-Key',
      description: 'Incluir API Key no header da requisição'
    },

    errorCodes: {
      400: 'Requisição inválida',
      404: 'Recurso não encontrado',
      500: 'Erro interno do servidor',
      503: 'Serviço indisponível (ETL não inicializado)'
    },

    examples: {
      fullSync: {
        url: 'POST /api/v1/etl/sync/full',
        body: {
          tables: ['users', 'transactions'],
          skipValidation: false
        }
      },
      incrementalSync: {
        url: 'POST /api/v1/etl/sync/incremental',
        body: {
          since: '2025-06-24T10:00:00Z'
        }
      },
      tableSync: {
        url: 'POST /api/v1/etl/sync/table/users',
        body: {
          syncType: 'incremental',
          skipValidation: false
        }
      }
    }
  });
});

module.exports = router;

