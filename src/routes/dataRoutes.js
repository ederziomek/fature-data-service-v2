const express = require('express');
const DataController = require('../controllers/dataController');
const { 
    validate, 
    schemas, 
    authenticate, 
    rateLimits, 
    validateParams,
    validateQuery,
    cacheResponse
} = require('../middleware/validation');

const router = express.Router();
const dataController = new DataController();

// Aplicar rate limiting geral
router.use(rateLimits.general);

// Health check (sem autenticação)
router.get('/health', dataController.healthCheck.bind(dataController));

// Aplicar autenticação para todas as rotas abaixo
router.use(authenticate);

// ===== ROTAS DE SINCRONIZAÇÃO =====

// Sincronizar dados de uma tabela específica
router.post('/sync/:tableName', 
    rateLimits.sync,
    validateParams.tableName,
    validate(schemas.syncExternalData),
    dataController.syncExternalData.bind(dataController)
);

// ===== ROTAS DE ANALYTICS =====

// Gerar analytics de usuário
router.post('/analytics/user/:userId/generate', 
    rateLimits.analytics,
    validateParams.userId,
    validateQuery.periodType,
    dataController.generateUserAnalytics.bind(dataController)
);

// Gerar analytics de afiliado
router.post('/analytics/affiliate/:affiliateId/generate', 
    rateLimits.analytics,
    validateParams.affiliateId,
    validateQuery.periodType,
    dataController.generateAffiliateAnalytics.bind(dataController)
);

// Buscar analytics de usuário
router.get('/analytics/user/:userId', 
    rateLimits.read,
    cacheResponse(300), // Cache por 5 minutos
    validateParams.userId,
    validateQuery.periodType,
    validateQuery.dateRange,
    dataController.getUserAnalytics.bind(dataController)
);

// Buscar analytics de afiliado
router.get('/analytics/affiliate/:affiliateId', 
    rateLimits.read,
    cacheResponse(300), // Cache por 5 minutos
    validateParams.affiliateId,
    validateQuery.periodType,
    validateQuery.dateRange,
    dataController.getAffiliateAnalytics.bind(dataController)
);

// ===== ROTAS DE DADOS EXTERNOS =====

// Buscar dados externos de uma tabela
router.get('/external/:tableName', 
    rateLimits.read,
    cacheResponse(60), // Cache por 1 minuto
    validateParams.tableName,
    validateQuery.dateRange,
    dataController.getExternalData.bind(dataController)
);

// ===== ROTAS DE EXPORTAÇÃO =====

// Criar nova exportação
router.post('/export', 
    rateLimits.export,
    validate(schemas.createDataExport),
    dataController.createDataExport.bind(dataController)
);

// Buscar todas as exportações
router.get('/export', 
    rateLimits.read,
    dataController.getDataExports.bind(dataController)
);

// Buscar exportação específica
router.get('/export/:exportId', 
    rateLimits.read,
    validateParams.exportId,
    dataController.getDataExport.bind(dataController)
);

// ===== ROTAS DE CONFIGURAÇÃO =====

// Buscar configurações atuais
router.get('/config', 
    rateLimits.read,
    cacheResponse(600), // Cache por 10 minutos
    dataController.getDataConfig.bind(dataController)
);

// ===== ROTAS DE MANUTENÇÃO =====

// Limpar cache expirado
router.delete('/cache/expired', 
    rateLimits.read,
    dataController.clearExpiredCache.bind(dataController)
);

// Estatísticas do serviço
router.get('/stats', 
    rateLimits.read,
    cacheResponse(60), // Cache por 1 minuto
    dataController.getServiceStats.bind(dataController)
);

module.exports = router;

