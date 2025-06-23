const Joi = require('joi');
const logger = require('../utils/logger');

// Middleware de validação genérico
const validate = (schema) => {
    return (req, res, next) => {
        const { error, value } = schema.validate(req.body);
        
        if (error) {
            const errors = error.details.map(detail => ({
                field: detail.path.join('.'),
                message: detail.message
            }));
            
            return res.status(400).json({
                success: false,
                message: 'Dados de entrada inválidos',
                errors
            });
        }
        
        req.body = value;
        next();
    };
};

// Schemas de validação
const schemas = {
    syncExternalData: Joi.object({
        batchSize: Joi.number().integer().min(1).max(10000).optional(),
        maxRetries: Joi.number().integer().min(0).max(10).optional(),
        dateFrom: Joi.date().optional(),
        dateTo: Joi.date().optional(),
        userId: Joi.number().integer().positive().optional(),
        affiliateId: Joi.number().integer().positive().optional(),
        forceSync: Joi.boolean().optional()
    }),

    createDataExport: Joi.object({
        exportType: Joi.string().valid('users', 'transactions', 'bets', 'deposits', 'analytics', 'commissions').required(),
        format: Joi.string().valid('CSV', 'JSON', 'XLSX', 'PDF').required(),
        filters: Joi.object().optional(),
        dateRangeStart: Joi.date().optional(),
        dateRangeEnd: Joi.date().optional(),
        retentionDays: Joi.number().integer().min(1).max(30).optional()
    })
};

// Middleware de autenticação
const authenticate = (req, res, next) => {
    const apiKey = req.headers['x-api-key'] || req.headers['authorization'];
    
    if (!apiKey) {
        return res.status(401).json({
            success: false,
            message: 'API Key obrigatória'
        });
    }

    // Verificar API Key
    const validApiKey = process.env.API_KEY_SECRET;
    if (apiKey !== validApiKey && apiKey !== `Bearer ${validApiKey}`) {
        return res.status(401).json({
            success: false,
            message: 'API Key inválida'
        });
    }

    // Adicionar usuário fictício para auditoria
    req.user = {
        username: 'api_user',
        role: 'admin'
    };

    next();
};

// Middleware de rate limiting
const rateLimit = require('express-rate-limit');

const createRateLimit = (windowMs, max, message) => {
    return rateLimit({
        windowMs,
        max,
        message: {
            success: false,
            message: message || 'Muitas requisições, tente novamente mais tarde'
        },
        standardHeaders: true,
        legacyHeaders: false,
    });
};

// Rate limits específicos
const rateLimits = {
    general: createRateLimit(
        parseInt(process.env.RATE_LIMIT_WINDOW) || 15 * 60 * 1000, // 15 minutos
        parseInt(process.env.RATE_LIMIT_MAX_REQUESTS) || 500,
        'Limite de requisições excedido'
    ),
    
    sync: createRateLimit(
        5 * 60 * 1000, // 5 minutos
        10, // 10 sincronizações por 5 minutos
        'Limite de sincronizações excedido'
    ),
    
    analytics: createRateLimit(
        1 * 60 * 1000, // 1 minuto
        100, // 100 gerações de analytics por minuto
        'Limite de geração de analytics excedido'
    ),

    export: createRateLimit(
        10 * 60 * 1000, // 10 minutos
        5, // 5 exportações por 10 minutos
        'Limite de exportações excedido'
    ),

    read: createRateLimit(
        1 * 60 * 1000, // 1 minuto
        1000, // 1000 operações de leitura por minuto
        'Limite de operações de leitura excedido'
    )
};

// Middleware de logging de requisições
const requestLogger = (req, res, next) => {
    const start = Date.now();
    
    res.on('finish', () => {
        const duration = Date.now() - start;
        const logData = {
            method: req.method,
            url: req.url,
            status: res.statusCode,
            duration: `${duration}ms`,
            ip: req.ip,
            userAgent: req.get('User-Agent')
        };
        
        if (res.statusCode >= 400) {
            logger.warn('Request completed with error', logData);
        } else {
            logger.info('Request completed', logData);
        }
    });
    
    next();
};

// Middleware de tratamento de erros
const errorHandler = (err, req, res, next) => {
    logger.error('Erro não tratado:', {
        error: err.message,
        stack: err.stack,
        url: req.url,
        method: req.method,
        ip: req.ip
    });

    // Erro de validação do Joi
    if (err.isJoi) {
        return res.status(400).json({
            success: false,
            message: 'Dados de entrada inválidos',
            errors: err.details.map(detail => ({
                field: detail.path.join('.'),
                message: detail.message
            }))
        });
    }

    // Erro de banco de dados
    if (err.code === '23505') { // Unique constraint violation
        return res.status(409).json({
            success: false,
            message: 'Registro já existe'
        });
    }

    if (err.code === '23503') { // Foreign key constraint violation
        return res.status(400).json({
            success: false,
            message: 'Referência inválida'
        });
    }

    // Erro de timeout
    if (err.code === 'ECONNABORTED' || err.message.includes('timeout')) {
        return res.status(504).json({
            success: false,
            message: 'Timeout na operação'
        });
    }

    // Erro genérico
    res.status(500).json({
        success: false,
        message: 'Erro interno do servidor',
        error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
};

// Middleware de CORS customizado
const corsHandler = (req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, X-API-Key');
    res.header('Access-Control-Allow-Credentials', 'true');
    
    if (req.method === 'OPTIONS') {
        res.sendStatus(200);
    } else {
        next();
    }
};

// Middleware de validação de parâmetros
const validateParams = {
    userId: (req, res, next) => {
        const { userId } = req.params;
        
        if (!userId || isNaN(parseInt(userId)) || parseInt(userId) <= 0) {
            return res.status(400).json({
                success: false,
                message: 'userId deve ser um número inteiro positivo'
            });
        }
        
        req.params.userId = parseInt(userId);
        next();
    },

    affiliateId: (req, res, next) => {
        const { affiliateId } = req.params;
        
        if (!affiliateId || isNaN(parseInt(affiliateId)) || parseInt(affiliateId) <= 0) {
            return res.status(400).json({
                success: false,
                message: 'affiliateId deve ser um número inteiro positivo'
            });
        }
        
        req.params.affiliateId = parseInt(affiliateId);
        next();
    },

    tableName: (req, res, next) => {
        const { tableName } = req.params;
        const validTables = ['users', 'transactions', 'bets', 'deposits'];
        
        if (!tableName || !validTables.includes(tableName)) {
            return res.status(400).json({
                success: false,
                message: `tableName deve ser um dos seguintes: ${validTables.join(', ')}`
            });
        }
        
        next();
    },

    exportId: (req, res, next) => {
        const { exportId } = req.params;
        
        if (!exportId || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(exportId)) {
            return res.status(400).json({
                success: false,
                message: 'exportId deve ser um UUID válido'
            });
        }
        
        next();
    }
};

// Middleware de validação de query parameters
const validateQuery = {
    periodType: (req, res, next) => {
        const { periodType } = req.query;
        const validPeriods = ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'];
        
        if (periodType && !validPeriods.includes(periodType)) {
            return res.status(400).json({
                success: false,
                message: `periodType deve ser um dos seguintes: ${validPeriods.join(', ')}`
            });
        }
        
        next();
    },

    dateRange: (req, res, next) => {
        const { periodStart, periodEnd } = req.query;
        
        if (periodStart && isNaN(Date.parse(periodStart))) {
            return res.status(400).json({
                success: false,
                message: 'periodStart deve ser uma data válida'
            });
        }
        
        if (periodEnd && isNaN(Date.parse(periodEnd))) {
            return res.status(400).json({
                success: false,
                message: 'periodEnd deve ser uma data válida'
            });
        }
        
        if (periodStart && periodEnd && new Date(periodStart) > new Date(periodEnd)) {
            return res.status(400).json({
                success: false,
                message: 'periodStart deve ser anterior a periodEnd'
            });
        }
        
        next();
    }
};

// Middleware de cache de resposta (simples)
const cacheResponse = (ttlSeconds = 300) => {
    const cache = new Map();
    
    return (req, res, next) => {
        // Apenas para métodos GET
        if (req.method !== 'GET') {
            return next();
        }
        
        const cacheKey = `${req.originalUrl}_${JSON.stringify(req.query)}`;
        const cached = cache.get(cacheKey);
        
        if (cached && Date.now() < cached.expiry) {
            return res.json(cached.data);
        }
        
        // Interceptar res.json para cachear a resposta
        const originalJson = res.json;
        res.json = function(data) {
            if (res.statusCode === 200 && data.success) {
                cache.set(cacheKey, {
                    data,
                    expiry: Date.now() + (ttlSeconds * 1000)
                });
            }
            return originalJson.call(this, data);
        };
        
        next();
    };
};

module.exports = {
    validate,
    schemas,
    authenticate,
    rateLimits,
    requestLogger,
    errorHandler,
    corsHandler,
    validateParams,
    validateQuery,
    cacheResponse
};

