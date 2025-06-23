const DataService = require('../services/dataService');
const logger = require('../utils/logger');

class DataController {
    constructor() {
        this.dataService = new DataService();
    }

    // Health check
    async healthCheck(req, res) {
        try {
            const health = await this.dataService.healthCheck();
            res.status(health.status === 'healthy' ? 200 : 503).json({
                success: health.status === 'healthy',
                data: health
            });
        } catch (error) {
            logger.error('Erro no health check:', error);
            res.status(503).json({
                success: false,
                message: 'Serviço indisponível',
                error: error.message
            });
        }
    }

    // Sincronizar dados externos
    async syncExternalData(req, res) {
        try {
            const { tableName } = req.params;
            const options = req.body || {};

            if (!tableName) {
                return res.status(400).json({
                    success: false,
                    message: 'tableName é obrigatório'
                });
            }

            const result = await this.dataService.syncExternalData(tableName, options);

            res.status(200).json({
                success: true,
                message: `Sincronização da tabela ${tableName} concluída`,
                data: result
            });

        } catch (error) {
            logger.error('Erro na sincronização de dados:', error);
            res.status(500).json({
                success: false,
                message: 'Erro na sincronização de dados',
                error: error.message
            });
        }
    }

    // Gerar analytics de usuário
    async generateUserAnalytics(req, res) {
        try {
            const { userId } = req.params;
            const { periodType, specificDate } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: 'userId é obrigatório'
                });
            }

            const result = await this.dataService.generateUserAnalytics(
                parseInt(userId),
                periodType || 'DAILY',
                specificDate ? new Date(specificDate) : null
            );

            if (!result) {
                return res.status(404).json({
                    success: false,
                    message: 'Nenhum dado encontrado para o usuário no período especificado'
                });
            }

            res.status(200).json({
                success: true,
                message: 'Analytics de usuário gerado com sucesso',
                data: result
            });

        } catch (error) {
            logger.error('Erro ao gerar analytics de usuário:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao gerar analytics de usuário',
                error: error.message
            });
        }
    }

    // Gerar analytics de afiliado
    async generateAffiliateAnalytics(req, res) {
        try {
            const { affiliateId } = req.params;
            const { periodType, specificDate } = req.query;

            if (!affiliateId) {
                return res.status(400).json({
                    success: false,
                    message: 'affiliateId é obrigatório'
                });
            }

            const result = await this.dataService.generateAffiliateAnalytics(
                parseInt(affiliateId),
                periodType || 'DAILY',
                specificDate ? new Date(specificDate) : null
            );

            if (!result) {
                return res.status(404).json({
                    success: false,
                    message: 'Nenhum dado encontrado para o afiliado no período especificado'
                });
            }

            res.status(200).json({
                success: true,
                message: 'Analytics de afiliado gerado com sucesso',
                data: result
            });

        } catch (error) {
            logger.error('Erro ao gerar analytics de afiliado:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao gerar analytics de afiliado',
                error: error.message
            });
        }
    }

    // Buscar analytics de usuário
    async getUserAnalytics(req, res) {
        try {
            const { userId } = req.params;
            const { periodType, periodStart, periodEnd } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: 'userId é obrigatório'
                });
            }

            const analytics = await this.dataService.dataModel.getUserAnalytics(
                parseInt(userId),
                periodType,
                periodStart ? new Date(periodStart) : null,
                periodEnd ? new Date(periodEnd) : null
            );

            res.status(200).json({
                success: true,
                data: {
                    userId: parseInt(userId),
                    analytics,
                    totalRecords: analytics.length,
                    filters: {
                        periodType,
                        periodStart,
                        periodEnd
                    }
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar analytics de usuário:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar analytics de usuário',
                error: error.message
            });
        }
    }

    // Buscar analytics de afiliado
    async getAffiliateAnalytics(req, res) {
        try {
            const { affiliateId } = req.params;
            const { periodType, periodStart, periodEnd } = req.query;

            if (!affiliateId) {
                return res.status(400).json({
                    success: false,
                    message: 'affiliateId é obrigatório'
                });
            }

            const analytics = await this.dataService.dataModel.getAffiliateAnalytics(
                parseInt(affiliateId),
                periodType,
                periodStart ? new Date(periodStart) : null,
                periodEnd ? new Date(periodEnd) : null
            );

            res.status(200).json({
                success: true,
                data: {
                    affiliateId: parseInt(affiliateId),
                    analytics,
                    totalRecords: analytics.length,
                    filters: {
                        periodType,
                        periodStart,
                        periodEnd
                    }
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar analytics de afiliado:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar analytics de afiliado',
                error: error.message
            });
        }
    }

    // Buscar dados externos
    async getExternalData(req, res) {
        try {
            const { tableName } = req.params;
            const { dateFrom, dateTo, userId, affiliateId, limit } = req.query;

            if (!tableName) {
                return res.status(400).json({
                    success: false,
                    message: 'tableName é obrigatório'
                });
            }

            const filters = {};
            if (dateFrom) filters.dateFrom = new Date(dateFrom);
            if (dateTo) filters.dateTo = new Date(dateTo);
            if (userId) filters.userId = parseInt(userId);
            if (affiliateId) filters.affiliateId = parseInt(affiliateId);

            const data = await this.dataService.dataModel.getExternalData(
                tableName,
                filters,
                limit ? parseInt(limit) : 1000
            );

            res.status(200).json({
                success: true,
                data: {
                    tableName,
                    records: data,
                    totalRecords: data.length,
                    filters
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar dados externos:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar dados externos',
                error: error.message
            });
        }
    }

    // Criar exportação de dados
    async createDataExport(req, res) {
        try {
            const { exportType, format, filters, dateRangeStart, dateRangeEnd } = req.body;

            if (!exportType || !format) {
                return res.status(400).json({
                    success: false,
                    message: 'exportType e format são obrigatórios'
                });
            }

            const filename = `export_${exportType}_${Date.now()}.${format.toLowerCase()}`;
            
            const exportData = {
                export_type: exportType,
                format: format.toUpperCase(),
                filename,
                filters: filters || {},
                date_range_start: dateRangeStart ? new Date(dateRangeStart) : null,
                date_range_end: dateRangeEnd ? new Date(dateRangeEnd) : null,
                requested_by: req.user?.username || 'api_user'
            };

            const result = await this.dataService.dataModel.createDataExport(exportData);

            // Aqui seria iniciado o processo de exportação em background
            // Por enquanto, apenas retornamos o registro criado

            res.status(201).json({
                success: true,
                message: 'Exportação criada com sucesso',
                data: result
            });

        } catch (error) {
            logger.error('Erro ao criar exportação:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao criar exportação',
                error: error.message
            });
        }
    }

    // Buscar exportações
    async getDataExports(req, res) {
        try {
            const { status, exportType, requestedBy, limit } = req.query;

            const filters = {};
            if (status) filters.status = status;
            if (exportType) filters.export_type = exportType;
            if (requestedBy) filters.requested_by = requestedBy;
            if (limit) filters.limit = parseInt(limit);

            const exports = await this.dataService.dataModel.getDataExports(filters);

            res.status(200).json({
                success: true,
                data: {
                    exports,
                    totalExports: exports.length,
                    filters
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar exportações:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar exportações',
                error: error.message
            });
        }
    }

    // Buscar exportação específica
    async getDataExport(req, res) {
        try {
            const { exportId } = req.params;

            if (!exportId) {
                return res.status(400).json({
                    success: false,
                    message: 'exportId é obrigatório'
                });
            }

            const exportData = await this.dataService.dataModel.getDataExport(exportId);

            if (!exportData) {
                return res.status(404).json({
                    success: false,
                    message: 'Exportação não encontrada'
                });
            }

            res.status(200).json({
                success: true,
                data: exportData
            });

        } catch (error) {
            logger.error('Erro ao buscar exportação:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar exportação',
                error: error.message
            });
        }
    }

    // Buscar configurações atuais
    async getDataConfig(req, res) {
        try {
            const syncSettings = await this.dataService.getDataSyncSettings();
            const analyticsSettings = await this.dataService.getAnalyticsSettings();
            const exportSettings = await this.dataService.getExportSettings();
            const systemSettings = await this.dataService.getSystemSettings();

            res.status(200).json({
                success: true,
                data: {
                    syncSettings,
                    analyticsSettings,
                    exportSettings,
                    systemSettings,
                    timestamp: new Date().toISOString()
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar configurações:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar configurações',
                error: error.message
            });
        }
    }

    // Limpar cache expirado
    async clearExpiredCache(req, res) {
        try {
            const clearedCount = await this.dataService.dataModel.clearExpiredCache();

            res.status(200).json({
                success: true,
                message: 'Cache expirado limpo com sucesso',
                data: {
                    clearedEntries: clearedCount,
                    timestamp: new Date().toISOString()
                }
            });

        } catch (error) {
            logger.error('Erro ao limpar cache:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao limpar cache',
                error: error.message
            });
        }
    }

    // Estatísticas gerais do serviço
    async getServiceStats(req, res) {
        try {
            // Buscar estatísticas básicas
            // Em uma implementação completa, isso seria calculado do banco
            
            res.status(200).json({
                success: true,
                data: {
                    service: 'fature-data-service-v2',
                    version: process.env.npm_package_version || '2.0.0',
                    uptime: process.uptime(),
                    memory: process.memoryUsage(),
                    stats: {
                        totalSyncs: 0,
                        totalAnalytics: 0,
                        totalExports: 0,
                        cacheEntries: 0
                    },
                    integrations: {
                        configService: process.env.CONFIG_SERVICE_URL,
                        externalDatabase: process.env.EXTERNAL_DB_HOST,
                        syncEnabled: process.env.ENABLE_DATA_SYNC === 'true'
                    },
                    timestamp: new Date().toISOString()
                }
            });

        } catch (error) {
            logger.error('Erro ao buscar estatísticas:', error);
            res.status(500).json({
                success: false,
                message: 'Erro ao buscar estatísticas',
                error: error.message
            });
        }
    }
}

module.exports = DataController;

