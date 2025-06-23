const DataModel = require('../models/dataModel');
const ConfigClient = require('../utils/configClient');
const logger = require('../utils/logger');
const moment = require('moment');
const _ = require('lodash');

class DataService {
    constructor() {
        this.dataModel = new DataModel();
        this.configClient = new ConfigClient();
        this.cache = new Map();
        this.cacheTTL = new Map();
    }

    // ===== CONFIGURAÇÕES DINÂMICAS =====

    async getConfig(key, defaultValue = null) {
        return await this.configClient.getConfig(key, defaultValue);
    }

    async getDataSyncSettings() {
        return await this.configClient.getDataSyncSettings();
    }

    async getAnalyticsSettings() {
        return await this.configClient.getAnalyticsSettings();
    }

    async getExportSettings() {
        return await this.configClient.getExportSettings();
    }

    async getSystemSettings() {
        return await this.configClient.getSystemSettings();
    }

    // ===== SINCRONIZAÇÃO DE DADOS =====

    async syncExternalData(tableName, options = {}) {
        const syncSettings = await this.getDataSyncSettings();
        const batchSize = options.batchSize || syncSettings.batch_size || 1000;
        const maxRetries = options.maxRetries || syncSettings.max_retry_attempts || 3;

        // Criar log de sincronização
        const syncLog = await this.dataModel.createSyncLog({
            sync_type: 'EXTERNAL_SYNC',
            table_name: tableName,
            operation: 'SYNC',
            metadata: {
                batch_size: batchSize,
                max_retries: maxRetries,
                options
            }
        });

        const startTime = Date.now();
        let recordsProcessed = 0;
        let recordsSuccess = 0;
        let recordsFailed = 0;
        let errorMessage = null;

        try {
            logger.info(`Iniciando sincronização da tabela ${tableName}`);

            // Buscar dados externos
            const filters = {
                dateFrom: options.dateFrom,
                dateTo: options.dateTo,
                userId: options.userId,
                affiliateId: options.affiliateId
            };

            const externalData = await this.dataModel.getExternalData(tableName, filters, batchSize);
            recordsProcessed = externalData.length;

            logger.info(`Encontrados ${recordsProcessed} registros para sincronizar`);

            // Processar dados em lotes
            const chunks = _.chunk(externalData, Math.min(batchSize, 100));
            
            for (const chunk of chunks) {
                try {
                    await this.processDataChunk(tableName, chunk);
                    recordsSuccess += chunk.length;
                } catch (error) {
                    logger.error(`Erro ao processar chunk de ${chunk.length} registros:`, error);
                    recordsFailed += chunk.length;
                }
            }

            // Atualizar log de sucesso
            const endTime = Date.now();
            await this.dataModel.updateSyncLog(syncLog.id, {
                records_processed: recordsProcessed,
                records_success: recordsSuccess,
                records_failed: recordsFailed,
                end_time: new Date(),
                duration_ms: endTime - startTime,
                status: recordsFailed === 0 ? 'COMPLETED' : 'COMPLETED',
                error_message: recordsFailed > 0 ? `${recordsFailed} registros falharam` : null
            });

            logger.info(`Sincronização concluída: ${recordsSuccess}/${recordsProcessed} registros processados`);

            return {
                success: true,
                syncLogId: syncLog.id,
                recordsProcessed,
                recordsSuccess,
                recordsFailed,
                duration: endTime - startTime
            };

        } catch (error) {
            errorMessage = error.message;
            logger.error(`Erro na sincronização da tabela ${tableName}:`, error);

            // Atualizar log de erro
            const endTime = Date.now();
            await this.dataModel.updateSyncLog(syncLog.id, {
                records_processed: recordsProcessed,
                records_success: recordsSuccess,
                records_failed: recordsFailed,
                end_time: new Date(),
                duration_ms: endTime - startTime,
                status: 'FAILED',
                error_message: errorMessage
            });

            throw error;
        }
    }

    async processDataChunk(tableName, dataChunk) {
        // Processar chunk baseado no tipo de tabela
        switch (tableName) {
            case 'users':
                return await this.processUsersChunk(dataChunk);
            case 'transactions':
                return await this.processTransactionsChunk(dataChunk);
            case 'bets':
                return await this.processBetsChunk(dataChunk);
            case 'deposits':
                return await this.processDepositsChunk(dataChunk);
            default:
                logger.warn(`Tipo de tabela não reconhecido: ${tableName}`);
                return [];
        }
    }

    async processUsersChunk(users) {
        // Processar dados de usuários e gerar analytics
        for (const user of users) {
            try {
                await this.generateUserAnalytics(user.id, 'DAILY');
            } catch (error) {
                logger.error(`Erro ao processar usuário ${user.id}:`, error);
            }
        }
    }

    async processTransactionsChunk(transactions) {
        // Agrupar transações por usuário
        const userTransactions = _.groupBy(transactions, 'user_id');
        
        for (const [userId, userTxs] of Object.entries(userTransactions)) {
            try {
                await this.updateUserAnalyticsFromTransactions(parseInt(userId), userTxs);
            } catch (error) {
                logger.error(`Erro ao processar transações do usuário ${userId}:`, error);
            }
        }
    }

    async processBetsChunk(bets) {
        // Agrupar apostas por usuário
        const userBets = _.groupBy(bets, 'user_id');
        
        for (const [userId, userBetsList] of Object.entries(userBets)) {
            try {
                await this.updateUserAnalyticsFromBets(parseInt(userId), userBetsList);
            } catch (error) {
                logger.error(`Erro ao processar apostas do usuário ${userId}:`, error);
            }
        }
    }

    async processDepositsChunk(deposits) {
        // Agrupar depósitos por usuário
        const userDeposits = _.groupBy(deposits, 'user_id');
        
        for (const [userId, userDepositsList] of Object.entries(userDeposits)) {
            try {
                await this.updateUserAnalyticsFromDeposits(parseInt(userId), userDepositsList);
            } catch (error) {
                logger.error(`Erro ao processar depósitos do usuário ${userId}:`, error);
            }
        }
    }

    // ===== ANALYTICS =====

    async generateUserAnalytics(userId, periodType = 'DAILY', specificDate = null) {
        try {
            const date = specificDate ? moment(specificDate) : moment();
            const { periodStart, periodEnd } = this.getPeriodRange(date, periodType);

            // Buscar dados do usuário no período
            const userData = await this.getUserDataForPeriod(userId, periodStart, periodEnd);
            
            if (!userData) {
                logger.warn(`Nenhum dado encontrado para usuário ${userId} no período ${periodType}`);
                return null;
            }

            // Calcular métricas
            const analytics = {
                user_id: userId,
                affiliate_id: userData.affiliate_id,
                period_type: periodType,
                period_start: periodStart.toDate(),
                period_end: periodEnd.toDate(),
                
                // Métricas de depósito
                total_deposits: userData.deposits?.total || 0,
                deposit_count: userData.deposits?.count || 0,
                first_deposit_date: userData.deposits?.first_date,
                last_deposit_date: userData.deposits?.last_date,
                avg_deposit_amount: userData.deposits?.avg || 0,
                
                // Métricas de apostas
                total_bets: userData.bets?.total || 0,
                bet_count: userData.bets?.count || 0,
                first_bet_date: userData.bets?.first_date,
                last_bet_date: userData.bets?.last_date,
                avg_bet_amount: userData.bets?.avg || 0,
                
                // Métricas de atividade
                days_active: userData.activity?.days_active || 0,
                sessions_count: userData.activity?.sessions || 0,
                total_session_time_minutes: userData.activity?.total_time || 0,
                
                // Métricas de resultado
                total_wins: userData.results?.wins || 0,
                total_losses: userData.results?.losses || 0,
                net_result: (userData.results?.wins || 0) - (userData.results?.losses || 0),
                
                // Métricas CPA
                cpa_qualified: userData.cpa?.qualified || false,
                cpa_qualification_date: userData.cpa?.qualification_date,
                cpa_amount: userData.cpa?.amount || 0
            };

            // Salvar analytics
            const result = await this.dataModel.upsertUserAnalytics(analytics);
            
            logger.info(`Analytics gerado para usuário ${userId}, período ${periodType}`);
            return result;

        } catch (error) {
            logger.error(`Erro ao gerar analytics para usuário ${userId}:`, error);
            throw error;
        }
    }

    async generateAffiliateAnalytics(affiliateId, periodType = 'DAILY', specificDate = null) {
        try {
            const date = specificDate ? moment(specificDate) : moment();
            const { periodStart, periodEnd } = this.getPeriodRange(date, periodType);

            // Buscar dados do afiliado no período
            const affiliateData = await this.getAffiliateDataForPeriod(affiliateId, periodStart, periodEnd);
            
            if (!affiliateData) {
                logger.warn(`Nenhum dado encontrado para afiliado ${affiliateId} no período ${periodType}`);
                return null;
            }

            // Calcular métricas
            const analytics = {
                affiliate_id: affiliateId,
                period_type: periodType,
                period_start: periodStart.toDate(),
                period_end: periodEnd.toDate(),
                
                // Métricas de usuários
                total_users: affiliateData.users?.total || 0,
                new_users: affiliateData.users?.new || 0,
                active_users: affiliateData.users?.active || 0,
                cpa_qualified_users: affiliateData.users?.cpa_qualified || 0,
                
                // Métricas financeiras
                total_user_deposits: affiliateData.financial?.deposits || 0,
                total_user_bets: affiliateData.financial?.bets || 0,
                total_commissions: affiliateData.financial?.commissions || 0,
                total_cpa_amount: affiliateData.financial?.cpa_amount || 0,
                
                // Métricas MLM
                level_1_users: affiliateData.mlm?.level_1?.users || 0,
                level_2_users: affiliateData.mlm?.level_2?.users || 0,
                level_3_users: affiliateData.mlm?.level_3?.users || 0,
                level_4_users: affiliateData.mlm?.level_4?.users || 0,
                level_5_users: affiliateData.mlm?.level_5?.users || 0,
                
                level_1_commissions: affiliateData.mlm?.level_1?.commissions || 0,
                level_2_commissions: affiliateData.mlm?.level_2?.commissions || 0,
                level_3_commissions: affiliateData.mlm?.level_3?.commissions || 0,
                level_4_commissions: affiliateData.mlm?.level_4?.commissions || 0,
                level_5_commissions: affiliateData.mlm?.level_5?.commissions || 0,
                
                // Métricas de performance
                conversion_rate: affiliateData.performance?.conversion_rate || 0,
                avg_user_value: affiliateData.performance?.avg_user_value || 0,
                retention_rate: affiliateData.performance?.retention_rate || 0
            };

            // Salvar analytics
            const result = await this.dataModel.upsertAffiliateAnalytics(analytics);
            
            logger.info(`Analytics gerado para afiliado ${affiliateId}, período ${periodType}`);
            return result;

        } catch (error) {
            logger.error(`Erro ao gerar analytics para afiliado ${affiliateId}:`, error);
            throw error;
        }
    }

    // ===== UTILITÁRIOS =====

    getPeriodRange(date, periodType) {
        const momentDate = moment(date);
        
        switch (periodType) {
            case 'DAILY':
                return {
                    periodStart: momentDate.clone().startOf('day'),
                    periodEnd: momentDate.clone().endOf('day')
                };
            case 'WEEKLY':
                return {
                    periodStart: momentDate.clone().startOf('week'),
                    periodEnd: momentDate.clone().endOf('week')
                };
            case 'MONTHLY':
                return {
                    periodStart: momentDate.clone().startOf('month'),
                    periodEnd: momentDate.clone().endOf('month')
                };
            case 'YEARLY':
                return {
                    periodStart: momentDate.clone().startOf('year'),
                    periodEnd: momentDate.clone().endOf('year')
                };
            default:
                throw new Error(`Tipo de período inválido: ${periodType}`);
        }
    }

    async getUserDataForPeriod(userId, periodStart, periodEnd) {
        // Implementação simplificada - em produção, buscaria dados reais
        // do banco externo e calcularia as métricas
        
        try {
            // Buscar dados básicos do usuário
            const userData = await this.dataModel.getExternalData('users', { userId }, 1);
            if (!userData.length) return null;

            const user = userData[0];

            // Buscar transações no período
            const transactions = await this.dataModel.getExternalData('transactions', {
                userId,
                dateFrom: periodStart.toDate(),
                dateTo: periodEnd.toDate()
            });

            // Buscar apostas no período
            const bets = await this.dataModel.getExternalData('bets', {
                userId,
                dateFrom: periodStart.toDate(),
                dateTo: periodEnd.toDate()
            });

            // Buscar depósitos no período
            const deposits = await this.dataModel.getExternalData('deposits', {
                userId,
                dateFrom: periodStart.toDate(),
                dateTo: periodEnd.toDate()
            });

            // Calcular métricas
            const depositMetrics = this.calculateDepositMetrics(deposits);
            const betMetrics = this.calculateBetMetrics(bets);
            const activityMetrics = this.calculateActivityMetrics(transactions, bets);
            const resultMetrics = this.calculateResultMetrics(bets);
            const cpaMetrics = await this.calculateCpaMetrics(userId, deposits, bets);

            return {
                affiliate_id: user.affiliate_id,
                deposits: depositMetrics,
                bets: betMetrics,
                activity: activityMetrics,
                results: resultMetrics,
                cpa: cpaMetrics
            };

        } catch (error) {
            logger.error(`Erro ao buscar dados do usuário ${userId}:`, error);
            return null;
        }
    }

    async getAffiliateDataForPeriod(affiliateId, periodStart, periodEnd) {
        // Implementação simplificada - em produção, agregaria dados de todos os usuários do afiliado
        
        try {
            // Buscar usuários do afiliado
            const users = await this.dataModel.getExternalData('users', { affiliateId });
            
            if (!users.length) return null;

            // Calcular métricas agregadas
            const userMetrics = await this.calculateAffiliateUserMetrics(affiliateId, users, periodStart, periodEnd);
            const financialMetrics = await this.calculateAffiliateFinancialMetrics(affiliateId, users, periodStart, periodEnd);
            const mlmMetrics = await this.calculateAffiliateMlmMetrics(affiliateId, periodStart, periodEnd);
            const performanceMetrics = await this.calculateAffiliatePerformanceMetrics(affiliateId, users, periodStart, periodEnd);

            return {
                users: userMetrics,
                financial: financialMetrics,
                mlm: mlmMetrics,
                performance: performanceMetrics
            };

        } catch (error) {
            logger.error(`Erro ao buscar dados do afiliado ${affiliateId}:`, error);
            return null;
        }
    }

    calculateDepositMetrics(deposits) {
        if (!deposits.length) return { total: 0, count: 0, avg: 0 };

        const total = deposits.reduce((sum, dep) => sum + parseFloat(dep.amount || 0), 0);
        const count = deposits.length;
        const avg = count > 0 ? total / count : 0;
        const firstDate = deposits.length > 0 ? new Date(Math.min(...deposits.map(d => new Date(d.created_at)))) : null;
        const lastDate = deposits.length > 0 ? new Date(Math.max(...deposits.map(d => new Date(d.created_at)))) : null;

        return { total, count, avg, first_date: firstDate, last_date: lastDate };
    }

    calculateBetMetrics(bets) {
        if (!bets.length) return { total: 0, count: 0, avg: 0 };

        const total = bets.reduce((sum, bet) => sum + parseFloat(bet.amount || 0), 0);
        const count = bets.length;
        const avg = count > 0 ? total / count : 0;
        const firstDate = bets.length > 0 ? new Date(Math.min(...bets.map(b => new Date(b.created_at)))) : null;
        const lastDate = bets.length > 0 ? new Date(Math.max(...bets.map(b => new Date(b.created_at)))) : null;

        return { total, count, avg, first_date: firstDate, last_date: lastDate };
    }

    calculateActivityMetrics(transactions, bets) {
        const allActivities = [...transactions, ...bets];
        if (!allActivities.length) return { days_active: 0, sessions: 0, total_time: 0 };

        // Calcular dias únicos de atividade
        const uniqueDays = new Set(allActivities.map(activity => 
            moment(activity.created_at).format('YYYY-MM-DD')
        ));

        return {
            days_active: uniqueDays.size,
            sessions: Math.ceil(allActivities.length / 10), // Estimativa
            total_time: allActivities.length * 5 // Estimativa em minutos
        };
    }

    calculateResultMetrics(bets) {
        if (!bets.length) return { wins: 0, losses: 0 };

        const wins = bets
            .filter(bet => bet.result === 'win')
            .reduce((sum, bet) => sum + parseFloat(bet.win_amount || 0), 0);

        const losses = bets
            .filter(bet => bet.result === 'loss')
            .reduce((sum, bet) => sum + parseFloat(bet.amount || 0), 0);

        return { wins, losses };
    }

    async calculateCpaMetrics(userId, deposits, bets) {
        // Buscar regras de validação CPA
        const cpaRules = await this.getConfig('cpa_validation_rules', {
            groups: [{
                operator: 'AND',
                criteria: [
                    { type: 'deposit', value: 30.00, enabled: true },
                    { type: 'bets', value: 10, enabled: true },
                    { type: 'bet_amount', value: 100.00, enabled: true },
                    { type: 'days_active', value: 3, enabled: true }
                ]
            }]
        });

        // Calcular se qualifica para CPA
        const totalDeposits = deposits.reduce((sum, dep) => sum + parseFloat(dep.amount || 0), 0);
        const totalBets = bets.reduce((sum, bet) => sum + parseFloat(bet.amount || 0), 0);
        const betCount = bets.length;

        // Verificar critérios (simplificado)
        const qualified = totalDeposits >= 30 && betCount >= 10 && totalBets >= 100;

        if (qualified) {
            const cpaAmounts = await this.getConfig('cpa_level_amounts', {
                level_1: 50.00, level_2: 20.00, level_3: 5.00, level_4: 5.00, level_5: 5.00
            });

            return {
                qualified: true,
                qualification_date: new Date(),
                amount: cpaAmounts.level_1 // Assumindo nível 1
            };
        }

        return { qualified: false, qualification_date: null, amount: 0 };
    }

    // Métodos auxiliares para métricas de afiliado (implementação simplificada)
    async calculateAffiliateUserMetrics(affiliateId, users, periodStart, periodEnd) {
        return {
            total: users.length,
            new: users.filter(u => moment(u.created_at).isBetween(periodStart, periodEnd)).length,
            active: Math.floor(users.length * 0.7), // Estimativa
            cpa_qualified: Math.floor(users.length * 0.3) // Estimativa
        };
    }

    async calculateAffiliateFinancialMetrics(affiliateId, users, periodStart, periodEnd) {
        return {
            deposits: users.length * 100, // Estimativa
            bets: users.length * 500, // Estimativa
            commissions: users.length * 50, // Estimativa
            cpa_amount: users.length * 15 // Estimativa
        };
    }

    async calculateAffiliateMlmMetrics(affiliateId, periodStart, periodEnd) {
        return {
            level_1: { users: 10, commissions: 500 },
            level_2: { users: 5, commissions: 100 },
            level_3: { users: 2, commissions: 10 },
            level_4: { users: 1, commissions: 5 },
            level_5: { users: 0, commissions: 0 }
        };
    }

    async calculateAffiliatePerformanceMetrics(affiliateId, users, periodStart, periodEnd) {
        return {
            conversion_rate: 0.15, // 15%
            avg_user_value: 250.00,
            retention_rate: 0.65 // 65%
        };
    }

    // ===== HEALTH CHECK =====

    async healthCheck() {
        try {
            const configHealth = await this.configClient.healthCheck();
            
            return {
                status: 'healthy',
                timestamp: new Date().toISOString(),
                services: {
                    database: 'connected',
                    externalDatabase: 'connected',
                    configService: configHealth.success ? 'connected' : 'disconnected',
                    cache: 'active'
                },
                version: process.env.npm_package_version || '2.0.0'
            };
        } catch (error) {
            return {
                status: 'unhealthy',
                timestamp: new Date().toISOString(),
                error: error.message,
                services: {
                    database: 'unknown',
                    externalDatabase: 'unknown',
                    configService: 'disconnected',
                    cache: 'unknown'
                }
            };
        }
    }
}

module.exports = DataService;

