const { Pool } = require('pg');
const logger = require('../utils/logger');

class DataModel {
    constructor() {
        this.pool = new Pool({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            database: process.env.DB_NAME,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            ssl: process.env.DB_SSL === 'true',
            min: parseInt(process.env.DB_POOL_MIN) || 2,
            max: parseInt(process.env.DB_POOL_MAX) || 20,
        });

        // Pool para banco externo (operação)
        this.externalPool = new Pool({
            host: process.env.EXTERNAL_DB_HOST,
            port: process.env.EXTERNAL_DB_PORT,
            database: process.env.EXTERNAL_DB_NAME,
            user: process.env.EXTERNAL_DB_USER,
            password: process.env.EXTERNAL_DB_PASSWORD,
            ssl: process.env.EXTERNAL_DB_SSL === 'true',
            min: 1,
            max: 5,
        });
    }

    // ===== SYNC LOGS =====

    async createSyncLog(syncData) {
        const client = await this.pool.connect();
        try {
            const query = `
                INSERT INTO data_sync_logs 
                (sync_type, table_name, operation, metadata)
                VALUES ($1, $2, $3, $4)
                RETURNING *
            `;
            
            const values = [
                syncData.sync_type,
                syncData.table_name,
                syncData.operation,
                JSON.stringify(syncData.metadata || {})
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao criar log de sincronização:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async updateSyncLog(logId, updateData) {
        const client = await this.pool.connect();
        try {
            const query = `
                UPDATE data_sync_logs 
                SET 
                    records_processed = $1,
                    records_success = $2,
                    records_failed = $3,
                    end_time = $4,
                    duration_ms = $5,
                    status = $6,
                    error_message = $7
                WHERE id = $8
                RETURNING *
            `;
            
            const values = [
                updateData.records_processed,
                updateData.records_success,
                updateData.records_failed,
                updateData.end_time || new Date(),
                updateData.duration_ms,
                updateData.status,
                updateData.error_message,
                logId
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao atualizar log de sincronização:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ===== USER ANALYTICS =====

    async upsertUserAnalytics(analyticsData) {
        const client = await this.pool.connect();
        try {
            const query = `
                INSERT INTO user_analytics 
                (user_id, affiliate_id, period_type, period_start, period_end,
                 total_deposits, deposit_count, first_deposit_date, last_deposit_date, avg_deposit_amount,
                 total_bets, bet_count, first_bet_date, last_bet_date, avg_bet_amount,
                 days_active, sessions_count, total_session_time_minutes,
                 total_wins, total_losses, net_result,
                 cpa_qualified, cpa_qualification_date, cpa_amount)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
                ON CONFLICT (user_id, period_type, period_start)
                DO UPDATE SET
                    affiliate_id = EXCLUDED.affiliate_id,
                    period_end = EXCLUDED.period_end,
                    total_deposits = EXCLUDED.total_deposits,
                    deposit_count = EXCLUDED.deposit_count,
                    first_deposit_date = EXCLUDED.first_deposit_date,
                    last_deposit_date = EXCLUDED.last_deposit_date,
                    avg_deposit_amount = EXCLUDED.avg_deposit_amount,
                    total_bets = EXCLUDED.total_bets,
                    bet_count = EXCLUDED.bet_count,
                    first_bet_date = EXCLUDED.first_bet_date,
                    last_bet_date = EXCLUDED.last_bet_date,
                    avg_bet_amount = EXCLUDED.avg_bet_amount,
                    days_active = EXCLUDED.days_active,
                    sessions_count = EXCLUDED.sessions_count,
                    total_session_time_minutes = EXCLUDED.total_session_time_minutes,
                    total_wins = EXCLUDED.total_wins,
                    total_losses = EXCLUDED.total_losses,
                    net_result = EXCLUDED.net_result,
                    cpa_qualified = EXCLUDED.cpa_qualified,
                    cpa_qualification_date = EXCLUDED.cpa_qualification_date,
                    cpa_amount = EXCLUDED.cpa_amount,
                    last_updated = CURRENT_TIMESTAMP
                RETURNING *
            `;
            
            const values = [
                analyticsData.user_id,
                analyticsData.affiliate_id,
                analyticsData.period_type,
                analyticsData.period_start,
                analyticsData.period_end,
                analyticsData.total_deposits || 0,
                analyticsData.deposit_count || 0,
                analyticsData.first_deposit_date,
                analyticsData.last_deposit_date,
                analyticsData.avg_deposit_amount || 0,
                analyticsData.total_bets || 0,
                analyticsData.bet_count || 0,
                analyticsData.first_bet_date,
                analyticsData.last_bet_date,
                analyticsData.avg_bet_amount || 0,
                analyticsData.days_active || 0,
                analyticsData.sessions_count || 0,
                analyticsData.total_session_time_minutes || 0,
                analyticsData.total_wins || 0,
                analyticsData.total_losses || 0,
                analyticsData.net_result || 0,
                analyticsData.cpa_qualified || false,
                analyticsData.cpa_qualification_date,
                analyticsData.cpa_amount || 0
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao inserir/atualizar analytics de usuário:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async getUserAnalytics(userId, periodType = null, periodStart = null, periodEnd = null) {
        const client = await this.pool.connect();
        try {
            let query = `
                SELECT * FROM user_analytics 
                WHERE user_id = $1
            `;
            
            const params = [userId];
            let paramIndex = 2;

            if (periodType) {
                query += ` AND period_type = $${paramIndex}`;
                params.push(periodType);
                paramIndex++;
            }

            if (periodStart) {
                query += ` AND period_start >= $${paramIndex}`;
                params.push(periodStart);
                paramIndex++;
            }

            if (periodEnd) {
                query += ` AND period_end <= $${paramIndex}`;
                params.push(periodEnd);
                paramIndex++;
            }

            query += ' ORDER BY period_start DESC';

            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
            logger.error('Erro ao buscar analytics de usuário:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ===== AFFILIATE ANALYTICS =====

    async upsertAffiliateAnalytics(analyticsData) {
        const client = await this.pool.connect();
        try {
            const query = `
                INSERT INTO affiliate_analytics 
                (affiliate_id, period_type, period_start, period_end,
                 total_users, new_users, active_users, cpa_qualified_users,
                 total_user_deposits, total_user_bets, total_commissions, total_cpa_amount,
                 level_1_users, level_2_users, level_3_users, level_4_users, level_5_users,
                 level_1_commissions, level_2_commissions, level_3_commissions, level_4_commissions, level_5_commissions,
                 conversion_rate, avg_user_value, retention_rate)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
                ON CONFLICT (affiliate_id, period_type, period_start)
                DO UPDATE SET
                    period_end = EXCLUDED.period_end,
                    total_users = EXCLUDED.total_users,
                    new_users = EXCLUDED.new_users,
                    active_users = EXCLUDED.active_users,
                    cpa_qualified_users = EXCLUDED.cpa_qualified_users,
                    total_user_deposits = EXCLUDED.total_user_deposits,
                    total_user_bets = EXCLUDED.total_user_bets,
                    total_commissions = EXCLUDED.total_commissions,
                    total_cpa_amount = EXCLUDED.total_cpa_amount,
                    level_1_users = EXCLUDED.level_1_users,
                    level_2_users = EXCLUDED.level_2_users,
                    level_3_users = EXCLUDED.level_3_users,
                    level_4_users = EXCLUDED.level_4_users,
                    level_5_users = EXCLUDED.level_5_users,
                    level_1_commissions = EXCLUDED.level_1_commissions,
                    level_2_commissions = EXCLUDED.level_2_commissions,
                    level_3_commissions = EXCLUDED.level_3_commissions,
                    level_4_commissions = EXCLUDED.level_4_commissions,
                    level_5_commissions = EXCLUDED.level_5_commissions,
                    conversion_rate = EXCLUDED.conversion_rate,
                    avg_user_value = EXCLUDED.avg_user_value,
                    retention_rate = EXCLUDED.retention_rate,
                    last_updated = CURRENT_TIMESTAMP
                RETURNING *
            `;
            
            const values = [
                analyticsData.affiliate_id,
                analyticsData.period_type,
                analyticsData.period_start,
                analyticsData.period_end,
                analyticsData.total_users || 0,
                analyticsData.new_users || 0,
                analyticsData.active_users || 0,
                analyticsData.cpa_qualified_users || 0,
                analyticsData.total_user_deposits || 0,
                analyticsData.total_user_bets || 0,
                analyticsData.total_commissions || 0,
                analyticsData.total_cpa_amount || 0,
                analyticsData.level_1_users || 0,
                analyticsData.level_2_users || 0,
                analyticsData.level_3_users || 0,
                analyticsData.level_4_users || 0,
                analyticsData.level_5_users || 0,
                analyticsData.level_1_commissions || 0,
                analyticsData.level_2_commissions || 0,
                analyticsData.level_3_commissions || 0,
                analyticsData.level_4_commissions || 0,
                analyticsData.level_5_commissions || 0,
                analyticsData.conversion_rate || 0,
                analyticsData.avg_user_value || 0,
                analyticsData.retention_rate || 0
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao inserir/atualizar analytics de afiliado:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async getAffiliateAnalytics(affiliateId, periodType = null, periodStart = null, periodEnd = null) {
        const client = await this.pool.connect();
        try {
            let query = `
                SELECT * FROM affiliate_analytics 
                WHERE affiliate_id = $1
            `;
            
            const params = [affiliateId];
            let paramIndex = 2;

            if (periodType) {
                query += ` AND period_type = $${paramIndex}`;
                params.push(periodType);
                paramIndex++;
            }

            if (periodStart) {
                query += ` AND period_start >= $${paramIndex}`;
                params.push(periodStart);
                paramIndex++;
            }

            if (periodEnd) {
                query += ` AND period_end <= $${paramIndex}`;
                params.push(periodEnd);
                paramIndex++;
            }

            query += ' ORDER BY period_start DESC';

            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
            logger.error('Erro ao buscar analytics de afiliado:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ===== DATA EXPORTS =====

    async createDataExport(exportData) {
        const client = await this.pool.connect();
        try {
            const query = `
                INSERT INTO data_exports 
                (export_type, format, filename, filters, date_range_start, date_range_end, requested_by, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *
            `;
            
            const expiresAt = new Date();
            expiresAt.setDate(expiresAt.getDate() + (exportData.retention_days || 7));
            
            const values = [
                exportData.export_type,
                exportData.format,
                exportData.filename,
                JSON.stringify(exportData.filters || {}),
                exportData.date_range_start,
                exportData.date_range_end,
                exportData.requested_by,
                expiresAt
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao criar exportação:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async updateDataExport(exportId, updateData) {
        const client = await this.pool.connect();
        try {
            const query = `
                UPDATE data_exports 
                SET 
                    status = $1,
                    progress_percentage = $2,
                    records_total = $3,
                    records_exported = $4,
                    file_path = $5,
                    file_size_bytes = $6,
                    started_at = $7,
                    completed_at = $8,
                    error_message = $9
                WHERE id = $10
                RETURNING *
            `;
            
            const values = [
                updateData.status,
                updateData.progress_percentage,
                updateData.records_total,
                updateData.records_exported,
                updateData.file_path,
                updateData.file_size_bytes,
                updateData.started_at,
                updateData.completed_at,
                updateData.error_message,
                exportId
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao atualizar exportação:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async getDataExport(exportId) {
        const client = await this.pool.connect();
        try {
            const query = 'SELECT * FROM data_exports WHERE id = $1';
            const result = await client.query(query, [exportId]);
            return result.rows[0] || null;
        } catch (error) {
            logger.error('Erro ao buscar exportação:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async getDataExports(filters = {}) {
        const client = await this.pool.connect();
        try {
            let query = 'SELECT * FROM data_exports WHERE 1=1';
            const params = [];
            let paramIndex = 1;

            if (filters.requested_by) {
                query += ` AND requested_by = $${paramIndex}`;
                params.push(filters.requested_by);
                paramIndex++;
            }

            if (filters.status) {
                query += ` AND status = $${paramIndex}`;
                params.push(filters.status);
                paramIndex++;
            }

            if (filters.export_type) {
                query += ` AND export_type = $${paramIndex}`;
                params.push(filters.export_type);
                paramIndex++;
            }

            query += ' ORDER BY created_at DESC';

            if (filters.limit) {
                query += ` LIMIT $${paramIndex}`;
                params.push(filters.limit);
            }

            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
            logger.error('Erro ao buscar exportações:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ===== EXTERNAL DATA ACCESS =====

    async getExternalData(tableName, filters = {}, limit = 1000) {
        const client = await this.externalPool.connect();
        try {
            let query = `SELECT * FROM ${tableName} WHERE 1=1`;
            const params = [];
            let paramIndex = 1;

            // Aplicar filtros básicos
            if (filters.dateFrom) {
                query += ` AND created_at >= $${paramIndex}`;
                params.push(filters.dateFrom);
                paramIndex++;
            }

            if (filters.dateTo) {
                query += ` AND created_at <= $${paramIndex}`;
                params.push(filters.dateTo);
                paramIndex++;
            }

            if (filters.userId) {
                query += ` AND user_id = $${paramIndex}`;
                params.push(filters.userId);
                paramIndex++;
            }

            if (filters.affiliateId) {
                query += ` AND affiliate_id = $${paramIndex}`;
                params.push(filters.affiliateId);
                paramIndex++;
            }

            query += ' ORDER BY created_at DESC';
            
            if (limit) {
                query += ` LIMIT $${paramIndex}`;
                params.push(limit);
            }

            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
            logger.error(`Erro ao buscar dados externos da tabela ${tableName}:`, error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ===== CACHE =====

    async setCache(key, data, ttlSeconds = 3600) {
        const client = await this.pool.connect();
        try {
            const expiresAt = new Date();
            expiresAt.setSeconds(expiresAt.getSeconds() + ttlSeconds);

            const query = `
                INSERT INTO data_cache (cache_key, cache_data, cache_type, ttl_seconds, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (cache_key)
                DO UPDATE SET
                    cache_data = EXCLUDED.cache_data,
                    ttl_seconds = EXCLUDED.ttl_seconds,
                    expires_at = EXCLUDED.expires_at,
                    access_count = data_cache.access_count + 1,
                    last_accessed_at = CURRENT_TIMESTAMP
                RETURNING *
            `;

            const values = [
                key,
                JSON.stringify(data),
                'general',
                ttlSeconds,
                expiresAt
            ];

            const result = await client.query(query, values);
            return result.rows[0];
        } catch (error) {
            logger.error('Erro ao salvar cache:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async getCache(key) {
        const client = await this.pool.connect();
        try {
            const query = `
                SELECT * FROM data_cache 
                WHERE cache_key = $1 AND expires_at > CURRENT_TIMESTAMP
            `;

            const result = await client.query(query, [key]);
            
            if (result.rows.length > 0) {
                const cacheEntry = result.rows[0];
                
                // Atualizar contador de acesso
                await client.query(`
                    UPDATE data_cache 
                    SET access_count = access_count + 1, last_accessed_at = CURRENT_TIMESTAMP
                    WHERE cache_key = $1
                `, [key]);

                return JSON.parse(cacheEntry.cache_data);
            }
            
            return null;
        } catch (error) {
            logger.error('Erro ao buscar cache:', error);
            return null; // Não propagar erro para não quebrar operação principal
        } finally {
            client.release();
        }
    }

    async clearExpiredCache() {
        const client = await this.pool.connect();
        try {
            const query = 'DELETE FROM data_cache WHERE expires_at <= CURRENT_TIMESTAMP';
            const result = await client.query(query);
            return result.rowCount;
        } catch (error) {
            logger.error('Erro ao limpar cache expirado:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    async close() {
        await this.pool.end();
        await this.externalPool.end();
    }
}

module.exports = DataModel;

