const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function createTables() {
    const client = await pool.connect();
    
    try {
        console.log('🚀 Iniciando criação das tabelas Data Service...');

        // Criar extensão UUID se não existir
        await client.query(`
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        `);

        // Tabela de sincronização de dados
        await client.query(`
            CREATE TABLE IF NOT EXISTS data_sync_logs (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sync_type VARCHAR(50) NOT NULL,
                table_name VARCHAR(100) NOT NULL,
                operation VARCHAR(20) NOT NULL,
                records_processed INTEGER DEFAULT 0,
                records_success INTEGER DEFAULT 0,
                records_failed INTEGER DEFAULT 0,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                duration_ms INTEGER,
                status VARCHAR(20) DEFAULT 'RUNNING',
                error_message TEXT,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_sync_operation CHECK (operation IN ('SYNC', 'EXPORT', 'IMPORT', 'CLEANUP', 'AGGREGATE')),
                CONSTRAINT valid_sync_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
                CONSTRAINT positive_counts CHECK (
                    records_processed >= 0 AND 
                    records_success >= 0 AND 
                    records_failed >= 0 AND
                    records_success + records_failed <= records_processed
                )
            );
        `);

        // Tabela de dados agregados de usuários
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_analytics (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id INTEGER NOT NULL,
                affiliate_id INTEGER,
                period_type VARCHAR(20) NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                
                -- Métricas de depósito
                total_deposits DECIMAL(12,2) DEFAULT 0,
                deposit_count INTEGER DEFAULT 0,
                first_deposit_date TIMESTAMP,
                last_deposit_date TIMESTAMP,
                avg_deposit_amount DECIMAL(10,2) DEFAULT 0,
                
                -- Métricas de apostas
                total_bets DECIMAL(12,2) DEFAULT 0,
                bet_count INTEGER DEFAULT 0,
                first_bet_date TIMESTAMP,
                last_bet_date TIMESTAMP,
                avg_bet_amount DECIMAL(10,2) DEFAULT 0,
                
                -- Métricas de atividade
                days_active INTEGER DEFAULT 0,
                sessions_count INTEGER DEFAULT 0,
                total_session_time_minutes INTEGER DEFAULT 0,
                
                -- Métricas de resultado
                total_wins DECIMAL(12,2) DEFAULT 0,
                total_losses DECIMAL(12,2) DEFAULT 0,
                net_result DECIMAL(12,2) DEFAULT 0,
                
                -- Métricas CPA
                cpa_qualified BOOLEAN DEFAULT FALSE,
                cpa_qualification_date TIMESTAMP,
                cpa_amount DECIMAL(10,2) DEFAULT 0,
                
                -- Metadados
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_period_type CHECK (period_type IN ('DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY')),
                CONSTRAINT valid_period CHECK (period_end > period_start),
                CONSTRAINT positive_amounts CHECK (
                    total_deposits >= 0 AND total_bets >= 0 AND 
                    deposit_count >= 0 AND bet_count >= 0 AND
                    days_active >= 0 AND sessions_count >= 0
                ),
                UNIQUE(user_id, period_type, period_start)
            );
        `);

        // Tabela de dados agregados de afiliados
        await client.query(`
            CREATE TABLE IF NOT EXISTS affiliate_analytics (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                affiliate_id INTEGER NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                
                -- Métricas de usuários
                total_users INTEGER DEFAULT 0,
                new_users INTEGER DEFAULT 0,
                active_users INTEGER DEFAULT 0,
                cpa_qualified_users INTEGER DEFAULT 0,
                
                -- Métricas financeiras
                total_user_deposits DECIMAL(12,2) DEFAULT 0,
                total_user_bets DECIMAL(12,2) DEFAULT 0,
                total_commissions DECIMAL(12,2) DEFAULT 0,
                total_cpa_amount DECIMAL(12,2) DEFAULT 0,
                
                -- Métricas MLM
                level_1_users INTEGER DEFAULT 0,
                level_2_users INTEGER DEFAULT 0,
                level_3_users INTEGER DEFAULT 0,
                level_4_users INTEGER DEFAULT 0,
                level_5_users INTEGER DEFAULT 0,
                
                level_1_commissions DECIMAL(12,2) DEFAULT 0,
                level_2_commissions DECIMAL(12,2) DEFAULT 0,
                level_3_commissions DECIMAL(12,2) DEFAULT 0,
                level_4_commissions DECIMAL(12,2) DEFAULT 0,
                level_5_commissions DECIMAL(12,2) DEFAULT 0,
                
                -- Métricas de performance
                conversion_rate DECIMAL(5,4) DEFAULT 0,
                avg_user_value DECIMAL(10,2) DEFAULT 0,
                retention_rate DECIMAL(5,4) DEFAULT 0,
                
                -- Metadados
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_period_type CHECK (period_type IN ('DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY')),
                CONSTRAINT valid_period CHECK (period_end > period_start),
                CONSTRAINT positive_counts CHECK (
                    total_users >= 0 AND new_users >= 0 AND active_users >= 0 AND
                    level_1_users >= 0 AND level_2_users >= 0 AND level_3_users >= 0 AND
                    level_4_users >= 0 AND level_5_users >= 0
                ),
                CONSTRAINT valid_rates CHECK (
                    conversion_rate >= 0 AND conversion_rate <= 1 AND
                    retention_rate >= 0 AND retention_rate <= 1
                ),
                UNIQUE(affiliate_id, period_type, period_start)
            );
        `);

        // Tabela de exportações
        await client.query(`
            CREATE TABLE IF NOT EXISTS data_exports (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                export_type VARCHAR(50) NOT NULL,
                format VARCHAR(10) NOT NULL,
                filename VARCHAR(255) NOT NULL,
                file_path VARCHAR(500),
                file_size_bytes BIGINT DEFAULT 0,
                
                -- Filtros aplicados
                filters JSONB DEFAULT '{}',
                date_range_start TIMESTAMP,
                date_range_end TIMESTAMP,
                
                -- Status da exportação
                status VARCHAR(20) DEFAULT 'PENDING',
                progress_percentage INTEGER DEFAULT 0,
                records_total INTEGER DEFAULT 0,
                records_exported INTEGER DEFAULT 0,
                
                -- Timing
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                expires_at TIMESTAMP,
                
                -- Metadados
                requested_by VARCHAR(100),
                download_count INTEGER DEFAULT 0,
                last_downloaded_at TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_export_format CHECK (format IN ('CSV', 'JSON', 'XLSX', 'PDF')),
                CONSTRAINT valid_export_status CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'EXPIRED')),
                CONSTRAINT valid_progress CHECK (progress_percentage >= 0 AND progress_percentage <= 100),
                CONSTRAINT positive_counts CHECK (
                    file_size_bytes >= 0 AND records_total >= 0 AND 
                    records_exported >= 0 AND download_count >= 0
                )
            );
        `);

        // Tabela de cache de dados
        await client.query(`
            CREATE TABLE IF NOT EXISTS data_cache (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                cache_key VARCHAR(255) NOT NULL UNIQUE,
                cache_data JSONB NOT NULL,
                cache_type VARCHAR(50) NOT NULL,
                ttl_seconds INTEGER DEFAULT 3600,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                access_count INTEGER DEFAULT 0,
                last_accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT positive_ttl CHECK (ttl_seconds > 0),
                CONSTRAINT valid_expiry CHECK (expires_at > created_at)
            );
        `);

        // Tabela de configurações de sincronização
        await client.query(`
            CREATE TABLE IF NOT EXISTS sync_configurations (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                table_name VARCHAR(100) NOT NULL,
                sync_enabled BOOLEAN DEFAULT TRUE,
                sync_interval_minutes INTEGER DEFAULT 30,
                last_sync_at TIMESTAMP,
                next_sync_at TIMESTAMP,
                
                -- Configurações de sincronização
                batch_size INTEGER DEFAULT 1000,
                max_retries INTEGER DEFAULT 3,
                timeout_seconds INTEGER DEFAULT 300,
                
                -- Mapeamento de campos
                field_mapping JSONB DEFAULT '{}',
                filters JSONB DEFAULT '{}',
                
                -- Status
                status VARCHAR(20) DEFAULT 'ACTIVE',
                error_count INTEGER DEFAULT 0,
                last_error_message TEXT,
                last_error_at TIMESTAMP,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_sync_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'ERROR')),
                CONSTRAINT positive_values CHECK (
                    sync_interval_minutes > 0 AND batch_size > 0 AND 
                    max_retries >= 0 AND timeout_seconds > 0
                ),
                UNIQUE(table_name)
            );
        `);

        // Criar índices para performance
        const indexes = [
            'CREATE INDEX IF NOT EXISTS idx_data_sync_logs_type ON data_sync_logs(sync_type)',
            'CREATE INDEX IF NOT EXISTS idx_data_sync_logs_table ON data_sync_logs(table_name)',
            'CREATE INDEX IF NOT EXISTS idx_data_sync_logs_status ON data_sync_logs(status)',
            'CREATE INDEX IF NOT EXISTS idx_data_sync_logs_created ON data_sync_logs(created_at)',
            
            'CREATE INDEX IF NOT EXISTS idx_user_analytics_user ON user_analytics(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_user_analytics_affiliate ON user_analytics(affiliate_id)',
            'CREATE INDEX IF NOT EXISTS idx_user_analytics_period ON user_analytics(period_type, period_start)',
            'CREATE INDEX IF NOT EXISTS idx_user_analytics_cpa ON user_analytics(cpa_qualified)',
            'CREATE INDEX IF NOT EXISTS idx_user_analytics_updated ON user_analytics(last_updated)',
            
            'CREATE INDEX IF NOT EXISTS idx_affiliate_analytics_affiliate ON affiliate_analytics(affiliate_id)',
            'CREATE INDEX IF NOT EXISTS idx_affiliate_analytics_period ON affiliate_analytics(period_type, period_start)',
            'CREATE INDEX IF NOT EXISTS idx_affiliate_analytics_updated ON affiliate_analytics(last_updated)',
            
            'CREATE INDEX IF NOT EXISTS idx_data_exports_type ON data_exports(export_type)',
            'CREATE INDEX IF NOT EXISTS idx_data_exports_status ON data_exports(status)',
            'CREATE INDEX IF NOT EXISTS idx_data_exports_created ON data_exports(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_exports_expires ON data_exports(expires_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_exports_requested_by ON data_exports(requested_by)',
            
            'CREATE INDEX IF NOT EXISTS idx_data_cache_key ON data_cache(cache_key)',
            'CREATE INDEX IF NOT EXISTS idx_data_cache_type ON data_cache(cache_type)',
            'CREATE INDEX IF NOT EXISTS idx_data_cache_expires ON data_cache(expires_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_cache_accessed ON data_cache(last_accessed_at)',
            
            'CREATE INDEX IF NOT EXISTS idx_sync_configurations_table ON sync_configurations(table_name)',
            'CREATE INDEX IF NOT EXISTS idx_sync_configurations_enabled ON sync_configurations(sync_enabled)',
            'CREATE INDEX IF NOT EXISTS idx_sync_configurations_next_sync ON sync_configurations(next_sync_at)',
            'CREATE INDEX IF NOT EXISTS idx_sync_configurations_status ON sync_configurations(status)'
        ];

        for (const indexQuery of indexes) {
            await client.query(indexQuery);
        }

        console.log('✅ Tabelas Data Service criadas com sucesso!');

        // Inserir dados de teste se necessário
        await insertTestData(client);

    } catch (error) {
        console.error('❌ Erro ao criar tabelas Data Service:', error);
        throw error;
    } finally {
        client.release();
    }
}

async function insertTestData(client) {
    console.log('📋 Verificando dados de teste...');

    try {
        // Verificar se já existem dados
        const existingData = await client.query('SELECT COUNT(*) FROM sync_configurations');
        const count = parseInt(existingData.rows[0].count);

        if (count === 0) {
            console.log('📝 Inserindo configurações de sincronização padrão...');

            // Configurações padrão de sincronização
            const defaultConfigs = [
                {
                    table_name: 'users',
                    sync_interval_minutes: 30,
                    batch_size: 1000,
                    field_mapping: {
                        'id': 'user_id',
                        'affiliate_id': 'affiliate_id',
                        'created_at': 'registration_date',
                        'email': 'email',
                        'status': 'status'
                    }
                },
                {
                    table_name: 'transactions',
                    sync_interval_minutes: 15,
                    batch_size: 500,
                    field_mapping: {
                        'id': 'transaction_id',
                        'user_id': 'user_id',
                        'amount': 'amount',
                        'type': 'transaction_type',
                        'created_at': 'transaction_date'
                    }
                },
                {
                    table_name: 'bets',
                    sync_interval_minutes: 10,
                    batch_size: 2000,
                    field_mapping: {
                        'id': 'bet_id',
                        'user_id': 'user_id',
                        'amount': 'bet_amount',
                        'result': 'bet_result',
                        'created_at': 'bet_date'
                    }
                },
                {
                    table_name: 'deposits',
                    sync_interval_minutes: 5,
                    batch_size: 200,
                    field_mapping: {
                        'id': 'deposit_id',
                        'user_id': 'user_id',
                        'amount': 'deposit_amount',
                        'status': 'deposit_status',
                        'created_at': 'deposit_date'
                    }
                }
            ];

            for (const config of defaultConfigs) {
                const nextSync = new Date();
                nextSync.setMinutes(nextSync.getMinutes() + config.sync_interval_minutes);

                await client.query(`
                    INSERT INTO sync_configurations 
                    (table_name, sync_interval_minutes, batch_size, field_mapping, next_sync_at)
                    VALUES ($1, $2, $3, $4, $5)
                `, [
                    config.table_name,
                    config.sync_interval_minutes,
                    config.batch_size,
                    JSON.stringify(config.field_mapping),
                    nextSync
                ]);
            }

            console.log('✅ Configurações padrão inseridas!');
        } else {
            console.log('⚠️  Dados já existem, pulando inserção de teste');
        }
    } catch (error) {
        console.error('❌ Erro ao inserir dados de teste:', error);
        // Não propagar erro para não quebrar a migração
    }
}

async function main() {
    try {
        await createTables();
        console.log('🎉 Migração Data Service concluída com sucesso!');
    } catch (error) {
        console.error('💥 Erro na migração Data Service:', error);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

// Executar se chamado diretamente
if (require.main === module) {
    main();
}

module.exports = { createTables, insertTestData };

