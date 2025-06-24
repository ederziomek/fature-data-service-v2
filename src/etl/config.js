require('dotenv').config();

const ETLConfig = {
  // Configurações gerais do ETL
  general: {
    enabled: process.env.ETL_ENABLED !== 'false',
    batchSize: parseInt(process.env.ETL_BATCH_SIZE) || 1000,
    maxRetries: parseInt(process.env.ETL_MAX_RETRIES) || 3,
    retryDelay: parseInt(process.env.ETL_RETRY_DELAY) || 5000,
    parallelJobs: parseInt(process.env.ETL_PARALLEL_JOBS) || 3,
    logLevel: process.env.ETL_LOG_LEVEL || 'info'
  },

  // Banco de origem (operação)
  source: {
    host: process.env.EXTERNAL_DB_HOST || '177.115.223.216',
    port: parseInt(process.env.EXTERNAL_DB_PORT) || 5999,
    database: process.env.EXTERNAL_DB_NAME || 'dados_interno',
    username: process.env.EXTERNAL_DB_USER || 'userschapz',
    password: process.env.EXTERNAL_DB_PASSWORD || 'mschaphz8881!',
    ssl: process.env.EXTERNAL_DB_SSL === 'true',
    connectionTimeout: parseInt(process.env.EXTERNAL_DB_TIMEOUT) || 30000,
    pool: {
      min: parseInt(process.env.EXTERNAL_DB_POOL_MIN) || 2,
      max: parseInt(process.env.EXTERNAL_DB_POOL_MAX) || 10
    }
  },

  // Banco de destino (Affiliate Service)
  target: {
    host: process.env.AFFILIATE_DB_HOST || 'localhost',
    port: parseInt(process.env.AFFILIATE_DB_PORT) || 5432,
    database: process.env.AFFILIATE_DB_NAME || 'fature_affiliate_db',
    username: process.env.AFFILIATE_DB_USER || 'postgres',
    password: process.env.AFFILIATE_DB_PASSWORD || 'password',
    ssl: process.env.AFFILIATE_DB_SSL === 'true',
    connectionTimeout: parseInt(process.env.AFFILIATE_DB_TIMEOUT) || 30000,
    pool: {
      min: parseInt(process.env.AFFILIATE_DB_POOL_MIN) || 2,
      max: parseInt(process.env.AFFILIATE_DB_POOL_MAX) || 10
    }
  },

  // Configurações de agendamento
  schedule: {
    enabled: process.env.ETL_SCHEDULE_ENABLED !== 'false',
    
    // Sincronização completa (diária às 02:00)
    fullSync: {
      cron: process.env.ETL_FULL_SYNC_CRON || '0 2 * * *',
      enabled: process.env.ETL_FULL_SYNC_ENABLED !== 'false',
      timeout: parseInt(process.env.ETL_FULL_SYNC_TIMEOUT) || 3600000 // 1 hora
    },

    // Sincronização incremental (a cada 15 minutos)
    incrementalSync: {
      cron: process.env.ETL_INCREMENTAL_SYNC_CRON || '*/15 * * * *',
      enabled: process.env.ETL_INCREMENTAL_SYNC_ENABLED !== 'false',
      timeout: parseInt(process.env.ETL_INCREMENTAL_SYNC_TIMEOUT) || 300000 // 5 minutos
    },

    // Limpeza de logs (semanal)
    cleanup: {
      cron: process.env.ETL_CLEANUP_CRON || '0 3 * * 0',
      enabled: process.env.ETL_CLEANUP_ENABLED !== 'false',
      retentionDays: parseInt(process.env.ETL_LOG_RETENTION_DAYS) || 30
    }
  },

  // Mapeamento de tabelas e transformações
  mappings: {
    // Usuários -> Afiliados
    users: {
      sourceTable: 'users',
      targetTable: 'affiliates',
      primaryKey: 'id',
      incrementalField: 'updated_at',
      enabled: process.env.ETL_SYNC_USERS !== 'false',
      
      // Campos mapeados
      fieldMapping: {
        'id': 'external_user_id',
        'username': 'username',
        'email': 'email',
        'first_name': 'first_name',
        'last_name': 'last_name',
        'phone': 'phone',
        'document': 'document',
        'birth_date': 'birth_date',
        'created_at': 'created_at',
        'updated_at': 'updated_at',
        'status': 'status',
        'referrer_id': 'referrer_id'
      },

      // Transformações específicas
      transformations: {
        status: (value) => {
          const statusMap = {
            'active': 'ACTIVE',
            'inactive': 'INACTIVE',
            'suspended': 'SUSPENDED',
            'banned': 'BANNED'
          };
          return statusMap[value] || 'INACTIVE';
        },
        
        document: (value) => {
          // Limpar formatação do documento
          return value ? value.replace(/[^\d]/g, '') : null;
        },

        phone: (value) => {
          // Limpar formatação do telefone
          return value ? value.replace(/[^\d]/g, '') : null;
        }
      },

      // Validações
      validations: {
        required: ['external_user_id', 'username', 'email'],
        email: 'email',
        unique: ['external_user_id', 'username', 'email']
      }
    },

    // Transações -> Referrals
    transactions: {
      sourceTable: 'transactions',
      targetTable: 'referrals',
      primaryKey: 'id',
      incrementalField: 'created_at',
      enabled: process.env.ETL_SYNC_TRANSACTIONS !== 'false',
      
      // Filtros para considerar apenas transações relevantes
      filters: {
        type: ['deposit', 'bet'],
        status: ['completed', 'approved'],
        amount: { '>': 0 }
      },

      fieldMapping: {
        'id': 'external_transaction_id',
        'user_id': 'referred_user_id',
        'type': 'transaction_type',
        'amount': 'transaction_amount',
        'status': 'validation_status',
        'created_at': 'transaction_date',
        'updated_at': 'updated_at'
      },

      transformations: {
        validation_status: (value, row) => {
          // Lógica para determinar se a transação valida o referral
          if (value === 'completed' && parseFloat(row.amount) >= 50) {
            return 'VALIDATED';
          }
          return 'PENDING';
        },

        transaction_type: (value) => {
          const typeMap = {
            'deposit': 'DEPOSIT',
            'bet': 'BET',
            'withdrawal': 'WITHDRAWAL'
          };
          return typeMap[value] || 'OTHER';
        }
      },

      validations: {
        required: ['external_transaction_id', 'referred_user_id', 'transaction_amount'],
        numeric: ['transaction_amount'],
        positive: ['transaction_amount']
      }
    },

    // Apostas -> Bet Activities
    bets: {
      sourceTable: 'bets',
      targetTable: 'bet_activities',
      primaryKey: 'id',
      incrementalField: 'created_at',
      enabled: process.env.ETL_SYNC_BETS !== 'false',

      fieldMapping: {
        'id': 'external_bet_id',
        'user_id': 'user_id',
        'amount': 'bet_amount',
        'odds': 'odds',
        'status': 'bet_status',
        'result': 'bet_result',
        'payout': 'payout_amount',
        'created_at': 'bet_date',
        'updated_at': 'updated_at'
      },

      transformations: {
        bet_status: (value) => {
          const statusMap = {
            'pending': 'PENDING',
            'won': 'WON',
            'lost': 'LOST',
            'cancelled': 'CANCELLED',
            'void': 'VOID'
          };
          return statusMap[value] || 'PENDING';
        },

        bet_result: (value) => {
          return value === 'won' ? 'WIN' : value === 'lost' ? 'LOSS' : 'PENDING';
        }
      },

      validations: {
        required: ['external_bet_id', 'user_id', 'bet_amount'],
        numeric: ['bet_amount', 'odds', 'payout_amount'],
        positive: ['bet_amount']
      }
    }
  },

  // Configurações de monitoramento
  monitoring: {
    enabled: process.env.ETL_MONITORING_ENABLED !== 'false',
    
    // Métricas a serem coletadas
    metrics: {
      recordsProcessed: true,
      processingTime: true,
      errorRate: true,
      throughput: true,
      memoryUsage: true
    },

    // Alertas
    alerts: {
      enabled: process.env.ETL_ALERTS_ENABLED !== 'false',
      errorThreshold: parseFloat(process.env.ETL_ERROR_THRESHOLD) || 0.05, // 5%
      timeoutThreshold: parseInt(process.env.ETL_TIMEOUT_THRESHOLD) || 1800000, // 30 min
      
      // Webhook para notificações (futuro)
      webhook: process.env.ETL_ALERT_WEBHOOK || null
    },

    // Retenção de logs
    logRetention: {
      days: parseInt(process.env.ETL_LOG_RETENTION_DAYS) || 30,
      maxSize: process.env.ETL_LOG_MAX_SIZE || '100MB'
    }
  },

  // Configurações de cache
  cache: {
    enabled: process.env.ETL_CACHE_ENABLED !== 'false',
    ttl: parseInt(process.env.ETL_CACHE_TTL) || 3600, // 1 hora
    
    // Cache de metadados
    metadata: {
      enabled: true,
      ttl: parseInt(process.env.ETL_METADATA_CACHE_TTL) || 7200 // 2 horas
    },

    // Cache de configurações
    config: {
      enabled: true,
      ttl: parseInt(process.env.ETL_CONFIG_CACHE_TTL) || 1800 // 30 minutos
    }
  }
};

module.exports = ETLConfig;

