require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');

const dataRoutes = require('./routes/dataRoutes');
const { requestLogger, errorHandler, corsHandler } = require('./middleware/validation');
const logger = require('./utils/logger');

class DataServiceApp {
    constructor() {
        this.app = express();
        this.port = process.env.PORT || 3004;
        this.host = process.env.HOST || '0.0.0.0';
        
        this.setupMiddleware();
        this.setupRoutes();
        this.setupErrorHandling();
    }

    setupMiddleware() {
        // Segurança
        this.app.use(helmet({
            crossOriginResourcePolicy: { policy: "cross-origin" }
        }));

        // CORS
        this.app.use(cors({
            origin: '*',
            credentials: true,
            methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
            allowedHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Authorization', 'X-API-Key']
        }));

        // Compressão
        this.app.use(compression());

        // Parsing
        this.app.use(express.json({ limit: '10mb' }));
        this.app.use(express.urlencoded({ extended: true, limit: '10mb' }));

        // Logging
        if (process.env.NODE_ENV !== 'test') {
            this.app.use(morgan('combined', {
                stream: {
                    write: (message) => logger.info(message.trim())
                }
            }));
        }

        this.app.use(requestLogger);

        // Headers customizados
        this.app.use((req, res, next) => {
            res.header('X-Service', 'fature-data-service-v2');
            res.header('X-Version', process.env.npm_package_version || '2.0.0');
            next();
        });
    }

    setupRoutes() {
        // Rota raiz
        this.app.get('/', (req, res) => {
            res.json({
                service: 'Fature Data Service V2',
                version: process.env.npm_package_version || '2.0.0',
                status: 'running',
                timestamp: new Date().toISOString(),
                description: 'Serviço de dados migrado para Node.js com configurações dinâmicas',
                features: {
                    'Data Synchronization': 'Sincronização automática com banco da operação',
                    'Analytics Generation': 'Geração de analytics para usuários e afiliados',
                    'External Data Access': 'Acesso a dados externos com cache inteligente',
                    'Data Export': 'Exportação de dados em múltiplos formatos',
                    'Dynamic Configuration': 'Configurações 100% dinâmicas via Config Service',
                    'Real-time Updates': 'Atualizações em tempo real via WebSocket',
                    'Performance Optimization': 'Cache inteligente e batch processing'
                },
                endpoints: {
                    health: '/api/v1/health',
                    sync: '/api/v1/sync/*',
                    analytics: '/api/v1/analytics/*',
                    external: '/api/v1/external/*',
                    export: '/api/v1/export/*',
                    config: '/api/v1/config',
                    stats: '/api/v1/stats',
                    docs: '/api/v1/docs'
                }
            });
        });

        // Rotas da API
        this.app.use('/api/v1', dataRoutes);

        // Documentação básica da API
        this.app.get('/api/v1/docs', (req, res) => {
            res.json({
                title: 'Fature Data Service V2 API',
                version: '2.0.0',
                description: 'API para gerenciamento de dados com sincronização automática e analytics',
                baseUrl: `${req.protocol}://${req.get('host')}/api/v1`,
                endpoints: {
                    'GET /health': 'Health check do serviço',
                    'POST /sync/:tableName': 'Sincronizar dados de uma tabela específica',
                    'POST /analytics/user/:id/generate': 'Gerar analytics para um usuário',
                    'POST /analytics/affiliate/:id/generate': 'Gerar analytics para um afiliado',
                    'GET /analytics/user/:id': 'Buscar analytics de um usuário',
                    'GET /analytics/affiliate/:id': 'Buscar analytics de um afiliado',
                    'GET /external/:tableName': 'Buscar dados externos de uma tabela',
                    'POST /export': 'Criar nova exportação de dados',
                    'GET /export': 'Buscar todas as exportações',
                    'GET /export/:id': 'Buscar exportação específica',
                    'GET /config': 'Buscar configurações atuais',
                    'DELETE /cache/expired': 'Limpar cache expirado',
                    'GET /stats': 'Estatísticas do serviço'
                },
                authentication: {
                    type: 'API Key',
                    header: 'X-API-Key ou Authorization',
                    description: 'Incluir API Key no header da requisição'
                },
                integrations: {
                    'Config Service': {
                        url: process.env.CONFIG_SERVICE_URL,
                        description: 'Configurações dinâmicas'
                    },
                    'External Database': {
                        host: process.env.EXTERNAL_DB_HOST,
                        enabled: process.env.ENABLE_DATA_SYNC === 'true',
                        description: 'Banco da operação para sincronização'
                    }
                },
                features: {
                    'Dynamic Configuration': 'Todas as configurações vêm do Config Service',
                    'Real-time Updates': 'Configurações atualizadas automaticamente via WebSocket',
                    'Data Synchronization': 'Sincronização automática com intervalos configuráveis',
                    'Analytics Generation': 'Geração de analytics por período (diário, semanal, mensal, anual)',
                    'External Data Access': 'Acesso otimizado a dados externos com cache',
                    'Data Export': 'Exportação em CSV, JSON, XLSX e PDF',
                    'Performance Optimization': 'Cache inteligente e processamento em lotes',
                    'Comprehensive Logging': 'Log detalhado de todas as operações'
                },
                supportedTables: {
                    'users': 'Dados de usuários',
                    'transactions': 'Transações financeiras',
                    'bets': 'Apostas realizadas',
                    'deposits': 'Depósitos efetuados'
                },
                analyticsTypes: {
                    'DAILY': 'Analytics diários',
                    'WEEKLY': 'Analytics semanais',
                    'MONTHLY': 'Analytics mensais',
                    'YEARLY': 'Analytics anuais'
                },
                exportFormats: {
                    'CSV': 'Comma-separated values',
                    'JSON': 'JavaScript Object Notation',
                    'XLSX': 'Excel spreadsheet',
                    'PDF': 'Portable Document Format'
                }
            });
        });

        // Rota 404
        this.app.use('*', (req, res) => {
            res.status(404).json({
                success: false,
                message: 'Endpoint não encontrado',
                path: req.originalUrl,
                method: req.method
            });
        });
    }

    setupErrorHandling() {
        this.app.use(errorHandler);

        // Handlers de processo
        process.on('uncaughtException', (error) => {
            logger.error('Uncaught Exception:', error);
            this.gracefulShutdown('uncaughtException');
        });

        process.on('unhandledRejection', (reason, promise) => {
            logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
            this.gracefulShutdown('unhandledRejection');
        });

        process.on('SIGTERM', () => {
            logger.info('SIGTERM recebido');
            this.gracefulShutdown('SIGTERM');
        });

        process.on('SIGINT', () => {
            logger.info('SIGINT recebido');
            this.gracefulShutdown('SIGINT');
        });
    }

    async start() {
        try {
            // Testar conexão com banco antes de iniciar
            const { createTables } = require('./database/migrate');
            await createTables();
            
            this.server = this.app.listen(this.port, this.host, () => {
                logger.info(`🚀 Data Service V2 iniciado em http://${this.host}:${this.port}`);
                logger.info(`📚 Documentação disponível em http://${this.host}:${this.port}/api/v1/docs`);
                logger.info(`🏥 Health check disponível em http://${this.host}:${this.port}/api/v1/health`);
                logger.info(`🔧 Configurações dinâmicas via Config Service`);
                logger.info(`🔄 Sincronização de dados: ${process.env.ENABLE_DATA_SYNC === 'true' ? 'HABILITADA' : 'DESABILITADA'}`);
                logger.info(`📊 Analytics automático: ${process.env.ENABLE_CRON_JOBS === 'true' ? 'HABILITADO' : 'DESABILITADO'}`);
                logger.info(`🗄️ Banco externo: ${process.env.EXTERNAL_DB_HOST}:${process.env.EXTERNAL_DB_PORT}`);
            });

        } catch (error) {
            logger.error('Erro ao iniciar o serviço:', error);
            process.exit(1);
        }
    }

    async gracefulShutdown(signal) {
        logger.info(`Iniciando shutdown graceful devido a: ${signal}`);

        // Fechar servidor HTTP
        if (this.server) {
            this.server.close(() => {
                logger.info('Servidor HTTP fechado');
            });
        }

        // Aguardar um tempo para conexões ativas terminarem
        setTimeout(() => {
            logger.info('Shutdown completo');
            process.exit(0);
        }, 5000);
    }

    // Método para obter estatísticas do serviço
    getStats() {
        return {
            service: 'fature-data-service-v2',
            version: process.env.npm_package_version || '2.0.0',
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            integrations: {
                configService: process.env.CONFIG_SERVICE_URL,
                externalDatabase: `${process.env.EXTERNAL_DB_HOST}:${process.env.EXTERNAL_DB_PORT}`,
                syncEnabled: process.env.ENABLE_DATA_SYNC === 'true',
                cronJobsEnabled: process.env.ENABLE_CRON_JOBS === 'true'
            },
            timestamp: new Date().toISOString()
        };
    }
}

// Iniciar aplicação se executado diretamente
if (require.main === module) {
    const app = new DataServiceApp();
    app.start();
}

module.exports = DataServiceApp;

