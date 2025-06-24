const ETLConfig = require('../config');
const logger = require('../../utils/logger');
const moment = require('moment');
const _ = require('lodash');

class DataTransformer {
  constructor() {
    this.transformationStats = {
      recordsProcessed: 0,
      recordsTransformed: 0,
      recordsRejected: 0,
      errors: []
    };
  }

  // Transformar dados de uma tabela
  async transformTable(tableName, sourceData) {
    const mapping = ETLConfig.mappings[tableName];
    if (!mapping) {
      throw new Error(`Mapeamento não encontrado para tabela ${tableName}`);
    }

    logger.info(`🔄 ETL Transformer: Iniciando transformação de ${tableName}`, {
      recordCount: sourceData.length
    });

    const startTime = Date.now();
    const transformedData = [];
    const rejectedRecords = [];

    for (const sourceRecord of sourceData) {
      try {
        this.transformationStats.recordsProcessed++;

        // Aplicar mapeamento de campos
        const mappedRecord = this.mapFields(sourceRecord, mapping.fieldMapping);

        // Aplicar transformações específicas
        const transformedRecord = this.applyTransformations(
          mappedRecord, 
          mapping.transformations || {},
          sourceRecord
        );

        // Validar registro transformado
        const validation = this.validateRecord(transformedRecord, mapping.validations || {});
        
        if (validation.isValid) {
          // Adicionar metadados
          transformedRecord._etl_metadata = {
            source_table: mapping.sourceTable,
            target_table: mapping.targetTable,
            transformed_at: new Date().toISOString(),
            source_id: sourceRecord[mapping.primaryKey]
          };

          transformedData.push(transformedRecord);
          this.transformationStats.recordsTransformed++;
        } else {
          rejectedRecords.push({
            sourceRecord,
            errors: validation.errors,
            rejectedAt: new Date().toISOString()
          });
          this.transformationStats.recordsRejected++;
        }

      } catch (error) {
        logger.error(`❌ ETL Transformer: Erro ao transformar registro`, {
          table: tableName,
          sourceId: sourceRecord[mapping.primaryKey],
          error: error.message
        });

        rejectedRecords.push({
          sourceRecord,
          errors: [error.message],
          rejectedAt: new Date().toISOString()
        });

        this.transformationStats.recordsRejected++;
        this.transformationStats.errors.push({
          table: tableName,
          sourceId: sourceRecord[mapping.primaryKey],
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    }

    const transformTime = Date.now() - startTime;

    logger.info(`✅ ETL Transformer: Transformação concluída`, {
      table: tableName,
      processed: this.transformationStats.recordsProcessed,
      transformed: transformedData.length,
      rejected: rejectedRecords.length,
      transformTime: `${transformTime}ms`
    });

    return {
      success: true,
      transformedData,
      rejectedRecords,
      stats: {
        recordsProcessed: sourceData.length,
        recordsTransformed: transformedData.length,
        recordsRejected: rejectedRecords.length,
        transformTime,
        successRate: ((transformedData.length / sourceData.length) * 100).toFixed(2)
      }
    };
  }

  // Mapear campos de origem para destino
  mapFields(sourceRecord, fieldMapping) {
    const mappedRecord = {};

    Object.entries(fieldMapping).forEach(([sourceField, targetField]) => {
      if (sourceRecord.hasOwnProperty(sourceField)) {
        mappedRecord[targetField] = sourceRecord[sourceField];
      }
    });

    return mappedRecord;
  }

  // Aplicar transformações específicas
  applyTransformations(record, transformations, originalRecord) {
    const transformedRecord = { ...record };

    Object.entries(transformations).forEach(([field, transformFunction]) => {
      if (transformedRecord.hasOwnProperty(field)) {
        try {
          if (typeof transformFunction === 'function') {
            transformedRecord[field] = transformFunction(
              transformedRecord[field], 
              originalRecord
            );
          }
        } catch (error) {
          logger.warn(`⚠️ ETL Transformer: Erro na transformação do campo ${field}`, {
            error: error.message,
            originalValue: transformedRecord[field]
          });
        }
      }
    });

    // Aplicar transformações padrão
    transformedRecord = this.applyDefaultTransformations(transformedRecord);

    return transformedRecord;
  }

  // Aplicar transformações padrão
  applyDefaultTransformations(record) {
    const transformed = { ...record };

    Object.entries(transformed).forEach(([field, value]) => {
      // Limpar strings
      if (typeof value === 'string') {
        transformed[field] = value.trim();
        
        // Converter strings vazias para null
        if (transformed[field] === '') {
          transformed[field] = null;
        }
      }

      // Converter datas
      if (field.includes('_at') || field.includes('_date') || field.includes('date_')) {
        if (value && !moment(value).isValid()) {
          logger.warn(`⚠️ ETL Transformer: Data inválida no campo ${field}`, {
            value
          });
          transformed[field] = null;
        } else if (value) {
          transformed[field] = moment(value).toISOString();
        }
      }

      // Converter números
      if (field.includes('amount') || field.includes('_id') || field === 'id') {
        if (value !== null && value !== undefined && value !== '') {
          const numValue = parseFloat(value);
          if (!isNaN(numValue)) {
            transformed[field] = numValue;
          }
        }
      }

      // Converter booleanos
      if (typeof value === 'string' && (value.toLowerCase() === 'true' || value.toLowerCase() === 'false')) {
        transformed[field] = value.toLowerCase() === 'true';
      }
    });

    return transformed;
  }

  // Validar registro transformado
  validateRecord(record, validations) {
    const errors = [];

    // Validar campos obrigatórios
    if (validations.required) {
      validations.required.forEach(field => {
        if (!record[field] || record[field] === null || record[field] === '') {
          errors.push(`Campo obrigatório '${field}' está vazio`);
        }
      });
    }

    // Validar emails
    if (validations.email) {
      const emailField = validations.email;
      if (record[emailField]) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(record[emailField])) {
          errors.push(`Campo '${emailField}' não é um email válido`);
        }
      }
    }

    // Validar campos numéricos
    if (validations.numeric) {
      validations.numeric.forEach(field => {
        if (record[field] !== null && record[field] !== undefined) {
          if (isNaN(parseFloat(record[field]))) {
            errors.push(`Campo '${field}' deve ser numérico`);
          }
        }
      });
    }

    // Validar valores positivos
    if (validations.positive) {
      validations.positive.forEach(field => {
        if (record[field] !== null && record[field] !== undefined) {
          const value = parseFloat(record[field]);
          if (!isNaN(value) && value <= 0) {
            errors.push(`Campo '${field}' deve ser positivo`);
          }
        }
      });
    }

    // Validar comprimento de strings
    if (validations.maxLength) {
      Object.entries(validations.maxLength).forEach(([field, maxLength]) => {
        if (record[field] && typeof record[field] === 'string') {
          if (record[field].length > maxLength) {
            errors.push(`Campo '${field}' excede o comprimento máximo de ${maxLength} caracteres`);
          }
        }
      });
    }

    // Validar valores únicos (será verificado no loader)
    // Por enquanto, apenas marcar os campos que devem ser únicos
    if (validations.unique) {
      record._unique_fields = validations.unique;
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  // Transformações específicas para afiliados
  transformAffiliate(sourceRecord) {
    const transformed = {
      external_user_id: parseInt(sourceRecord.id),
      username: sourceRecord.username?.toLowerCase(),
      email: sourceRecord.email?.toLowerCase(),
      first_name: this.capitalizeFirstLetter(sourceRecord.first_name),
      last_name: this.capitalizeFirstLetter(sourceRecord.last_name),
      phone: this.cleanPhone(sourceRecord.phone),
      document: this.cleanDocument(sourceRecord.document),
      birth_date: this.parseDate(sourceRecord.birth_date),
      created_at: this.parseDate(sourceRecord.created_at),
      updated_at: this.parseDate(sourceRecord.updated_at),
      status: this.mapUserStatus(sourceRecord.status),
      referrer_id: sourceRecord.referrer_id ? parseInt(sourceRecord.referrer_id) : null,
      
      // Campos calculados
      total_referrals: 0,
      total_validated_referrals: 0,
      total_cpa_earned: 0.00,
      last_activity_at: this.parseDate(sourceRecord.last_login_at || sourceRecord.updated_at),
      
      // Metadados
      sync_status: 'SYNCED',
      last_sync_at: new Date().toISOString()
    };

    return transformed;
  }

  // Transformações específicas para referrals
  transformReferral(sourceRecord) {
    const transformed = {
      external_transaction_id: parseInt(sourceRecord.id),
      referred_user_id: parseInt(sourceRecord.user_id),
      transaction_type: this.mapTransactionType(sourceRecord.type),
      transaction_amount: parseFloat(sourceRecord.amount),
      transaction_date: this.parseDate(sourceRecord.created_at),
      validation_status: this.determineValidationStatus(sourceRecord),
      validation_date: this.determineValidationDate(sourceRecord),
      cpa_amount: this.calculateCPAAmount(sourceRecord),
      updated_at: this.parseDate(sourceRecord.updated_at),
      
      // Metadados
      sync_status: 'SYNCED',
      last_sync_at: new Date().toISOString()
    };

    return transformed;
  }

  // Utilitários de transformação
  capitalizeFirstLetter(str) {
    if (!str) return null;
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
  }

  cleanPhone(phone) {
    if (!phone) return null;
    return phone.replace(/[^\d]/g, '');
  }

  cleanDocument(document) {
    if (!document) return null;
    return document.replace(/[^\d]/g, '');
  }

  parseDate(dateStr) {
    if (!dateStr) return null;
    const date = moment(dateStr);
    return date.isValid() ? date.toISOString() : null;
  }

  mapUserStatus(status) {
    const statusMap = {
      'active': 'ACTIVE',
      'inactive': 'INACTIVE',
      'suspended': 'SUSPENDED',
      'banned': 'BANNED',
      'pending': 'PENDING'
    };
    return statusMap[status?.toLowerCase()] || 'INACTIVE';
  }

  mapTransactionType(type) {
    const typeMap = {
      'deposit': 'DEPOSIT',
      'bet': 'BET',
      'withdrawal': 'WITHDRAWAL',
      'bonus': 'BONUS'
    };
    return typeMap[type?.toLowerCase()] || 'OTHER';
  }

  determineValidationStatus(transaction) {
    // Lógica para determinar se a transação valida o referral
    const amount = parseFloat(transaction.amount);
    const status = transaction.status?.toLowerCase();
    
    if (status === 'completed' && amount >= 50) {
      return 'VALIDATED';
    } else if (status === 'pending') {
      return 'PENDING';
    } else {
      return 'REJECTED';
    }
  }

  determineValidationDate(transaction) {
    if (this.determineValidationStatus(transaction) === 'VALIDATED') {
      return this.parseDate(transaction.updated_at || transaction.created_at);
    }
    return null;
  }

  calculateCPAAmount(transaction) {
    // Por enquanto, retornar 0. O cálculo real será feito no Affiliate Service
    return 0.00;
  }

  // Obter estatísticas de transformação
  getStats() {
    return { ...this.transformationStats };
  }

  // Resetar estatísticas
  resetStats() {
    this.transformationStats = {
      recordsProcessed: 0,
      recordsTransformed: 0,
      recordsRejected: 0,
      errors: []
    };
  }
}

module.exports = DataTransformer;

