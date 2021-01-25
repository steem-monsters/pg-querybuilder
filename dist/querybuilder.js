"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const utils_1 = require("./utils");
const pg_1 = require("pg");
;
class QueryBuilder {
    constructor(config) {
        this.slow_query_limit = 0;
        this.pool = new pg_1.Pool({
            user: config.connection.user,
            host: config.connection.host,
            database: config.connection.database,
            password: config.connection.password,
            port: config.connection.port
        });
        this.slow_query_limit = config.slow_query_limit;
    }
    query(c, text, params, conn) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!conn)
                conn = yield this.pool.connect();
            try {
                let start_time = Date.now();
                let result = yield conn.query(text, params);
                let total_time = Date.now() - start_time;
                if (this.slow_query_limit && total_time > this.slow_query_limit)
                    utils_1.Utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');
                return result.rows.map(r => new c(r));
            }
            catch (err) {
                utils_1.Utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
                throw err;
            }
        });
    }
    valueQuery(text, params, conn) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!conn)
                conn = yield this.pool.connect();
            try {
                let start_time = Date.now();
                let result = yield conn.query(text, params);
                let total_time = Date.now() - start_time;
                if (this.slow_query_limit && total_time > this.slow_query_limit)
                    utils_1.Utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');
                return result.rows[0];
            }
            catch (err) {
                utils_1.Utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
                throw err;
            }
        });
    }
    count(type, filters, conn) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!conn)
                conn = yield this.pool.connect();
            let _filters = new Filters(filters);
            let query_str = 'SELECT COUNT(*) as "count" FROM ' + type + ' WHERE ' + _filters.clauses.join(' AND ');
            return (yield this.valueQuery(query_str, _filters.params, conn)).count;
        });
    }
    sum(type, sum_column, filters, conn) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!conn)
                conn = yield this.pool.connect();
            let _filters = new Filters(filters);
            let query_str = 'SELECT SUM(' + sum_column + ') as "sum" FROM ' + type + ' WHERE ' + _filters.clauses.join(' AND ');
            return (yield this.valueQuery(query_str, _filters.params, conn)).sum;
        });
    }
    transaction(callback) {
        return __awaiter(this, void 0, void 0, function* () {
            const client = yield this.pool.connect();
            try {
                yield client.query('BEGIN');
                let result = yield callback(client);
                if (result && result.error) {
                    yield client.query('ROLLBACK');
                    return result;
                }
                yield client.query('COMMIT');
                return result;
            }
            catch (e) {
                yield client.query('ROLLBACK');
                throw e;
            }
            finally {
                client.release();
            }
        });
    }
}
exports.QueryBuilder = QueryBuilder;
class Filters {
    constructor(filters, start = 0) {
        this.params = this.parseParams(filters);
        this.clauses = this.parseClauses(filters, start);
    }
    parseParams(filters) {
        let params = [];
        Object.keys(filters).forEach(f => {
            if (Array.isArray(filters[f]))
                params = params.concat(filters[f]);
            else if (f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
                params = params.concat(this.parseParams(filters[f]));
            }
            else if (filters[f] != null && typeof filters[f] == 'object' && filters[f].op) {
                params.push(filters[f].value);
            }
            else if (filters[f] != null)
                params.push(filters[f]);
        });
        return params;
    }
    parseClauses(filters, start = 0, param_index = 0) {
        return Object.keys(filters).map(f => {
            if (filters[f] == null)
                return f + ' IS NULL';
            else if (f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
                return '(' + this.parseClauses(filters[f], start, param_index).join(' OR ') + ')';
            }
            else if (Array.isArray(filters[f])) {
                let val = f + ' IN ' + this.inClause(start + ++param_index, filters[f]);
                param_index += filters[f].length - 1;
                return val;
            }
            else if (typeof filters[f] == 'object' && filters[f].op) {
                return `${f} ${filters[f].op} $${start + ++param_index}`;
            }
            else
                return f + ' = $' + (start + ++param_index);
        });
    }
    inClause(start, length) {
        // I'm sure there's a better way to do this but I don't know how...
        let in_clause = '(';
        for (let i = 0; i < length; i++)
            in_clause += '$' + (i + start) + ((i < length - 1) ? ',' : '');
        in_clause += ')';
        return in_clause;
    }
}
//# sourceMappingURL=querybuilder.js.map