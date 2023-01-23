const { Pool } = require('pg');
const utils = require('./utils');

class QueryBuilder {
	pool = null;
	_config = {};

	constructor(config) {
		this._config = config;

		this.pool = new Pool({
			user: config.connection.user,
			host: config.connection.host,
			database: config.connection.database,
			password: config.connection.password,
			port: config.connection.port
		});
	}

	async count(type, filters, conn) {
		if(!conn)
			conn = this.pool;

		let parsed_filters = this.parseFilters(filters);

		const query_str = 'SELECT COUNT(*) as "count" FROM ' + type + ' WHERE ' + parsed_filters.clauses.join(' AND ');
		return (await this.query(query_str, parsed_filters.params, conn)).rows[0].count;
	}

	async sum(type, sum_column, filters, conn) {
		if(!conn)
			conn = this.pool;

		let parsed_filters = this.parseFilters(filters);

		const query_str = 'SELECT SUM(' + sum_column + ') as "sum" FROM ' + type + ' WHERE ' + parsed_filters.clauses.join(' AND ');
		return (await this.query(query_str, parsed_filters.params, conn)).rows[0].sum;
	}

	parseFilters(filters, start) {
		return { params: this.parseParams(filters), clauses: this.parseClauses(filters, start) };
	}

	parseClauses(filters, start, param_index) {
		if(!start)
			start = 0;

		if(!param_index)
			param_index = 0;

		return Object.keys(filters).map(f => {
			if(filters[f] == null)
				return f + ' IS NULL';
			else if(f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
				return '(' + this.parseClauses(filters[f], start, param_index).join(' OR ') + ')';
			} else if(Array.isArray(filters[f])) {
				var val = f + ' IN ' + utils.inClause(start + ++param_index, filters[f]);
				param_index += filters[f].length - 1;
				return val
			} else if(typeof filters[f] == 'object' && filters[f].op) {
				return `${f} ${filters[f].op} $${start + ++param_index}`;
			} else
				return f + ' = $' + (start + ++param_index);
		});
	}

	parseParams(filters) {
		let params = [];

		Object.keys(filters).forEach(f => {
			if(Array.isArray(filters[f]))
				params = params.concat(filters[f]);
			else if(f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
				params = params.concat(this.parseParams(filters[f]));
			} else if(filters[f] != null && typeof filters[f] == 'object' && filters[f].op) {
				params.push(filters[f].value);
			} else if(filters[f] != null)
				params.push(filters[f])
		});

		return params;
	}

	async lookup(type, options, conn) {
		try {
			if (!conn) conn = this.pool;
	
			if (!options) options = {};
	
			let params = [];
			const columns = options.columns && Array.isArray(options.columns) ? options.columns.join(', ') : '*';
			let query_str = `SELECT ${columns} FROM ${type}`;
	
			if (options.filters && Object.keys(options.filters).length > 0) {
				const filters = this.parseFilters(options.filters);
				params = filters.params;
				query_str += ` WHERE ${filters.clauses.join(' AND ')}`;
			}
	
			if (options.sort_by) {
				query_str += ` ORDER BY ${options.sort_by}`;
	
				if (options.sort_descending) query_str += ' DESC';
			}
	
			if (options.limit) query_str += ` LIMIT ${options.limit}`;
			if (options.offset) query_str += ` OFFSET ${options.offset}`;
			if (options.lockLevel) {
				if (options.lockLevel) query_str += ` FOR ${options.lockLevel}`;
	
				if (options.nowait) query_str += ' NOWAIT';
				else if (options.skip_locked) query_str += ' SKIP LOCKED';
			}
	
			return (await this.query_internal(query_str, params, conn)).rows;
		} catch (err) {
			utils.log(err.message, 1, 'Red');
			utils.log(err.stack, 1, 'Red');
		}
	}
	
	async lookupSingle(type, filters, conn, options) {
		const records = await this.lookup(type, { filters, ...options }, conn);
		return records && records.length > 0 ? records[0] : null;
	}

	async insert(type, data, conn, no_return, on_conflict_clause) {
		if (!conn) conn = this.pool;
	
		const fields = Object.keys(data);
		const params = fields.map((f) => data[f]);
		const indices = fields.map((f, i) => `$${i + 1}`);
	
		let query_str = `INSERT INTO ${type}(${fields.join(',')}) VALUES (${indices.join(',')})`;
	
		if (!no_return) query_str += RETURNING_STR;
		if (on_conflict_clause) query_str += ` ${on_conflict_clause}`;
	
		const ret_val = await this.query_internal(query_str, params, conn);
		return no_return ? ret_val.rowCount : ret_val.rows.length > 0 ? ret_val.rows[0] : null;
	}
	
	async insertMultiple(type, data, conn, no_return) {
		if (!conn) conn = this.pool;
	
		const fields = Object.keys(data[0]);
		let params = [];
		let values = '';
	
		for (let i = 0; i < data.length; i++) {
			const obj = data[i];
			params = params.concat(fields.map((f) => obj[f]));
			const indices = fields.map((f, d) => `$${i * fields.length + d + 1}`);
			values += `${i > 0 ? ',' : ''}(${indices.join(',')})`;
		}
	
		let query_str = `INSERT INTO ${type}(${fields.join(',')}) VALUES ${values}`;
	
		if (!no_return) query_str += RETURNING_STR;
	
		const ret_val = await this.query_internal(query_str, params, conn);
		return no_return ? ret_val.rowCount : ret_val.rows;
	}
	
	async deleteRows(type, filters, conn, no_return) {
		if (!conn) conn = this.pool;
	
		const parsed_filters = this.parseFilters(filters);
	
		let query_str = `DELETE FROM ${type} WHERE ${parsed_filters.clauses.join(' AND ')}`;
	
		if (!no_return) query_str += RETURNING_STR;
	
		return (await this.query_internal(query_str, parsed_filters.params, conn)).rows;
	}

	async deleteSingle(type, filters, conn, no_return) {
		const deleted = await this.deleteRows(type, filters, conn, no_return);
		return deleted && deleted.length > 0 ? deleted[0] : null;
	}
	
	async update(type, data, filters, conn, no_return) {
		if (!conn) conn = this.pool;
	
		const data_fields = Object.keys(data);
		const data_clauses = data_fields.map((f, i) => `${f} = $${i + 1}`);
	
		const parsed_filters = this.parseFilters(filters, data_fields.length);
		const params = data_fields.map((f) => data[f]).concat(parsed_filters.params);
	
		let query_str = `UPDATE ${type} SET ${data_clauses.join(',')} WHERE ${parsed_filters.clauses.join(' AND ')}`;
		if (!no_return) {
			query_str += RETURNING_STR;
		}
		return (await this.query_internal(query_str, params, conn)).rows;
	}
	
	async updateSingle(type, data, filters, conn, no_return) {
		const updated = await this.update(type, data, filters, conn, no_return);
		return updated && updated.length > 0 ? updated[0] : null;
	}
	
	async upsert(type, keys, values, conn, no_return) {
		if (!conn) conn = this.pool;
	
		const key_fields = Object.keys(keys);
		const value_fields = Object.keys(values);
		const all_fields = [...key_fields, ...value_fields];
	
		const params = [...key_fields.map((f) => keys[f]), ...value_fields.map((f) => values[f])];
		const indices = all_fields.map((f, i) => `$${i + 1}`);
		const value_clauses = value_fields.map((f, i) => `${f} = $${i + key_fields.length + 1}`);
	
		const unique_fields = [...new Set(all_fields)];
	
		let query_str = `INSERT INTO ${type}(${unique_fields.join(',')}) VALUES (${indices.slice(0, unique_fields.length).join(',')}) ON CONFLICT(${key_fields.join(',')}) DO UPDATE SET ${value_clauses.join(
			','
		)}`;
		if (!no_return) {
			query_str += RETURNING_STR;
		}
		const upserted = (await this.query_internal(query_str, params, conn)).rows;
		return upserted && upserted.length > 0 ? upserted[0] : null;
	}

	async increment(type, data, filters, conn, no_return) {
		if (!conn) conn = this.pool;
	
		const data_fields = Object.keys(data);
		const data_clauses = data_fields.map((f, i) => `${f} = ${f} + $${i + 1}`);
	
		const parsed_filters = this.parseFilters(filters, data_fields.length);
		const params = data_fields.map((f) => data[f]).concat(parsed_filters.params);
	
		let query_str = `UPDATE ${type} SET ${data_clauses.join(',')} WHERE ${parsed_filters.clauses.join(' AND ')}`;
	
		if (!no_return) query_str += RETURNING_STR;
	
		const ret_val = await this.query_internal(query_str, params, conn);
		return no_return ? ret_val.rowCount : ret_val.rows;
	}
	
	async incrementSingle(type, data, filters, conn, no_return) {
		const records = await this.increment(type, data, filters, conn, no_return);
		return no_return ? records : records && records.length > 0 ? records[0] : null;
	}

	async readQuery(text, params, retries) {
		try {
			const start_time = Date.now();
			const result = await this.pool.query(text, params);
			const total_time = Date.now() - start_time;
	
			if (config.slow_query_time && total_time > config.slow_query_time) utils.log(`Slow query: ${text}, time: ${total_time}`, 1, 'Yellow');
	
			return result;
		} catch (err) {
			utils.log(`Query failed (read-replica) [${text}], Error: ${err.message}`, 0, 'Red');
		}
	}
	
	async query(text, params, conn, retries) {
		try {
			if (!conn) conn = this.pool;
	
			const start_time = Date.now();
			const result = await conn.query(text, params);
			const total_time = Date.now() - start_time;
	
			if (config.slow_query_time && total_time > config.slow_query_time) {
				utils.log(`Slow query: ${text},  Param: ${params && params.length > 0 ? params[0] : ''}, Time: ${total_time}`, 1, 'Yellow');
			}
	
			return result;
		} catch (err) {
			utils.log(`Query failed [${text}], Error: ${err.message} Param: ${params && params.length > 0 ? params[0] : ''}`, 0, 'Red');
			return null;
		}
	}

	async transaction(callback) {
		const client = await this.pool.connect();
	
		try {
			await client.query('BEGIN');
	
			const result = await callback(client);
	
			if (result && result.error) {
				await client.query('ROLLBACK');
				return result;
			}
	
			await client.query('COMMIT');
			return result;
		} catch (e) {
			await client.query('ROLLBACK');
			throw e;
		} finally {
			client.release();
		}
	}
	
	async transaction_fake(callback) {
		return callback(this.pool);
	}

	async query_internal(text, params, conn) {
		if (!conn) conn = this.pool;
	
		try {
			const start_time = Date.now();
			const result = await conn.query(text, params);
			const total_time = Date.now() - start_time;
	
			if (config.slow_query_time && total_time > config.slow_query_time) {
				utils.log(`Slow query: ${text}, time: ${total_time}`, 1, 'Yellow');
			}
	
			return result;
		} catch (err) {
			utils.log(`Query failed [${text}], Error: ${err.message} Param: ${params && params.length > 0 ? params[0] : ''}`, 0, 'Red');
			throw err;
		}
	}

	async query(text, params, conn) {
		if(!conn) conn = this.pool;

		try {
			const start_time = Date.now();
			const result = await conn.query(text, params);
			const total_time = Date.now() - start_time;

			if(this._config.slow_query_limit && total_time > this._config.slow_query_limit)
				utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');

			return result;
		} catch(err) {
			utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
			throw err;
		}
	}
}

module.exports = { QueryBuilder };