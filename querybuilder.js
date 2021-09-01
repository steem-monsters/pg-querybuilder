const { Pool } = require('pg');
const utils = require('./utils');
const { QueryBuilder } = require('./querybuilder2');

let pool = null;
let _config = {};

function init(config) {
	_config = config;

	pool = new Pool({
		user: config.connection.user,
		host: config.connection.host,
		database: config.connection.database,
		password: config.connection.password,
		port: config.connection.port
	});
}

async function count(type, filters, conn) {
	if(!conn)
		conn = pool;

	let parsed_filters = parseFilters(filters);

	const query_str = 'SELECT COUNT(*) as "count" FROM ' + type + ' WHERE ' + parsed_filters.clauses.join(' AND ');
	return (await query(query_str, parsed_filters.params, conn)).rows[0].count;
}

async function sum(type, sum_column, filters, conn) {
	if(!conn)
		conn = pool;

	let parsed_filters = parseFilters(filters);

	const query_str = 'SELECT SUM(' + sum_column + ') as "sum" FROM ' + type + ' WHERE ' + parsed_filters.clauses.join(' AND ');
	return (await query(query_str, parsed_filters.params, conn)).rows[0].sum;
}

function parseFilters(filters, start) {
	return { params: parseParams(filters), clauses: parseClauses(filters, start) };
}

function parseClauses(filters, start, param_index) {
	if(!start)
		start = 0;

	if(!param_index)
		param_index = 0;

	return Object.keys(filters).map(f => {
		if(filters[f] == null)
			return f + ' IS NULL';
		else if(f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
			return '(' + parseClauses(filters[f], start, param_index).join(' OR ') + ')';
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

function parseParams(filters) {
	var params = [];

	Object.keys(filters).forEach(f => {
		if(Array.isArray(filters[f]))
			params = params.concat(filters[f]);
		else if(f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
			params = params.concat(parseParams(filters[f]));
		} else if(filters[f] != null && typeof filters[f] == 'object' && filters[f].op) {
			params.push(filters[f].value);
		} else if(filters[f] != null)
			params.push(filters[f])
	});

	return params;
}

async function lookup(type, options, conn) {
	try {
		if(!conn)
			conn = pool;

		if(!options)
			options = {};

		var params = [];
		var query_str = 'SELECT * FROM ' + type;

		if(options.filters && Object.keys(options.filters).length > 0) {
			var filters = parseFilters(options.filters);
			params = filters.params;
			query_str += ' WHERE ' + filters.clauses.join(' AND ');
		}

		if(options.sort_by) {
			query_str += ' ORDER BY ' + options.sort_by;

			if(options.sort_descending)
				query_str += ' DESC'
		}

		if(options.limit)
			query_str += ' LIMIT ' + options.limit;

		if(options.offset)
			query_str += ' OFFSET ' + options.offset;

		return (await query(query_str, params, conn)).rows;
	} catch (err) {
		utils.log(err.message, 1, 'Red');
		utils.log(err.stack, 1, 'Red');
	}
}

async function lookupSingle(type, filters, conn) {
	var records = await lookup(type, { filters: filters }, conn);
	return (records && records.length > 0) ? records[0] : null;
}

async function insert(type, data, conn) {
	if(!conn)
		conn = pool;

	var fields = Object.keys(data);
	var params = fields.map(f => data[f]);
	var indices = fields.map((f, i) => '$' + (i + 1));

	return (await query('INSERT INTO ' + type + '(' + fields.join(',') + ') VALUES (' + indices.join(',') + ') RETURNING *', params, conn)).rows[0];
}

async function insertMultiple(type, data, conn) {
	if(!conn)
		conn = pool;

	var fields = Object.keys(data[0]);
	var params = [];
	var values = '';

	for(var i = 0; i < data.length; i++) {
		var obj = data[i];
		params = params.concat(fields.map(f => obj[f]));
		var indices = fields.map((f, d) => '$' + (i * fields.length + d + 1));
		values += (i > 0 ? ',' : '') + '(' + indices.join(',') + ')';
	}
	
	return (await query('INSERT INTO ' + type + '(' + fields.join(',') + ') VALUES ' + values + ' RETURNING *', params, conn)).rows;
}

async function deleteRows(type, filters, conn) {
	if(!conn)
		conn = pool;

	var parsed_filters = parseFilters(filters);

	var query_str = 'DELETE FROM ' + type + ' WHERE ' + parsed_filters.clauses.join(' AND ') + ' RETURNING *';
	return (await query(query_str, parsed_filters.params, conn)).rows;
}

async function deleteSingle(type, filters, conn) {
	var deleted = await deleteRows(type, filters, conn);
	return (deleted && deleted.length > 0) ? deleted[0] : null;
}

async function update(type, data, filters, conn) {
	if(!conn)
		conn = pool;

	var data_fields = Object.keys(data);
	var data_clauses = data_fields.map((f, i) => f + ' = $' + (i + 1));

	var parsed_filters = parseFilters(filters, data_fields.length);
	var params = data_fields.map(f => data[f]).concat(parsed_filters.params);

	var query_str = 'UPDATE ' + type + ' SET ' + data_clauses.join(',') + ' WHERE ' + parsed_filters.clauses.join(' AND ') + ' RETURNING *';
	return (await query(query_str, params, conn)).rows;
}

async function updateSingle(type, data, filters, conn) {
	var updated = await update(type, data, filters, conn);
	return (updated && updated.length > 0) ? updated[0] : null;
}

async function upsert(type, keys, values, conn) {
	if(!conn)
		conn = pool;

	let key_fields = Object.keys(keys);
	let value_fields = Object.keys(values);
	let all_fields = [...key_fields, ...value_fields];
	
	var params = [...key_fields.map(f => keys[f]), ...value_fields.map(f => values[f])];
	var indices = all_fields.map((f, i) => '$' + (i + 1));
	var value_clauses = value_fields.map((f, i) => f + ' = $' + (i + key_fields.length + 1));

	let unique_fields = [...new Set(all_fields)];

	let query_str = `INSERT INTO ${type}(${unique_fields.join(',')}) VALUES (${indices.slice(0, unique_fields.length).join(',')}) ON CONFLICT(${key_fields.join(',')}) DO UPDATE SET ${value_clauses.join(',')} RETURNING *`;
	let upserted = (await query(query_str, params, conn)).rows;
	return (upserted && upserted.length > 0) ? upserted[0] : null;
}

async function increment(type, data, filters, conn) {
	if(!conn)
		conn = pool;

	var data_fields = Object.keys(data);
	var data_clauses = data_fields.map((f, i) => f + ' = ' + f + ' + $' + (i + 1));

	var filter_fields = Object.keys(filters);
	var filter_clauses = filter_fields.map((f, i) => f + ' = $' + (i + data_fields.length + 1));

	var params = data_fields.map(f => data[f]).concat(filter_fields.map(f => filters[f]));

	var query_str = 'UPDATE ' + type + ' SET ' + data_clauses.join(',') + ' WHERE ' + filter_clauses.join(' AND ') + ' RETURNING *';
	return (await query(query_str, params, conn)).rows;
}

async function incrementSingle(type, data, filters, conn) {
	var records = await increment(type, data, filters, conn);
	return (records && records.length > 0) ? records[0] : null;
}

async function transaction(callback) {
  const client = await pool.connect();

	try {
		await client.query('BEGIN');

		let result = await callback(client);

		if(result && result.error) {
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

async function query(text, params, conn) {
	if(!conn)
		conn = pool;

	try {
		var start_time = Date.now();
		var result = await conn.query(text, params);
		var total_time = Date.now() - start_time;

		if(_config.slow_query_limit && total_time > _config.slow_query_limit)
			utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');

		return result;
	} catch(err) {
		utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
		throw err;
	}
}

module.exports = {
	init,
	query,
	pool,
	lookup,
	lookupSingle,
	insert,
	count,
	update,
	updateSingle,
	upsert,
	increment,
	incrementSingle,
	insertMultiple,
	deleteRows,
	sum,
	transaction,
	deleteSingle,
	QueryBuilder
}