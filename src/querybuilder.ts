import { Utils, IDictionary } from './utils';
import { Config } from './config';
import { Pool, PoolClient } from 'pg';

export interface IConnection extends PoolClient {};

export class QueryBuilder {
	pool: Pool;
	slow_query_limit: number = 0;

	constructor(config: Config) {
		this.pool = new Pool({
			user: config.connection.user,
			host: config.connection.host,
			database: config.connection.database,
			password: config.connection.password,
			port: config.connection.port
		});

		this.slow_query_limit = config.slow_query_limit;
	}

	async query<T>(c: new(data: any) => T, text: string, params: Array<any>, conn?: PoolClient) : Promise<T[]> {
		if(!conn)
			conn = await this.pool.connect();
	
		try {
			let start_time = Date.now();
			let result = await conn.query(text, params);
			let total_time = Date.now() - start_time;
	
			if(this.slow_query_limit && total_time > this.slow_query_limit)
				Utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');
	
			return result.rows.map(r => new c(r));
		} catch(err) {
			Utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
			throw err;
		}
	}

	async valueQuery(text: string, params: Array<any>, conn?: PoolClient) : Promise<any> {
		if(!conn)
			conn = await this.pool.connect();
	
		try {
			let start_time = Date.now();
			let result = await conn.query(text, params);
			let total_time = Date.now() - start_time;
	
			if(this.slow_query_limit && total_time > this.slow_query_limit)
				Utils.log('Slow query: ' + text + ', time: ' + total_time, 1, 'Yellow');
	
			return result.rows[0];
		} catch(err) {
			Utils.log('Query failed [' + text + '], Error: ' + err.message + ' Param: ' + (params && params.length > 0 ? params[0] : ''), 0, 'Red');
			throw err;
		}
	}

	async count(type: string, filters: IDictionary<any>, conn?: PoolClient) : Promise<number> {
		if(!conn)
			conn = await this.pool.connect();
	
		let _filters = new Filters(filters);
	
		let query_str = 'SELECT COUNT(*) as "count" FROM ' + type + ' WHERE ' + _filters.clauses.join(' AND ');
		return (await this.valueQuery(query_str, _filters.params, conn)).count;
	}
	
	async sum(type: string, sum_column: string, filters: IDictionary<any>, conn?: PoolClient) {
		if(!conn)
			conn = await this.pool.connect();

		let _filters = new Filters(filters);

		let query_str = 'SELECT SUM(' + sum_column + ') as "sum" FROM ' + type + ' WHERE ' + _filters.clauses.join(' AND ');
		return (await this.valueQuery(query_str, _filters.params, conn)).sum;
	}

	async transaction(callback: (client: PoolClient) => Promise<any>) {
		const client = await this.pool.connect();
	
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
}

class Filters {
	params: Array<any>;
	clauses: Array<string>;

	constructor(filters: IDictionary<any>, start: number = 0) {
		this.params = this.parseParams(filters);
		this.clauses = this.parseClauses(filters, start);
	}

	private parseParams(filters: IDictionary<any>) : Array<any> {
		let params: Array<any> = [];
	
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

	private parseClauses(filters: IDictionary<any>, start: number = 0, param_index: number = 0) : Array<string> {	
		return Object.keys(filters).map(f => {
			if(filters[f] == null)
				return f + ' IS NULL';
			else if(f.startsWith('_') && filters[f] != null && typeof filters[f] == 'object') {
				return '(' + this.parseClauses(filters[f], start, param_index).join(' OR ') + ')';
			} else if(Array.isArray(filters[f])) {
				let val = f + ' IN ' + this.inClause(start + ++param_index, filters[f]);
				param_index += filters[f].length - 1;
				return val
			} else if(typeof filters[f] == 'object' && filters[f].op) {
				return `${f} ${filters[f].op} $${start + ++param_index}`;
			} else
				return f + ' = $' + (start + ++param_index);
		});
	}

	private inClause(start: number, length: number) : string {
		// I'm sure there's a better way to do this but I don't know how...
		let in_clause = '(';
		for(let i = 0; i < length; i++)
			in_clause += '$' + (i + start) + ((i < length - 1) ? ',' : '');
		in_clause += ')';
	
		return in_clause;
	}
}