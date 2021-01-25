import { Utils, IDictionary } from './utils';
import { Config } from './config';
import { Pool, PoolClient } from 'pg';

export class DbConnection {
	static pool: Pool;
	static slow_query_limit: number = 0;

	static init(config: Config) {
		DbConnection.pool = new Pool({
			user: config.connection.user,
			host: config.connection.host,
			database: config.connection.database,
			password: config.connection.password,
			port: config.connection.port
		});

		DbConnection.slow_query_limit = config.slow_query_limit;
	}

	static select() { return new Query(QueryType.Select); }
	static update() { return new Query(QueryType.Update); }
	static insert() { return new Query(QueryType.Insert); }
}

export class Query {
	type: QueryType;

	constructor(type: QueryType) {
		this.type = type;
	}
}

enum QueryType {
	Select = "SELECT",
	Update = "UPDATE",
	Insert = "INSERT"
}