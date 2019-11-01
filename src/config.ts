export interface Config
{  
	slow_query_limit: number;
	connection: DbConnection;
}

export interface DbConnection
{
	user: string;
	host: string;
	database: string;
	password: string;
	port: number;
}