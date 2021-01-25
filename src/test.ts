import { Config } from './config';
let config: Config = require('../config.json');
import { QueryBuilder, IConnection } from './querybuilder';

class Player {
	name: string;
	rating: number;

	constructor(data: any) {
		this.name = data.name;
		this.rating = data.rating;
	}

	test() { return this.name + ' - ' + this.rating; }
}

start();

async function start() {
	let db = new QueryBuilder(config);

	//console.log(db.parseParams({ test: 1, moo: 'cheese', bob: [1, 2, 4] }));

	let result = await db.transaction(async (client: IConnection) : Promise<any> => {
		let result = await db.query<Player>(Player, "SELECT * FROM players WHERE name = $1", ['yabapmatt'], client);
		console.log(result);

		let count = await db.count('players', { rating: { op: '>', value: 0 } }, client);

		return count;
	});

	console.log(result);
}
