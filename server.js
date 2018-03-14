const db = require('mongodb');
const async = require('async');
const mongoClient = db.MongoClient;
const uri = 'mongodb://localhost:27017/nba_players';
//Invoke the first data source
const players = require('./nba_players.json');
const player_ages = require('./nba_players_age.json');
//Invoke the tasks that execute a certain number of tasks.
let tasks = [];
//Have the limit on the number of tasks.
const limit = parseInt(process.argv[2], 5) || 25;

mongoClient.connect(uri, (err, db) => {
    if(err) return process.exit(1);
    //Invoke a foreach loop that loops through the array of json objects of players
    //With three args on for player which is the individual player object, index the index in position of the array, 
    //list is 
    players.forEach((player, index, list) => {
        //WHen merging one object use first arg as obj you would like to merge, and the second arg as the data being merged to.
        players[index] = Object.assign(player, player_ages[index]);
        if(index % limit) {
            //Have the start equal to the index. 
            const start = index;
            //Use a terinary operation on the end constant
            const end = (players.length < start + limit ? players.length - 1: start + limit);
            //Push values to the array using a callback.
            tasks.push((done) => {
                console.log(`Processing ${start} - ${end} out of ${players.length}`)
                db.collection('ranking').insert(players.slice(start, end), (err, results) => {
                    if(err) return process.exit(1);
                    ///Callback error and results.
                    done(err, results);
                });
            });

        }
        
    })
    //Show how many tasks be logs by getting array length.
    console.log(`Launching this many tasks ${tasks.length}`);
    const startTime = Date.now();
    async.parallel(tasks, (err, results) => {
        if(err) console.error(err);
        //Invoke the end time, by adding time now.
        const endTime = Date.now();
        //Log the time elapsed
        console.log(`Time Elapsed: ${endTime - startTime}`);
        //Log out of the results
        console.log(results);
        db.close();
    })
});