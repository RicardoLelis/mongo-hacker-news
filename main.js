const ndjson = require("ndjson");
const through2 = require("through2");
const filter = require("through2-filter");
const request = require("request");
const sentiment = require("sentiment");
const { MongoClient } = require("mongodb");
const stream = require("stream");
const util = require("util");

const pipeline = util.promisify(stream.pipeline);

(async () => {
    const client = new MongoClient(
        "mongodb+srv://<user>:<pwd>@cluster0.ozsep.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",
        {
            useUnifiedTopology: true
        }
    )
    const textRank = new sentiment();
    try {
        await client.connect();
        const collection = client.db("hacker-news").collection("mentions")
        await pipeline(
            request("http://api.hnstream.com/comments/stream/"),
            ndjson.parse({ strict: false }),
            filter({ objectMode: true }, chunk => {
                return chunk["body"].toLowerCase().includes("python") || chunk["article-title"].toLowerCase().includes("python");
            }),
            through2.obj((row, enc, next) => {
                let result = textRank.analyze(row.body);
                row.score = result.score;
                next(null, row);
            }),
            through2.obj((row, enc, next) => {
                collection.insertOne({
                    ...row,
                    "user-url": `https://news.ycombinator.com/user?id=${row["author"]}`,
                    "item-url": `https://news.ycombinator.com/item?id=${row["article-id"]}`
                });
                next();
            })
        );
        console.log("FINISHED");
    } catch (error) {
        console.log(error)
    }
})();