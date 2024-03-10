require('dotenv').config();

const http = require('http');
const https = require('https');
const url = require('url');
const { createClient } = require("redis");
const zlib = require("zlib");
const cluster = require('cluster');
const os = require('os');

// Configuration
const config = {
    API_KEY: process.env.API_KEY,
    SPORTS_FOOTBALL_URL_V2: process.env.SPORTS_FOOTBALL_URL_V2,
    SPORTS_FOOTBALL_URL_V3: process.env.SPORTS_FOOTBALL_URL_V3,
    SPORTS_CRICKET_URL_V2: process.env.SPORTS_CRICKET_URL_V2,
    REDIS_PASSWORD: process.env.REDIS_PASSWORD,
    REDIS_HOST: process.env.REDIS_HOST,
    REDIS_PORT: process.env.REDIS_PORT,
};

let redisClient = undefined;

async function initializeRedisClient() {
    try {
        const redisPassword = config.REDIS_PASSWORD;
        const redisHost = config.REDIS_HOST;
        const redisPort = config.REDIS_PORT;

        if (redisPassword && redisHost && redisPort) {
            // Create the Redis client object
            redisClient = createClient({
                password: redisPassword,
                socket: {
                    host: redisHost,
                    port: redisPort
                }
            });

            // Handle connection errors
            redisClient.on("error", error => {
                console.error(`Redis client error:`);
                console.error(error.message);
            });

            // Connect to the Redis server
            await redisClient.connect();
            console.log(`Connected to Redis successfully!`);
        } else {
            console.warn(`Missing Redis configuration. Cannot initialize Redis client.`);
        }
    } catch (error) {
        console.error(`Failed to initialize Redis client:`);
        console.error(error.message);

        // Close the Redis client on failure
        if (redisClient) {
            await redisClient.quit();
            console.log(`Closed Redis client.`);
        }
    }
}



function isRedisWorking() {
    // Verify whether there is an active connection to a Redis server or not
    return !!redisClient?.isOpen;
}


// Request Handler
const requestHandler = {
    handleFootballV2Request: (path, res) => {
        const apiUrl = `${config.SPORTS_FOOTBALL_URL_V2}${path.replace("/football-v2", "")}?api_token=${config.API_KEY}`;
        console.log("API URL", apiUrl)
        fetchDataFromApi(apiUrl, res);
    },
    handleFootballV3Request: (path, res) => {
        const apiUrl = `${config.SPORTS_FOOTBALL_URL_V3}${path.replace("/football-v3", "")}?api_token=${config.API_KEY}`;
        fetchDataFromApi(apiUrl, res);
    },
    handleCricketV2Request: (path, res) => {
        const apiUrl = `${config.SPORTS_CRICKET_URL_V2}${path.replace("/cricket-v2", "")}?api_token=${config.API_KEY}`;
        console.log("API URL", apiUrl)
        fetchDataFromApi(apiUrl, res);
    },
};

// Fetch Data from API
async function fetchDataFromApi(apiUrl, res) {
    try {
        const cachedData = await readData(apiUrl, true);

        console.log("Cached Data")
        if (cachedData) {
            console.log("Cache Hit");
            res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(cachedData);
        } else {
            console.log("Cache Miss");
            const data = await fetchFromApi(apiUrl);

            const options = { EX: 60, NX: false };

            await writeData(apiUrl, data, options, true); // Cache for 1 hour
            res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(data);
        }
    } catch (error) {
        console.error(`Error fetching data: ${error.message}`);
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Internal Server Error');
    }
}

// Fetch Data from External API
function fetchFromApi(apiUrl) {
    return new Promise((resolve, reject) => {
        https.get(apiUrl, { headers: { 'Content-Type': 'application/json', 'api_token': config.API_KEY } }, (apiRes) => {
            let data = '';
            apiRes.on('data', (chunk) => {
                data += chunk;
            });
            apiRes.on('end', () => {
                resolve(data);
            });
        }).on('error', (error) => {
            reject(error);
        });
    });
}

// Redis Operations
async function writeData(key, data, options, compress) {
    if (isRedisWorking()) {
        let dataToCache = data;
        if (compress) {
            // compress the value with ZLIB to save RAM
            dataToCache = zlib.deflateSync(data).toString("base64");
        }

        try {
            await redisClient.set(key, dataToCache, options);
        } catch (e) {
            console.error(`Failed to cache data for key=${key}`, e);
        }
    }
}

async function readData(key, compressed) {
    let cachedValue = undefined;
    if (isRedisWorking()) {
        cachedValue = await redisClient.get(key);
        if (cachedValue) {
            if (compressed) {
                // decompress the cached value with ZLIB
                return zlib.inflateSync(Buffer.from(cachedValue, "base64")).toString();
            } else {
                return cachedValue;
            }
        }
    }

    return cachedValue;
}


if (cluster.isMaster) {
    console.log(`Master ${process.pid} is running`);

    // Fork workers.
    for (let i = 0; i < os.cpus().length; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });
} else {

    // Server
    const server = http.createServer(async (req, res) => {

        await initializeRedisClient()

        const parsedUrl = url.parse(req.url, true);
        const pathname = parsedUrl.pathname;
        const startPoint = pathname.split('/')[1];

        if (startPoint === 'football-v2') {
            requestHandler.handleFootballV2Request(pathname, res);
        } else if (startPoint === 'football-v3') {
            requestHandler.handleFootballV3Request(pathname, res);
        } else if (startPoint === 'cricket-v2') {
            requestHandler.handleCricketV2Request(pathname, res);
        } else {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('Not Found');
        }
    });

    const PORT = 5678;
    server.listen(PORT, () => {
        console.log(`Server is listening on port ${PORT}`);
    });

}

