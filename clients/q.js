#! /usr/bin/env node

const crypto = require('crypto');
const http = require('http');

const trimQuery = sql => sql.trim().replace(/(?:\r\n|\r|\n)/g, " ");

const hash = sql => crypto.createHash('sha256').update(sql).digest('hex');

const hostname = 'localhost';
const port = 8080
const pathPrefix = '/q'

const readQuery = query => {
    const options = {
      hostname,
      port,
      path: `${pathPrefix}/${hash(query)}.csv`,
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'X-Seafowl-Query': encodeURIComponent(query)
      }
    };

    const req = http.request(options, res => {
      console.log(`statusCode: ${res.statusCode}`);

      res.on('data', d => {
        process.stdout.write(d);
      });
    });

    req.on('error', error => {
      console.error(error);
    });

    req.end();
};

const writeQuery = query => {
    const data = JSON.stringify({query});
    const options = {
      hostname,
      port,
      path: pathPrefix,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
      }
    };

    const req = http.request(options, res => {
      console.log(`statusCode: ${res.statusCode}`);

      res.on('data', d => {
        process.stdout.write(d);
      });
    });

    req.write(data)

    req.on('error', error => {
      console.error(error);
    });

    req.end();
};

if (require.main === module) {
    const args = process.argv.slice(2)
    if (args[0]?.trim() === '-w') {
        writeQuery(args.slice(1).join(" "))
    } else {
        readQuery(args.join(" "))
    }
}

