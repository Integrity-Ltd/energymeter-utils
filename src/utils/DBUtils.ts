import moment from "moment";
import { Database } from "sqlite3";
import Net from 'net';
import sqlite3 from 'sqlite3';
import fs from "fs";
import path from "path";

function runQuery(dbase: Database, sql: string, params: Array<any>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}

function getMeasurementsFromEnergyMeter(energymeter: any, channels: any) {
    let response = '';
    const client = new Net.Socket();
    try {
        client.connect({ port: energymeter.port, host: energymeter.ip_address }, () => {
            console.log(moment().format(), energymeter.ip_address, `TCP connection established with the server.`);
            client.write('read all');
        });
    } catch (err) {
        console.error(moment().format(), err);
    }
    client.on('error', function (err) {
        console.error(moment().format(), err);
        client.destroy();
    });
    client.on('data', function (chunk) {
        response += chunk.toString('utf8');
        if (response.indexOf("channel_13") > 0) {
            client.end();
        }
    });

    client.on('end', async function () {
        console.log(moment().format(), energymeter.ip_address, "Data received from the server.");
        let db: Database | undefined = await getMeasurementsDB(energymeter.ip_address, moment().format("YYYY-MM") + '-monthly.sqlite', true);
        if (!db) {
            console.error(moment().format(), energymeter.ip_address, "No database exists.");
            return;
        }
        try {
            console.log(moment().format(), energymeter.ip_address, "Try lock DB.");
            await runQuery(db, "BEGIN EXCLUSIVE", []);
            console.log(moment().format(), energymeter.ip_address, "allowed channels:", channels.length);
            processMeasurements(db, energymeter.ip_address, response, channels);
        } catch (err) {
            console.log(moment().format(), energymeter.ip_address, `DB access error: ${err}`);
        }
        finally {
            try {
                await runQuery(db, "COMMIT", []);
            } catch (err) {
                console.log(moment().format(), energymeter.ip_address, `Commit transaction error: ${err}`);
            }
            console.log(moment().format(), energymeter.ip_address, 'Closing DB connection...');
            db.close();
            console.log(moment().format(), energymeter.ip_address, 'DB connection closed.');
            console.log(moment().format(), energymeter.ip_address, 'Closing TCP connection...');
            client.destroy();
            console.log(moment().format(), energymeter.ip_address, 'TCP connection destroyed.');
        }

    });
}

async function getMeasurementsDB(IPAddress: string, fileName: string, create: boolean): Promise<Database | undefined> {
    let db: Database | undefined = undefined;
    const dbFilePath = getDBFilePath(IPAddress);
    if (!fs.existsSync(dbFilePath) && create) {
        fs.mkdirSync(dbFilePath, { recursive: true });
        console.log(moment().format(), `Directory '${dbFilePath}' created.`);
    }
    const dbFileName = path.join(dbFilePath, fileName);
    if (!fs.existsSync(dbFileName)) {
        if (create) {
            db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            console.log(moment().format(), `DB file '${dbFileName}' created.`);
            await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
        }
    } else {
        console.log(moment().format(), `DB file '${dbFileName}' opened.`);
        db = new Database(dbFileName, sqlite3.OPEN_READWRITE);
    }
    return db;
}

function processMeasurements(db: Database, ip_address: string, response: string, channels: String[]) {
    let currentUnixTimeStampRoundedToHour = moment().set("minute", 0).set("second", 0).set("millisecond", 0).unix();
    //console.log(moment().format(), "received response:", response);
    response.split('\n').forEach((line) => {
        let matches = line.match(/^channel_(\d{1,2}) : (.*)/);
        if (matches && channels.includes(matches[1])) {
            let measuredValue = parseFloat(matches[2]) * 1000;
            db.exec(`INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (${matches[1]}, ${measuredValue}, ${currentUnixTimeStampRoundedToHour})`);
            console.log(moment().format(), ip_address, matches[1], matches[2]);
        }
    });
}

function getDBFilePath(IPAddress: string): string {
    const dbFilePath = path.join(process.env.WORKDIR as string, IPAddress);
    return dbFilePath;
}

export default { runQuery, getMeasurementsFromEnergyMeter, getMeasurementsDB, processMeasurements, getDBFilePath };