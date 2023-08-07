import dayjs from "dayjs";
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'
import { Database } from "sqlite3";
import Net from 'net';
import sqlite3 from 'sqlite3';
import fs from "fs";
import path from "path";
import { rejects } from "assert";

dayjs.extend(utc)
dayjs.extend(timezone)

export function runQuery(dbase: Database, sql: string, params: Array<any>) {
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

export async function getMeasurementsFromEnergyMeter(currentTime: dayjs.Dayjs, energymeter: any, channels: any): Promise<any> {
    return new Promise<any>((resolve, reject) => {
        let response = '';
        const client = new Net.Socket();
        client.setTimeout(5000);
        try {
            client.connect({ port: energymeter.port, host: energymeter.ip_address }, () => {
                console.log(dayjs().format(), energymeter.ip_address, `TCP connection established with the server.`);
                client.write('read all');
            });
        } catch (err) {
            console.error(dayjs().format(), energymeter.ip_address, err);
            reject(err);
        }
        client.on('timeout', function () {
            console.error(dayjs().format(), energymeter.ip_address, "Connection timeout");
            reject(new Error("Connection timeout"));
        });

        client.on('error', function (err) {
            console.error(dayjs().format(), energymeter.ip_address, err);
            client.destroy();
            reject(err);
        });
        client.on('data', function (chunk) {
            response += chunk.toString('utf8');
            if (response.indexOf("channel_13") > 0) {
                client.end();
            }
        });

        client.on('end', async function () {
            console.log(dayjs().format(), energymeter.ip_address, "Data received from the server.");
            let db: Database | undefined = await getMeasurementsDB(energymeter.ip_address, currentTime.format("YYYY-MM") + '-monthly.sqlite', true);
            if (db) {
                try {
                    console.log(dayjs().format(), energymeter.ip_address, "Try lock DB.");
                    await runQuery(db, "BEGIN EXCLUSIVE", []);
                    console.log(dayjs().format(), energymeter.ip_address, "allowed channels:", channels.length);
                    processMeasurements(db, currentTime, energymeter.ip_address, response, channels);
                } catch (err) {
                    console.log(dayjs().format(), energymeter.ip_address, `DB access error: ${err}`);
                    reject(err);
                }
                finally {
                    try {
                        await runQuery(db, "COMMIT", []);
                    } catch (err) {
                        console.log(dayjs().format(), energymeter.ip_address, `Commit transaction error: ${err}`);
                        reject(err)
                    }
                    console.log(dayjs().format(), energymeter.ip_address, 'Closing DB connection...');
                    db.close();
                    console.log(dayjs().format(), energymeter.ip_address, 'DB connection closed.');
                    console.log(dayjs().format(), energymeter.ip_address, 'Closing TCP connection...');
                    client.destroy();
                    console.log(dayjs().format(), energymeter.ip_address, 'TCP connection destroyed.');
                    resolve(true);
                }
            } else {
                console.error(dayjs().format(), energymeter.ip_address, "No database exists.");
                reject(new Error("No database exists."));
            }
        });
    });
}

export async function getMeasurementsDB(IPAddress: string, fileName: string, create: boolean): Promise<Database | undefined> {
    let db: Database | undefined = undefined;
    const dbFilePath = getDBFilePath(IPAddress);
    if (!fs.existsSync(dbFilePath) && create) {
        fs.mkdirSync(dbFilePath, { recursive: true });
        console.log(dayjs().format(), `Directory '${dbFilePath}' created.`);
    }
    const dbFileName = path.join(dbFilePath, fileName);
    if (!fs.existsSync(dbFileName)) {
        if (create) {
            db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            console.log(dayjs().format(), `DB file '${dbFileName}' created.`);
            const result = await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
            console.log(dayjs().format(), "Measurements table created:", result);
        }
    } else {
        console.log(dayjs().format(), `DB file '${dbFileName}' opened.`);
        db = new Database(dbFileName, sqlite3.OPEN_READWRITE);
    }
    return db;
}

export function processMeasurements(db: Database, currentTime: dayjs.Dayjs, ip_address: string, response: string, channels: String[]) {
    let currentTimeRoundedToHour = dayjs(currentTime).set("minute", 0).set("second", 0).set("millisecond", 0);
    console.log(dayjs().format(), ip_address, "currentTimeRoundedToHour:", currentTimeRoundedToHour.format());
    //console.log(dayjs().format(), "received response:", response);
    response.split('\n').forEach((line) => {
        let matches = line.match(/^channel_(\d{1,2}) : (.*)/);
        if (matches && channels.includes(matches[1])) {
            let measuredValue = parseFloat(matches[2]) * 1000;
            db.exec(`INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (${matches[1]}, ${measuredValue}, ${currentTimeRoundedToHour.unix()})`);
            console.log(dayjs().format(), ip_address, matches[1], matches[2]);
        }
    });
}

export async function getMeasurementsFromDBs(fromDate: dayjs.Dayjs, toDate: dayjs.Dayjs, ip: string, channel?: number): Promise<any[]> {
    let monthlyIterator = dayjs(fromDate).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
    let result: any[] = [];
    while (monthlyIterator.isBefore(toDate) || monthlyIterator.isSame(toDate)) {
        const filePath = (process.env.WORKDIR as string);
        const dbFile = path.join(filePath, ip, monthlyIterator.format("YYYY-MM") + "-monthly.sqlite");
        if (fs.existsSync(dbFile)) {
            const db = new Database(dbFile);
            try {
                const fromSec = fromDate.unix();
                const toSec = toDate.unix();
                let filters = [fromSec, toSec];
                if (channel) {
                    filters.push(channel);
                }
                let measurements = await runQuery(db, "select * from measurements where recorded_time between ? and ? " + (channel ? "and channel=?" : "") + " order by recorded_time, channel", filters);
                measurements.forEach((element: any) => {
                    result.push(element);
                })
            } catch (err) {
                console.error(dayjs().format(), err);
            } finally {
                db.close();
            }
        }
        monthlyIterator = monthlyIterator.add(1, "months");
    }

    return result;
}

export function getDetails(measurements: any[], timeZone: string, details: string, addFirst: boolean) {
    let result: any[] = [];
    let prevElement: any = {};
    let lastElement: any = {};
    const isHourlyEnabled = details == 'hourly';
    const isDaily = details == 'daily';
    const isMonthly = details == 'monthly';
    let isAddableEntry = false;
    measurements.forEach((element: any, idx: number) => {
        if (prevElement[element.channel] == undefined) {
            prevElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel, diff: 0 };
            if (addFirst) {
                result.push({ ...prevElement[element.channel] });
            }
        } else {
            const roundedPrevDay = dayjs.unix(prevElement[element.channel].recorded_time).tz(timeZone).set("hour", 0).set("minute", 0).set("second", 0);
            const roundedDay = dayjs.unix(element.recorded_time).tz(timeZone).set("hour", 0).set("minute", 0).set("second", 0);
            const diffDays = roundedDay.diff(roundedPrevDay, "days");
            const isDailyEnabled = isDaily && diffDays >= 1;

            const roundedPrevMonth = dayjs.unix(prevElement[element.channel].recorded_time).tz(timeZone).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0);
            const roundedMonth = dayjs.unix(element.recorded_time).tz(timeZone).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0);
            const diffMonths = roundedMonth.diff(roundedPrevMonth, "months");
            const isMonthlyEnabled = isMonthly && diffMonths >= 1;
            isAddableEntry = isHourlyEnabled || isDailyEnabled || isMonthlyEnabled;
            if (isAddableEntry) {
                prevElement[element.channel] = {
                    recorded_time: element.recorded_time,
                    from_utc_time: dayjs.unix(prevElement[element.channel].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    to_utc_time: dayjs.unix(element.recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    from_server_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_server_time: dayjs.unix(element.recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    from_local_time: dayjs.unix(prevElement[element.channel].recorded_time).format("YYYY-MM-DD HH:mm:ss"),
                    to_local_time: dayjs.unix(element.recorded_time).format("YYYY-MM-DD HH:mm:ss"),
                    measured_value: element.measured_value,
                    channel: element.channel,
                    diff: element.measured_value - prevElement[element.channel].measured_value
                };
                result.push({ ...prevElement[element.channel] });
            }

            lastElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel };
        }
    });
    if (!isAddableEntry) {
        Object.keys(lastElement).forEach((key) => {
            try {
                const diff = lastElement[key].measured_value - prevElement[lastElement[key].channel].measured_value;
                prevElement[lastElement[key].channel] = {
                    recorded_time: lastElement[key].recorded_time,
                    from_utc_time: dayjs.unix(prevElement[lastElement[key].channel].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    to_utc_time: dayjs.unix(lastElement[key].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    from_server_time: dayjs.unix(prevElement[lastElement[key].channel].recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_server_time: dayjs.unix(lastElement[key].recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    from_local_time: dayjs.unix(lastElement[key].recorded_time).format("YYYY-MM-DD HH:mm:ss"),
                    to_local_time: dayjs.unix(lastElement[key].recorded_time).format("YYYY-MM-DD HH:mm:ss"),
                    measured_value: lastElement[key].measured_value,
                    channel: lastElement[key].channel,
                    diff: diff
                };

                result.push({ ...prevElement[lastElement[key].channel] });

            } catch (err) {
                console.error(dayjs().format(), err);
            }
        });
    }
    return result;
}

export function getDBFilePath(IPAddress: string): string {
    const dbFilePath = path.join(process.env.WORKDIR as string, IPAddress);
    return dbFilePath;
}