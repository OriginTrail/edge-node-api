const externalSequelize = require('./runtime-node-sequelize-connection');
const { QueryTypes } = require('sequelize');
const sequelize = require('sequelize');
const { SyncedAsset, Notification } = require('./models');
const { Queue, Worker } = require('bullmq');
const { BullMQOtel } = require('bullmq-otel');
const redis = require('ioredis');
const axios = require('axios');
const connection = new redis({
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
    db: process.env.REDIS_DB,
    maxRetriesPerRequest: null
});

const mockWait = ms => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

const syncQueue = new Queue('syncQueue', {
    connection,
    telemetry: new BullMQOtel('sync-assets-queue')
});

new Worker(
    'syncQueue',
    async job => {
        console.log(`Starting sync job...Time: ${job.data.timestamp}`);
        console.log(
            `Checking Edge node publish mode...Time: ${job.data.timestamp}`
        );

        const publicConfig = await axios.get(
            `${process.env.AUTH_SERVICE_ENDPOINT}/params/public`
        );
        const edgeNodePublishMode =
            publicConfig.data.config.find(
                item => item.option === 'edge_node_publish_mode'
            ).value || null;

        if (edgeNodePublishMode === 'public') {
            console.log(
                `Edge node publish mode is public, aborting sync operation...Time: ${job.data.timestamp}`
            );
            return;
        }

        const paranetUAL =
            publicConfig.data.config.find(
                item => item.option === 'edge_node_paranet_ual'
            ).value || null;

        try {
            const internalSyncedAssets = await SyncedAsset.findAll({
                where: {
                    paranet_ual: paranetUAL
                }
            });
            if (internalSyncedAssets.length === 0) {
                console.log(`First time query...`);
                const assets = await externalSequelize.query(
                    getInitialQuery(paranetUAL),
                    {
                        type: QueryTypes.SELECT
                    }
                );
                if (assets.length > 0) {
                    let notification = await storeNotification(assets);
                    await storeSyncedAssets(assets, notification);
                }
            } else if (internalSyncedAssets.length > 0) {
                const lastSyncedAsset = await SyncedAsset.findOne({
                    where: {
                        paranet_ual: paranetUAL
                    },
                    order: [['id', 'DESC']]
                });
                const date = new Date(lastSyncedAsset.backend_synced_at);
                const utcDateString = date
                    .toISOString()
                    .replace('Z', '')
                    .replace('T', ' ')
                    .slice(0, 19);
                const assets = await externalSequelize.query(
                    getNextQuery(utcDateString, paranetUAL),
                    {
                        type: QueryTypes.SELECT
                    }
                );
                if (assets.length > 0) {
                    let notification = await storeNotification(assets);
                    await storeSyncedAssets(assets, notification);
                }
            }
        } catch (error) {
            console.error(error);
        }
        console.log(`Sync job completed.Time: ${job.data.timestamp}`);
    },
    {
        connection,
        concurrency: 1, // Ensure only one job runs at a time
        telemetry: new BullMQOtel('sync-assets-queue')
    }
);

const getInitialQuery = paranetUAL => {
    return `
        SELECT sa.*
        FROM paranet_synced_asset sa
                 INNER JOIN (
            SELECT ual, MAX(id) AS max_id
            FROM paranet_synced_asset
            WHERE (ual, created_at) IN
                  (SELECT ual, MAX(created_at)
                   FROM paranet_synced_asset
                   GROUP BY ual)
              AND paranet_ual = '${paranetUAL}'
            GROUP BY ual
        ) latest ON sa.id = latest.max_id;`;
};

const getNextQuery = (date, paranetUAL) => {
    return `
        SELECT sa.*
        FROM paranet_synced_asset sa
                 INNER JOIN (
            SELECT ual, MAX(id) AS max_id
            FROM paranet_synced_asset
            WHERE created_at > CONVERT_TZ('${date}', @@session.time_zone, '+00:00')
              AND (ual, created_at) IN
                  (SELECT ual, MAX(created_at)
                   FROM paranet_synced_asset
                   WHERE created_at > CONVERT_TZ('${date}', @@session.time_zone, '+00:00')
                   GROUP BY ual)
              AND paranet_ual = '${paranetUAL}'
            GROUP BY ual
        ) latest ON sa.id = latest.max_id;
    `;
};

function getCurrentTimeProperFormat() {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function getFormattedDate(inputDate) {
    // Convert to a string and remove the 'Z' to avoid UTC conversion
    const dateString = inputDate.toISOString().replace('Z', '');

    // Manually extract year, month, day, hour, minute, second from the ISO string
    const [datePart, timePart] = dateString.split('T');
    const [year, month, day] = datePart.split('-');
    const [hour, minute, second] = timePart.split(':');

    // Combine them into the desired format
    return `${year}-${month}-${day} ${hour}:${minute}:${second.split('.')[0]}`;
}

function getFormattedDate2(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-based
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

async function storeNotification(assets) {
    let notification = await Notification.create({
        title: 'New Knowledge assets are created!'
    });
    notification.message = `Your node has ingested ${assets.length} new knowledge assets since your last login.`;
    await notification.save();
    return notification;
}

async function storeSyncedAssets(assets, notification) {
    for (let x = 0; x < assets.length; x++) {
        let syncedData = assets[x];
        syncedData.backend_synced_at = getCurrentTimeProperFormat();
        syncedData.runtime_node_synced_at = getFormattedDate(
            syncedData.created_at
        );
        syncedData.notification_id = notification.id;
        delete syncedData.id;
        delete syncedData.created_at;
        delete syncedData.updated_at;
        let createdSyncedAsset = await SyncedAsset.create(syncedData);
    }
    return true;
}

// Add Jobs Every 10 Seconds
setInterval(async () => {
    console.log('Queueing sync job...');
    await syncQueue.add('syncJob', { timestamp: Date.now() });
}, 10000);
