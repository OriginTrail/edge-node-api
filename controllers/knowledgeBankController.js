const datasetService = require('../services/datasetService');
const kMiningService = require('../services/kMiningService');
const path = require('path');
const fs = require('fs');
const { Asset, SyncedAsset } = require('../models');
const {
    OPERATION_STATUSES,
    DKG_CONSTS,
    getBlockchainFromUAL
} = require('../helpers/utils');
const publishService = require('../services/publishService.js');
const vectorService = require('../services/vectorService.js');
const { Op, Sequelize } = require('sequelize');
const internalSequelize = require('../models/index');
const milvusService = require('../services/milvusService.js');
const redis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const { BullMQOtel } = require('bullmq-otel');

exports.getDatasets = async (req, res) => {
    try {
        const datasets = [];
        res.status(200).json(datasets);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

exports.getAssets = async (req, res) => {
    try {
        const edgeNodePublishMode =
            req.user.config.find(
                item => item.option === 'edge_node_publish_mode'
            ).value || null;
        const paranetUAL =
            req.user.config.find(
                item => item.option === 'edge_node_paranet_ual'
            ).value || null;

        const offset = parseInt(req.query.offset) || 0;
        const limit = parseInt(req.query.limit) || 10;

        if (edgeNodePublishMode === 'public') {
            const { count, rows: assets } = await Asset.findAndCountAll({
                attributes: [
                    'id',
                    'ual',
                    [Sequelize.col('assertion_id'), 'public_assertion_id'],
                    [Sequelize.col('created_at'), 'backend_synced_at']
                ],
                where: {
                    publishing_status: 'COMPLETED'
                },
                limit: limit,
                offset: offset
            });

            res.json({
                totalItems: count,
                offset: offset,
                limit: limit,
                data: assets
            });
        } else {
            const assets = await internalSequelize.sequelize.query(
                `WITH RankedAssets AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY ual ORDER BY created_at DESC, id DESC) AS row_num
                FROM synced_assets
                WHERE paranet_ual = :paranetUAL -- Add the filter for paranet_ual
            )
             SELECT *
             FROM RankedAssets
             WHERE row_num = 1
             ORDER BY created_at DESC, id DESC
                 LIMIT ${limit}
             OFFSET ${offset}`,
                {
                    type: internalSequelize.Sequelize.QueryTypes.SELECT,
                    replacements: { paranetUAL }
                }
            );

            const total = await internalSequelize.sequelize.query(
                `SELECT ual, COUNT(*)
             FROM synced_assets
             WHERE paranet_ual = :paranetUAL -- Add the filter for paranet_ual
             GROUP BY ual`,
                {
                    type: internalSequelize.Sequelize.QueryTypes.SELECT,
                    replacements: { paranetUAL }
                }
            );

            res.json({
                totalItems: total.length,
                offset: offset,
                limit: limit,
                data: assets
            });
        }
    } catch (error) {
        console.error('Error fetching paginated assets:', error);
        res.status(500).json({ error: 'Failed to fetch assets' });
    }
};

exports.previewAssetExternal = async (req, res) => {
    const { assetUAL } = req.query;

    try {
        // const sessionCookie = req.headers.cookie;

        const userConfig = req.user.config;
        const formattedUserConfig = publishService.setUserConfig(userConfig);

        const DkgClient = publishService.initDkgClient({
            name: getBlockchainFromUAL(assetUAL)
        });
        let result;
        if (formattedUserConfig.edge_node_publish_mode === 'public') {
            result = await DkgClient.asset.get(assetUAL);
        } else {
            result = await DkgClient.asset.get(assetUAL, {
                paranetUAL: formattedUserConfig.edge_node_paranet_ual
            });
        }

        let formattedKnowledgeAsset = {};
        formattedKnowledgeAsset.assertion = result.assertion;
        res.json(formattedKnowledgeAsset);
    } catch (error) {
        console.error('Error fetching asset:', error);
        res.status(500).json({ error: 'Server error' });
    }
};

exports.previewAsset = async (req, res) => {
    const { assetId } = req.params;

    try {
        const asset = await Asset.findOne({
            where: { id: assetId }
        });

        if (!asset) {
            return res.status(404).json({ error: 'Asset not found' });
        }

        const filePath = path.join(__dirname, `../${asset.url}`);

        if (!fs.existsSync(filePath)) {
            return res
                .status(404)
                .json({ error: 'File not found at the provided URL' });
        }

        const stream = fs.createReadStream(filePath);
        stream.on('error', error => {
            console.error('Error reading file:', error);
            return res.status(500).json({ error: 'Error reading file' });
        });

        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader(
            'Content-Disposition',
            `attachment; filename="${path.basename(filePath)}"`
        );

        stream.pipe(res);
    } catch (error) {
        console.error('Error fetching asset or file:', error);
        res.status(500).json({ error: 'Server error' });
    }
};

exports.importDataset = async (req, res) => {
    let inputDatasetDBRecord;
    try {
        console.time('Store input dataset and prepare KMining request');
        if (!req.file) {
            if (req.datasetIdCb) req.datasetIdCb(null);
            return res.status(400).send('No file uploaded.');
        }
        const sessionCookie = req.headers.cookie;

        let storagePath = '/storage/datasets';
        const relativePath = `${storagePath}/${req.file.filename}`;
        inputDatasetDBRecord = await datasetService.storeInputDataset(
            relativePath,
            req.file.filename
        );
        if (req.datasetIdCb) req.datasetIdCb(inputDatasetDBRecord.id);

        await datasetService.updateDatasetProcessingStatus(
            inputDatasetDBRecord.id,
            OPERATION_STATUSES['IN-PROGRESS']
        );

        const kMiningEndpoint = req.user.config.find(
            item => item.option === 'kmining_endpoint'
        ).value;
        const publishMode = req.user.config.find(
            item => item.option === 'edge_node_publish_mode'
        ).value;

        const kMiningPipelineId =
            await kMiningService.defineProcessingPipelineId(req);
        console.timeEnd('Store input dataset and prepare KMining request');

        console.time('K Mining pipeline');
        const stagedKnowledgeAssets = await kMiningService.triggerPipeline(
            req,
            req.file,
            sessionCookie,
            kMiningEndpoint,
            kMiningPipelineId,
            inputDatasetDBRecord
        );
        console.timeEnd('K Mining pipeline');

        console.time('Other');
        let { filenames, assetsData } =
            await datasetService.storeStagedAssetsToStorage(
                stagedKnowledgeAssets,
                inputDatasetDBRecord,
                publishMode
            );
        let { errors, finalAssets } =
            await datasetService.storeStagedAssetsToDB(
                filenames,
                inputDatasetDBRecord,
                assetsData
            );

        await datasetService.updateDatasetProcessingStatus(
            inputDatasetDBRecord.id,
            OPERATION_STATUSES.COMPLETED
        );
        console.timeEnd('Other');
        res.status(200).json({
            message: 'Dataset successfully processed and stored.',
            datasetId: inputDatasetDBRecord.id,
            errors: errors,
            assets: finalAssets
        });
    } catch (e) {
        console.error(e);
        if (inputDatasetDBRecord) {
            if (req.datasetIdCb) req.datasetIdCb(inputDatasetDBRecord.id);
            await datasetService.updateDatasetProcessingStatus(
                inputDatasetDBRecord.id,
                OPERATION_STATUSES.FAILED,
                e.message
            );
        }
        res.status(400).json({
            message: e.message
        });
    }
};

exports.confirmAndCreateAssets = async (req, res) => {
    try {
        const { knowledgeAssets } = req.body;
        let wallets = await publishService.getWallets(req);
        const userConfig = req.user.config;
        const formattedUserConfig = publishService.setUserConfig(userConfig);
        const edgeNodePublishMode = formattedUserConfig.edge_node_publish_mode;
        const paranetUAL = formattedUserConfig.edge_node_paranet_ual;

        if (knowledgeAssets.length === 0)
            throw new Error('You must provide at least one knowledge asset.');

        const promises = knowledgeAssets.map((ka, i) => async () => {
            let asset = null;
            let wallet = null;
            let result = null;

            try {
                asset = await Asset.findByPk(ka.assetId);
                const assetContent = JSON.parse(ka.content);
                await datasetService.storeUpdatedKAContent(asset, assetContent);
                const publishServiceEndpoint = req.user.config.find(
                    item => item.option === 'publish_service_endpoint'
                ).value;

                while (
                    asset.publishing_status !== OPERATION_STATUSES.COMPLETED
                ) {
                    if (
                        Date.now() - asset.updated_at.getTime() >=
                        10 * 60 * 1000
                    ) {
                        // 10 minutes
                        throw new Error(
                            `Operation timeout. Asset internal id ${asset.id}`
                        );
                    }

                    console.time(
                        `[${asset.dataset_id}_${i + 1}] Before create`
                    );
                    wallet = await publishService.defineNextWallet(wallets);
                    await publishService.updatePublishingStatus(
                        asset,
                        OPERATION_STATUSES['IN-PROGRESS'],
                        null,
                        null,
                        wallet
                    );
                    console.timeEnd(
                        `[${asset.dataset_id}_${i + 1}] Before create`
                    );

                    console.time(`[${asset.dataset_id}_${i + 1}] Asset create`);
                    result = await publishService.createAsset(
                        publishServiceEndpoint,
                        assetContent,
                        wallet
                    );
                    console.timeEnd(
                        `[${asset.dataset_id}_${i + 1}] Asset create`
                    );

                    const publishStatus = result?.UAL
                        ? OPERATION_STATUSES.COMPLETED
                        : OPERATION_STATUSES.FAILED;

                    await publishService.updatePublishingStatus(
                        asset,
                        publishStatus,
                        result,
                        publishStatus === OPERATION_STATUSES.COMPLETED
                            ? null
                            : 'Something went wrong! Publish data is missing.',
                        wallet
                    );

                    if (publishStatus === OPERATION_STATUSES.COMPLETED) {
                        break;
                    }
                }

                return asset;
            } catch (e) {
                console.error(e);
                await publishService.updatePublishingStatus(
                    asset,
                    OPERATION_STATUSES.FAILED,
                    result,
                    e.message,
                    wallet
                );
                throw e;
            }
        });

        const results = await Promise.allSettled(promises.map(f => f()));

        if (results.length === 1) {
            const asset = results[0].value;
            if (!asset?.ual) {
                return res.status(500).json({
                    error: 'An error occurred while creating the knowledge asset',
                    details: `${results[0].reason}`
                });
            }

            // Add vectorization if enabled
            const vectorizationEnabled =
                req.user.config.find(
                    item => item.option === 'vectorization_enabled'
                )?.value || null;
            if (vectorizationEnabled === 'true') {
                const sessionCookie = req.headers.cookie;
                const content = JSON.parse(knowledgeAssets[0].content);
                try {
                    await vectorService.vectorizeKnowledgeAsset(
                        asset,
                        content,
                        req,
                        sessionCookie
                    );
                } catch (error) {
                    console.error('Vectorization error:', error);
                    // Continue even if vectorization fails
                }
            } else {
                console.log('Skipping vectorization');
            }

            return res.status(200).json({
                message: 'Knowledge asset created.',
                data: {
                    UAL: asset.ual,
                    assertionId: asset.assertion_id,
                    transactionHash: asset.transaction_hash,
                    status: asset.publishing_status
                }
            });
        }

        return res.status(200).json({
            message: 'Creation of knowledge assets is finished.',
            results
        });
    } catch (error) {
        console.error(error);
        return res.status(500).json({
            error: 'An error occurred while processing the knowledge asset',
            details: error.message
        });
    }
};

const redisConnection = new redis({
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
    db: process.env.REDIS_DB,
    maxRetriesPerRequest: null
});
const importCreateAsyncQueue = {
    _q: new Queue('import-create', {
        connection: redisConnection,
        telemetry: new BullMQOtel('import-create', '0.0.1')
    }),
    _w: new Worker(
        'import-create',
        async ({ data }) => {
            console.time('Action ' + data.actionId);
            let req;
            let error;
            try {
                req = importCreateAsyncQueue._reqMap[data.actionId];
                if (!req) throw new Error('Request data is missing.');
                const res = {
                    statusCode: 0,
                    status(s) {
                        this.statusCode = s;
                        return this;
                    },
                    data: {},
                    json(d) {
                        this.data = d;
                    }
                };

                if (!data.datasetId) await exports.importDataset(req, res);
                else {
                    const finalAssets =
                        await datasetService.getStagedAssetsFromDB(
                            data.datasetId
                        );
                    res.status(200).json({ assets: finalAssets });
                }

                if (res.statusCode < 400) {
                    req.body = {
                        knowledgeAssets: res.data.assets.map(a => ({
                            ...a,
                            content: JSON.stringify(a.content)
                        }))
                    };
                    await exports.confirmAndCreateAssets(req, res);
                }
            } catch (err) {
                console.error('Action Error ' + data.actionId, err);
                error = err;
            } finally {
                console.timeEnd('Action ' + data.actionId);
                delete importCreateAsyncQueue._reqMap[data.actionId];
                if (error) throw error;
            }
        },
        {
            connection: redisConnection,
            concurrency: 10,
            telemetry: new BullMQOtel('import-create', '0.0.1')
        }
    ),
    _reqMap: {},
    queueJob(req) {
        const actionId = `import-create-${Math.random()}`;
        importCreateAsyncQueue._reqMap[actionId] = req;
        return this._q.add(
            actionId,
            { actionId },
            {
                removeOnComplete: true,
                removeOnFail: true
            }
        );
    }
};
require('../bull-board.js').addQueue(importCreateAsyncQueue._q);
exports.importCreateAsync = async (req, res) => {
    let didReturn = false;
    req.datasetIdCb = datasetId => {
        if (didReturn) return;
        didReturn = true;
        console.log(`[${datasetId}] Started processing dataset`);
        if (!datasetId)
            res.status(400).json({
                datasetId,
                error: 'Unknown error occurred.'
            });
        else {
            job.updateData({ ...job.data, datasetId });
            res.status(202).json({ datasetId });
        }
    };
    const job = await importCreateAsyncQueue.queueJob(req);
};

exports.getDatasetStatus = async (req, res) => {
    try {
        const { datasetId } = req.params;
        const status = await datasetService.getStatus(datasetId);
        res.status(202).json(status);
    } catch (error) {
        console.error(error);
        return res.status(500).json({ error: error.message });
    }
};

exports.getAssetsMetadata = async (req, res) => {
    try {
        const { uals } = req.body;
        let finalData = {};
        if (uals.length > 0) {
            for (let x = 0; x < uals.length; x++) {
                let asset = await SyncedAsset.findOne({
                    where: {
                        ual: uals[x]
                    }
                });
                if (asset) {
                    finalData[uals[x]] = {
                        transaction_hash: asset.transaction_hash,
                        public_assertion_id: asset.public_assertion_id
                    };
                } else {
                    finalData[uals[x]] = 'No data found';
                }
            }
        }
        res.json({
            data: finalData
        });
    } catch (e) {
        console.error('Error getting assets metadata:', e);
        res.status(500).json({ error: 'Failed to get assets metadata' });
    }
};

exports.query = async (req, res) => {
    const {
        query,
        queryConfig: requestQueryConfig,
        queryType = 'SELECT'
    } = req.body;
    const offset = parseInt(req.body.offset) || 0;
    const limit = parseInt(req.body.limit) || 10;

    try {
        const userConfig = req.user.config;

        const formattedUserConfig = publishService.setUserConfig(userConfig);

        const queryConfig = {
            // Uncomment if you want to query whole paranet
            //   paranetUAL: formattedUserConfig.edge_node_paranet_ual,
            graphLocation: 'LOCAL_KG',
            graphState: 'CURRENT',
            ...requestQueryConfig
        };

        const DkgClient = publishService.initDkgClient(
            formattedUserConfig.blockchain
        );

        let result = await DkgClient.graph.query(query, queryType, queryConfig);

        if (!Array.isArray(result?.data)) {
            throw Error(`Error querying the network using query ${query}`);
        }

        const paginatedData = result.data.slice(offset, offset + limit);

        res.json({
            totalItems: result.data.length,
            offset: offset,
            limit: limit,
            data: paginatedData
        });
    } catch (error) {
        console.error(error.message);
        res.status(500).json({ error: 'Failed to query network' });
    }
};
