const path = require('path');
const FormData = require('form-data');
const fs = require('fs');
const https = require('https');
const axios = require('axios');
const DKG = require('dkg.js');
const { OPERATION_STATUSES } = require('../helpers/utils');
const {
    Queue,
    QueueEvents,
    Worker,
    Job,
    UnrecoverableError
} = require('bullmq');
const redis = require('ioredis');
const bullBoard = require('../bull-board');
const { BullMQOtel } = require('bullmq-otel');

const publishQueueConnection = new redis({
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
    db: process.env.REDIS_DB,
    maxRetriesPerRequest: null
});

class PublishService {
    constructor() {
        this.userConfig = null;
    }

    setUserConfig(config) {
        return (this.userConfig = config.reduce((acc, obj) => {
            acc[obj.option] = obj.value;
            return acc;
        }, {}));
    }

    initDkgClient(blockchain) {
        this.dkgClient = new DKG({
            environment: this.userConfig.edge_node_environment,
            endpoint: this.userConfig.run_time_node_endpoint,
            port: this.userConfig.run_time_node_port,
            blockchain: blockchain || this.userConfig.blockchain,
            maxNumberOfRetries: 30,
            frequency: 2,
            contentType: 'all',
            nodeApiVersion: '/v1'
        });
        return this.dkgClient;
    }

    publishQueue = {
        _queues: {},
        _queueEvents: {},
        _actions: {},
        createQueryKey(wallet) {
            return `wallet-jobs-${wallet.wallet}-${wallet.blockchain?.replace(
                ':',
                ''
            )}`;
        },
        async addJobForWallet(wallet, action) {
            const qKey = this.createQueryKey(wallet);
            const actionId = `${qKey}__${Math.random()}`;
            console.log(`Creating job with ID: ${actionId}`);

            if (!this._queues[qKey]) {
                // Initialize queue for the wallet
                console.log(
                    `Initializing new queue for wallet: ${wallet.wallet}`
                );
                this._queues[qKey] = new Queue(qKey, {
                    connection: publishQueueConnection,
                    telemetry: new BullMQOtel('publish-service', '0.0.1')
                });
                this._queueEvents[qKey] = new QueueEvents(qKey, {
                    connection: publishQueueConnection
                });

                // Create a worker for this wallet's queue
                new Worker(
                    qKey,
                    async ({ data }) => {
                        console.log('Running action:', data.actionId);
                        try {
                            if (this._actions[data.actionId])
                                return await this._actions[data.actionId]();
                            else
                                throw new UnrecoverableError(
                                    'Unexpected error - no job action.'
                                );
                        } catch (error) {
                            console.log(
                                `Job execution error for ${data.actionId}:`,
                                error.message
                            );
                            throw error; // Re-throw to trigger job failure handling
                        }
                    },
                    {
                        connection: publishQueueConnection,
                        concurrency: 1, // important!
                        telemetry: new BullMQOtel('publish-service', '0.0.1')
                    }
                );

                // Add to bull-board UI
                bullBoard.addQueue(this._queues[qKey]);
            }

            //add job to the queue
            this._actions[actionId] = action;
            console.log(
                `Adding job ${actionId} to queue with 10 retry attempts`
            );
            return await this._queues[qKey].add(
                actionId,
                { actionId },
                {
                    removeOnComplete: true,
                    removeOnFail: true,
                    attempts: 10,
                    backoff: 1000
                }
            );
        },
        getQueueEvents(wallet) {
            const qKey = this.createQueryKey(wallet);
            return this._queueEvents[qKey];
        },
        _walletsUseCount: {},
        walletNextOptimal(availableWallets) {
            const optimal_qKey = Object.entries(this._walletsUseCount)
                .sort((a, b) => a[1] - b[1])
                .at(0)?.[0];

            let optimal_wallet = availableWallets[0];
            for (let i = 0; i < availableWallets.length; i++) {
                const qKey = this.createQueryKey(availableWallets[i]);
                if (!this._walletsUseCount[qKey]) return availableWallets[i];
                if (qKey === optimal_qKey) optimal_wallet = availableWallets[i];
            }
            return optimal_wallet;
        },
        walletMarkUsed(wallet) {
            const qKey = this.createQueryKey(wallet);
            if (qKey in this._walletsUseCount) this._walletsUseCount[qKey]++;
            else this._walletsUseCount[qKey] = 1;
        },
        jobCleanup(job) {
            const qKey = job.data.actionId.split('__')[0];
            if (qKey in this._walletsUseCount) this._walletsUseCount[qKey]--;
            delete this._actions[job.data.actionId];
        }
    };

    async createAsset(endpoint, asset, wallet = null) {
        let type = this.definePublishType(endpoint);
        let blockchain = this.defineBlockchainSettings(wallet);
        this.initDkgClient(blockchain);

        switch (type) {
            case 'internal':
                return this.internalPublishService(
                    asset,
                    this.userConfig.edge_node_publish_mode,
                    this.userConfig.edge_node_paranet_ual,
                    wallet
                );
            case 'external':
                return this.externalPublishService(endpoint, asset);
            default:
                return this.internalPublishService(
                    asset,
                    this.userConfig.edge_node_publish_mode,
                    this.userConfig.edge_node_paranet_ual,
                    wallet
                );
        }
    }

    async externalPublishService(endpoint, asset) {
        try {
            const filepath = path.join(__dirname, `../${asset.url}`);

            let data = new FormData();
            data.append('file', fs.createReadStream(filepath));
            data.append('useCaseUid', 'GENERAL');
            data.append('autoPublish', 'true');

            let config = {
                method: 'post',
                maxBodyLength: Infinity,
                url: `${endpoint}/api/data-assets-module/datasets`,
                httpsAgent: new https.Agent({
                    rejectUnauthorized: false // Ignore self-signed certificate verification
                }),
                headers: {
                    Authorization:
                        'Bearer vs6wvJfOA1JoDYoDscf6PjI6i1Zb0jjq5jX8uPZu',
                    'X-Requested-With': 'XMLHttpRequest',
                    ...data.getHeaders() // This will include the correct Content-Type header for the form-data
                },
                data: data
            };

            const response = await axios.request(config);
            return response.data;
        } catch (error) {
            console.log(error);
            return false;
        }
    }

    async internalPublishService(
        asset,
        edgeNodePublishMode,
        paranetUAL,
        wallet = null
    ) {
        switch (edgeNodePublishMode) {
            case 'paranet':
                const paranetJob = await this.publishQueue.addJobForWallet(
                    wallet,
                    () =>
                        this.initDkgClient(
                            this.defineBlockchainSettings(wallet)
                        ).asset.create(asset, {
                            epochsNum: 2,
                            paranetUAL,
                            minimumNumberOfFinalizationConfirmations: 1,
                            minimumNumberOfNodeReplications: 1
                        })
                );

                try {
                    console.time(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            paranetJob.id
                        }`
                    );
                    const paranetResult = await paranetJob.waitUntilFinished(
                        this.publishQueue.getQueueEvents(wallet)
                    );
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            paranetJob.id
                        }`
                    );
                    this.publishQueue.jobCleanup(paranetJob);
                    return paranetResult;
                } catch (error) {
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            paranetJob.id
                        }`
                    );
                    throw error;
                }
            case 'curated_paranet':
                const curatedJob = await this.publishQueue.addJobForWallet(
                    wallet,
                    () =>
                        this.initDkgClient(
                            this.defineBlockchainSettings(wallet)
                        ).asset.create(asset, {
                            epochsNum: 2,
                            minimumNumberOfFinalizationConfirmations: 1,
                            minimumNumberOfNodeReplications: 1
                        })
                );

                try {
                    console.time(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            curatedJob.id
                        }`
                    );
                    const curatedResult = await curatedJob.waitUntilFinished(
                        this.publishQueue.getQueueEvents(wallet)
                    );
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            curatedJob.id
                        }`
                    );
                    this.publishQueue.jobCleanup(curatedJob);

                    if (
                        curatedResult?.operation?.publish?.status ===
                        OPERATION_STATUSES.COMPLETED
                    ) {
                        console.time(
                            `[${asset.dataset_id}_${
                                i + 1
                            }] Asset submitToParanet`
                        );
                        const submitToParanetResult =
                            await this.submitToParanet(
                                curatedResult.UAL,
                                wallet
                            );
                        console.timeEnd(
                            `[${asset.dataset_id}_${
                                i + 1
                            }] Asset submitToParanet`
                        );

                        curatedResult.operation.submitToParanet = {
                            status: submitToParanetResult.UAL
                                ? OPERATION_STATUSES.COMPLETED
                                : OPERATION_STATUSES.FAILED
                        };
                    }

                    return curatedResult;
                } catch (error) {
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            curatedJob.id
                        }`
                    );
                    throw error;
                }
            default:
                const defaultJob = await this.publishQueue.addJobForWallet(
                    wallet,
                    () =>
                        this.initDkgClient(
                            this.defineBlockchainSettings(wallet)
                        ).asset.create(asset, {
                            epochsNum: 2,
                            minimumNumberOfFinalizationConfirmations: 1,
                            minimumNumberOfNodeReplications: 1
                        })
                );

                try {
                    console.time(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            defaultJob.id
                        }`
                    );
                    const defaultResult = await defaultJob.waitUntilFinished(
                        this.publishQueue.getQueueEvents(wallet)
                    );
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            defaultJob.id
                        }`
                    );
                    this.publishQueue.jobCleanup(defaultJob);
                    return defaultResult;
                } catch (error) {
                    console.timeEnd(
                        `[${this.publishQueue.createQueryKey(wallet)}] #${
                            defaultJob.id
                        }`
                    );
                    throw error;
                }
        }
    }

    async submitToParanet(UAL, wallet) {
        const job = await this.publishQueue.addJobForWallet(wallet, () =>
            this.initDkgClient(this.defineBlockchainSettings(wallet))
                .asset.submitToParanet(
                    UAL,
                    this.userConfig.edge_node_paranet_ual
                )
                .then(r => r.operation)
                .catch(() => undefined)
        );

        try {
            console.time(
                `[${this.publishQueue.createQueryKey(wallet)}] #${job.id}`
            );
            const receipt = await job.waitUntilFinished(
                this.publishQueue.getQueueEvents(wallet)
            );
            console.timeEnd(
                `[${this.publishQueue.createQueryKey(wallet)}] #${job.id}`
            );
            this.publishQueue.jobCleanup(job);
            return receipt;
        } catch (error) {
            console.timeEnd(
                `[${this.publishQueue.createQueryKey(wallet)}] #${job.id}`
            );
            throw error;
        }
    }

    definePublishType(endpoint) {
        const domainRegex = /^(?!:\/\/)([a-zA-Z0-9-_]{1,63}\.)+[a-zA-Z]{2,}$/;
        if (domainRegex.test(endpoint)) {
            return 'external';
        }
        return 'internal';
    }

    async getWallets(req) {
        try {
            const authHeader = req.headers['authorization'];

            if (authHeader && authHeader.startsWith('Bearer ')) {
                // Bearer token is present
                const token = authHeader.split(' ')[1];

                if (!token) {
                    throw Error('Invalid token format');
                }

                const wallets = await axios.get(
                    `${process.env.AUTH_SERVICE_ENDPOINT}/wallets`,
                    {
                        headers: {
                            Authorization: `Bearer ${token}`
                        },
                        withCredentials: true
                    }
                );
                return wallets.data.wallets;
            } else {
                const sessionCookie = req.headers.cookie;

                const wallets = await axios.get(
                    `${process.env.AUTH_SERVICE_ENDPOINT}/wallets`,
                    {
                        headers: {
                            Cookie: sessionCookie
                        },
                        withCredentials: true
                    }
                );
                return wallets.data.wallets;
            }
        } catch (e) {
            return null;
        }
    }

    async defineNextWallet(wallets) {
        const { sequelize } = require('../models');

        const result = await sequelize.query(
            `
                    SELECT w.wallet, COALESCE(COUNT(a.wallet), 0) AS total_used
                    FROM (
                             ${this.defineQueryBasedOnAvailableWallets(wallets)}
                             ) AS w
                             LEFT JOIN assets a ON w.wallet = a.wallet
                    GROUP BY w.wallet
                    ORDER BY total_used ASC;`,
            {
                type: sequelize.QueryTypes.SELECT
            }
        );

        return wallets.find(item => item.wallet === result[0].wallet);
    }

    defineQueryBasedOnAvailableWallets(wallets) {
        let query = '';
        for (let x = 0; x < wallets.length; x++) {
            if (x === wallets.length - 1) {
                query += `SELECT '${wallets[x].wallet}' AS wallet`;
            } else {
                query += `SELECT '${wallets[x].wallet}' AS wallet UNION ALL `;
            }
        }
        return query;
    }

    defineBlockchainSettings(wallet) {
        return {
            name: wallet.blockchain,
            publicKey: wallet.wallet,
            privateKey: wallet.private_key
        };
    }

    async updatePublishingStatus(
        asset,
        status,
        result = null,
        operation_message = null,
        wallet = null
    ) {
        asset.publishing_status = status;
        asset.operation_id = result?.operation?.publish?.operationId
            ? result.operation.publish.operationId
            : null;
        asset.operation_message =
            operation_message !== null
                ? operation_message
                : this.parseOperationMessage(result);
        asset.ual = result?.UAL ? result.UAL : null;
        asset.assertion_id = result?.publicAssertionId
            ? result.publicAssertionId
            : null;
        asset.blockchain = wallet?.blockchain ? wallet.blockchain : null;
        asset.wallet = wallet?.wallet ? wallet.wallet : null;
        asset.transaction_hash = result?.operation?.mintKnowledgeAsset
            ?.transactionHash
            ? result?.operation?.mintKnowledgeAsset.transactionHash
            : null;
        await asset.save();
    }

    parseOperationMessage(result) {
        if (result?.operation?.publish?.errorType) {
            return result?.operation?.publish?.errorMessage;
        }
        return null;
    }

    defineStatus(status, submitToParanetStatus) {
        console.log(
            `defineStatus called with status: "${status}", submitToParanetStatus: "${submitToParanetStatus}"`
        );

        if (status && status === 'FINALIZED') {
            console.log('Status is FINALIZED - returning COMPLETED');
            return OPERATION_STATUSES.COMPLETED;
        }
        if (
            (status === OPERATION_STATUSES.COMPLETED ||
                status === OPERATION_STATUSES.REPLICATE_END) &&
            submitToParanetStatus
        ) {
            console.log(
                'Status meets completion criteria - returning COMPLETED'
            );
            return OPERATION_STATUSES.COMPLETED;
        } else {
            console.log(
                `Status does not meet completion criteria - returning FAILED. Status: ${status}`
            );
            return OPERATION_STATUSES.FAILED;
        }
    }
}

module.exports = new PublishService();
