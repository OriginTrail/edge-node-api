const path = require('path');
const FormData = require('form-data');
const fs = require('fs');
const https = require('https');
const axios = require('axios');
const DKG = require('dkg.js');
const { OPERATION_STATUSES } = require('../helpers/utils');

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
            case 'public':
                return await this.dkgClient.asset.create(asset, {
                    epochsNum: 2,
                    tokenAmount: '100000000000'
                });
            case 'paranet':
                return await this.dkgClient.asset.create(asset, {
                    epochsNum: 2,
                    paranetUAL: paranetUAL
                });
            case 'curated_paranet':
                return await this.dkgClient.asset.localStore(asset, {
                    epochsNum: 2,
                    paranetUAL: paranetUAL
                });
            default:
                return await this.dkgClient.asset.create(asset, {
                    epochsNum: 2
                });
        }
    }

    definePublishType(endpoint) {
        const domainRegex = /^(?!:\/\/)([a-zA-Z0-9-_]{1,63}\.)+[a-zA-Z]{2,}$/;
        if (domainRegex.test(endpoint)) {
            return 'external';
        }
        return 'internal';
    }

    async getWallets(sessionCookie) {
        const wallets = await axios.get(
            `${process.env.AUTH_SERVICE_ENDPOINT}/auth/wallets`,
            {
                headers: {
                    Cookie: sessionCookie
                },
                withCredentials: true
            }
        );
        return wallets.data.wallets;
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
        asset.operation_id = result?.operation?.localStore?.operationId
            ? result.operation.localStore.operationId
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
        if (result?.operation?.localStore?.errorType) {
            return result?.operation?.localStore?.errorMessage;
        }
        return null;
    }

    defineStatus(localStoreStatus, submitToParanetStatus) {
        if (
            localStoreStatus === OPERATION_STATUSES.COMPLETED &&
            submitToParanetStatus
        ) {
            return OPERATION_STATUSES.COMPLETED;
        } else {
            return OPERATION_STATUSES.FAILED;
        }
    }
}

module.exports = new PublishService();
