const path = require('path');
const fs = require('fs');
const kMiningService = require('./kMiningService');
const milvusService = require('./milvusService');

class VectorService {
    constructor() {
        this.userConfig = null;
    }

    async vectorizeKnowledgeAsset(result, content, req, sessionCookie) {
        const UAL = result.UAL;
        const parsedContent = JSON.parse(content);

        const contentForVectorize = [];

        if (parsedContent.private) {
            contentForVectorize.push({
                ...parsedContent.private,
                ual: UAL
            });
        }

        if (parsedContent.public) {
            contentForVectorize.push({
                ...parsedContent.public,
                ual: UAL
            });
        }

        if (!parsedContent.private && !parsedContent.public) {
            contentForVectorize.push({
                ...parsedContent,
                ual: UAL
            });
        }

        const storageDir = path.join(__dirname, '../storage/vector_assets');
        const sanitizedUAL = UAL.replace(/[:/\\?%*|"<>]/g, '_');
        const filePath = path.join(storageDir, `${sanitizedUAL}.json`);
        if (!fs.existsSync(storageDir)) {
            fs.mkdirSync(storageDir);
        }
        fs.writeFileSync(
            filePath,
            JSON.stringify(contentForVectorize, null, 2)
        );
        try {
            const kMiningEndpoint = req.user.config.find(
                item => item.option === 'kmining_endpoint'
            ).value;
            const vectorizePipeline = req.user.config.find(
                item => item.option === 'vectorize_pipeline'
            ).value;

            const embeddingsAndMetadata = await kMiningService.triggerPipeline(
                req,
                { path: filePath },
                sessionCookie,
                kMiningEndpoint,
                vectorizePipeline,
                null
            );
            if (
                !embeddingsAndMetadata.embeddings ||
                !embeddingsAndMetadata.texts ||
                !embeddingsAndMetadata.metadatas
            ) {
                throw Error(
                    'KA Mining did not return a valid vector DB entry object.'
                );
            }
            milvusService.setUserConfig(req.user.config);
            milvusService.initMilvusClient();
            const milvusResult = await milvusService.insert(
                embeddingsAndMetadata
            );
            console.log(
                `Performed Milvus insert with status ${JSON.stringify(
                    milvusResult.status
                )}`
            );
        } catch (error) {
            console.error('Error during vectorization pipeline:', error);
        }
    }
}
module.exports = new VectorService();
