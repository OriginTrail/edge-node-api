const { Dataset, Asset } = require('../models');
const fs = require('fs');
const path = require('path');
const { OPERATION_STATUSES } = require('../helpers/utils');

exports.processUpload = async file => {
    // Logic for processing the uploaded file and saving the dataset
};

exports.getAll = async () => {
    return Dataset.findAll();
};

exports.storeInputDataset = async (relativePath, filename) => {
    return await Dataset.create({
        filename: filename,
        url: relativePath,
        processing_status: 'NOT-STARTED'
    });
};

exports.storeStagedAssetsToStorage = async (
    stagedKnowledgeAssets,
    inputDatasetDBRecord,
    publishMode = 'public'
) => {
    let filenames = [];
    let inputStagedAssets = [];
    let assetsData = {};
    if (
        typeof stagedKnowledgeAssets === 'object' &&
        stagedKnowledgeAssets !== null
    ) {
        inputStagedAssets.push(stagedKnowledgeAssets);
    } else {
        inputStagedAssets = stagedKnowledgeAssets;
    }

    for (let index = 0; index < inputStagedAssets.length; index++) {
        const row = inputStagedAssets[index];
        let transformedToPrivateAsset = {};
        if (!('@context' in row) && ('private' in row || 'public' in row)) {
            transformedToPrivateAsset = row;
        } else if (publishMode !== 'public') {
            transformedToPrivateAsset.private = row;
        } else {
            transformedToPrivateAsset.public = row;
        }

        const filename = `KA-OUTPUT-${
            index + 1
        }-FROM-FILE-${inputDatasetDBRecord.filename
            .replace('.json', '')
            .replace('.pdf', '')}.json`;
        const filepath = path.join(__dirname, '../storage/assets', filename);
        const relativeFilePath = `/storage/assets/${filename}`;
        const jsonData = JSON.stringify(transformedToPrivateAsset, null, 2); // Pretty print with 2-space indentation

        try {
            await fs.promises.writeFile(filepath, jsonData, 'utf8');
            assetsData[filename] = transformedToPrivateAsset;
            filenames.push({ filename, relativeFilePath });
        } catch (err) {
            console.error(`Error writing file ${filename}:`, err);
            throw err; // Throw error to stop further execution if needed
        }
    }

    return { filenames, assetsData };
};

exports.storeStagedAssetsToDB = async (
    filenames,
    inputDatasetDBRecord,
    assetsData
) => {
    let errors = [];
    let finalAssets = [];

    for (const [filename, assetContent] of Object.entries(assetsData)) {
        try {
            let asset = await Asset.create({
                dataset_id: inputDatasetDBRecord.id,
                filename: filename,
                url: `/storage/assets/${filename}`,
                publishing_status: 'NOT-STARTED'
            });
            finalAssets.push({
                assetId: asset.id,
                content: assetContent
            });
        } catch (error) {
            errors.push(error);
        }
    }
    return { errors, finalAssets };
};

exports.storeUpdatedKAContent = async (asset, knowledgeAssetContent) => {
    try {
        const filepath = path.join(__dirname, `../${asset.url}`);
        let jsonData = JSON.stringify(knowledgeAssetContent, null, 2);
        await fs.promises.writeFile(filepath, jsonData, 'utf8');
        return asset;
    } catch (err) {
        console.error('Error updating JSON file:', err);
        return false;
    }
};

exports.markDatasetAsFailed = async (inputDatasetDBRecordId, error_message) => {
    let dataset = await Dataset.findByPk(inputDatasetDBRecordId);
    if (dataset) {
        dataset.processing_status = OPERATION_STATUSES.FAILED;
        dataset.error_message = error_message;
        return await dataset.save();
    }
};

exports.markDatasetAsInProgress = async inputDatasetDBRecordId => {
    let dataset = await Dataset.findByPk(inputDatasetDBRecordId);
    if (dataset) {
        dataset.processing_status = OPERATION_STATUSES['IN-PROGRESS'];
        return await dataset.save();
    }
};

exports.updateDatasetProcessingStatus = async (
    inputDatasetDBRecordId,
    status,
    error_message = null
) => {
    let dataset = await Dataset.findByPk(inputDatasetDBRecordId);
    if (dataset) {
        dataset.processing_status = status;
        if (error_message !== null) {
            dataset.error_message = error_message;
        }
        return await dataset.save();
    }
};

exports.storePipelineInfo = async (inputDatasetDBRecord, pipelineId, runId) => {
    inputDatasetDBRecord.pipeline_id = pipelineId;
    inputDatasetDBRecord.run_id = runId;
    return await inputDatasetDBRecord.save();
};

exports.getStatus = async inputDatasetDBRecordId => {
    const dataset = await Dataset.findByPk(inputDatasetDBRecordId);
    const assets = await Asset.findAll({
        where: { dataset_id: inputDatasetDBRecordId }
    });
    return {
        ...dataset.dataValues,
        assets
    };
};
