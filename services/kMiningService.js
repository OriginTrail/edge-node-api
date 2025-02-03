const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const datasetService = require('./datasetService.js');

exports.defineProcessingPipelineId = async req => {
    const kmining_json_pipeline_id = req.user.config.find(
        item => item.option === 'kmining_json_pipeline_id'
    )?.value;
    const kmining_pdf_pipeline_id = req.user.config.find(
        item => item.option === 'kmining_pdf_pipeline_id'
    )?.value;
    const kmining_csv_pipeline_id = req.user.config.find(
        item => item.option === 'kmining_csv_pipeline_id'
    )?.value;

    if (req.file.mimetype === 'application/ld+json') {
        return 'simple_json_to_jsonld';
    }
    if (req.file.mimetype === 'application/json') {
        return kmining_json_pipeline_id;
    }
    if (req.file.mimetype === 'application/pdf') {
        return kmining_pdf_pipeline_id;
    }
    if (req.file.mimetype === 'text/csv') {
        return kmining_csv_pipeline_id;
    }
};

exports.triggerPipeline = async (
    file,
    sessionCookie,
    kMiningEndpoint,
    kMiningPipelineId,
    inputDatasetDBRecord
) => {
    try {
        // Create form data
        const formData = new FormData();
        const filePath = file.path;
        formData.append('file', fs.createReadStream(filePath));
        formData.append('pipelineId', kMiningPipelineId);
        formData.append(
            'fileFormat',
            file.mimetype === 'application/json' ||
                file.mimetype === 'application/ld+json'
                ? 'json'
                : file.mimetype === 'application/pdf'
                ? 'pdf'
                : 'csv'
        );

        let result = await axios.post(
            `${kMiningEndpoint}/trigger-pipeline`,
            formData,
            {
                withCredentials: true,
                headers: {
                    Cookie: sessionCookie,
                    ...formData.getHeaders() // Include multipart/form-data headers
                }
            }
        );

        console.log('Trigger pipeline result', result.status, result.data);
        if (result.data.success) {
            const pipelineId = result.data.pipelineId;
            const runId = result.data.runId;
            if (inputDatasetDBRecord) {
                await datasetService.storePipelineInfo(
                    inputDatasetDBRecord,
                    pipelineId,
                    runId
                );
            }
            while (true) {
                await wait(1000);

                console.log('Checking pipeline status...');
                let pipelineResp = await axios.get(
                    `${kMiningEndpoint}/check-pipeline-status`,
                    {
                        params: {
                            pipelineId,
                            runId
                        },
                        withCredentials: true,
                        headers: {
                            Cookie: sessionCookie,
                            ...formData.getHeaders() // Include multipart/form-data headers
                        }
                    }
                );

                console.log('Pipeline status result', pipelineResp.data);

                if (pipelineResp.data.status === 'success') {
                    return pipelineResp.data.result;
                } else if (
                    pipelineResp.data.status === 'failed' ||
                    pipelineResp.data.status === 'not_found'
                ) {
                    return false;
                }
            }
        } else {
            return false;
        }
    } catch (error) {
        console.error('Error triggering pipeline:', error);
        throw error;
    }
};
function wait(ms) {
    console.log(`Waiting for ${ms}ms`);
    return new Promise(resolve => {
        setTimeout(() => {
            console.log('Finished waiting');
            resolve();
        }, 1000);
    });
}
