const OPERATION_STATUSES = {
    FAILED: 'FAILED',
    'NOT-STARTED': 'NOT-STARTED',
    'IN-PROGRESS': 'IN-PROGRESS',
    COMPLETED: 'COMPLETED',
    'NOT-READY': 'NOT-READY',
    REPLICATE_END: 'PUBLISH_REPLICATE_END'
};

const DKG_CONSTS = {
    OPERATION_STATUSES: {
        COMPLETED: 'COMPLETED',
        FAILED: 'FAILED'
    },
    OPERATIONS: {
        LOCAL_STORE: 'LOCAL_STORE'
    }
};

function getBlockchainFromUAL(ual) {
    if (!ual || typeof ual !== 'string') {
        return null;
    }

    const match = ual.match(/^did:dkg:([^:\/]+:[^\/]+)/);
    return match ? match[1] : null;
}

module.exports = { OPERATION_STATUSES, DKG_CONSTS, getBlockchainFromUAL };
