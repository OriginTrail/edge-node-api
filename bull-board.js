const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');

const basePath = (process.env.ROUTES_PREFIX || '/') + 'bull-board';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath(basePath);

const board = createBullBoard({
    queues: [],
    serverAdapter
});

exports.addQueue = q =>
    board.addQueue(new BullMQAdapter(q, { readOnlyMode: true }));
exports.basePath = basePath;
exports.router = serverAdapter.getRouter();
