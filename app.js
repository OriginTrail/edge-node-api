require('dotenv').config();
if (process.env.OTEL_ENABLED?.toLowerCase() === 'true') {
    require('@opentelemetry/auto-instrumentations-node/register');
}

const express = require('express');
// const { createAssetJob } = require('./queue');
require('./sync-assets-queue');
require('./retry-publish-queue');
const cors = require('cors');
const cookieParser = require('cookie-parser');
const app = express();
const port = process.env.PORT || 3000;

app.use(cookieParser());

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// List of allowed origins
const allowedOrigins = [
    'http://localhost:8100',
    'http://localhost:5173',
    process.env.UI_ENDPOINT
];

// CORS configuration function
const corsOptions = {
    origin: (origin, callback) => {
        if (allowedOrigins.includes(origin) || !origin) {
            callback(null, true);
        } else {
            callback(new Error('Not allowed by CORS'));
        }
    },
    credentials: true // Allow credentials (cookies, authorization headers, etc.)
};

app.use(cors(corsOptions));
app.set('trust proxy', 1);

// Routes
const dashboardRoutes = require('./routes/dashboard');
const knowledgeBankRoutes = require('./routes/knowledgeBank');
const notificationRoutes = require('./routes/notifications');

const router = express.Router();
router.use('/dashboard', dashboardRoutes);
router.use('/knowledge-bank', knowledgeBankRoutes);
router.use('/notifications', notificationRoutes);

const bullBoard = require('./bull-board');
app.use(bullBoard.basePath, bullBoard.router);

app.use(process.env.ROUTES_PREFIX || '/', router);

const server = app.listen(port, () => {
    console.log(`Edge node backend running on port ${port}`);
});
server.setTimeout(120000);

module.exports = app;
