const axios = require('axios');

exports.authMiddleware = async (req, res, next) => {
    try {
        const authHeader = req.headers['authorization'];

        if (authHeader && authHeader.startsWith('Bearer ')) {
            // Bearer token is present
            const token = authHeader.split(' ')[1];

            if (!token) {
                return res.status(401).json({
                    authenticated: false,
                    message: 'Invalid Bearer token format'
                });
            }

            const authResponse = await axios.get(
                `${process.env.AUTH_SERVICE_ENDPOINT}/check`,
                {
                    headers: {
                        Authorization: `Bearer ${token}`
                    },
                    withCredentials: true
                }
            );

            if (authResponse.data.authenticated) {
                req.user = authResponse.data.user;
                return next();
            } else {
                return res.status(401).json({
                    authenticated: false,
                    message: 'User not authenticated'
                });
            }
        } else {
            // Bearer token not present, check for session cookie
            const sessionCookie = req.headers.cookie;

            if (!sessionCookie) {
                return res.status(401).json({
                    authenticated: false,
                    message: 'No session cookie found'
                });
            }

            const authResponse = await axios.get(
                `${process.env.AUTH_SERVICE_ENDPOINT}/check`,
                {
                    headers: {
                        Cookie: sessionCookie
                    },
                    withCredentials: true
                }
            );

            if (authResponse.data.authenticated) {
                req.user = authResponse.data.user;
                next();
            } else {
                return res.status(401).json({
                    authenticated: false,
                    message: 'User not authenticated'
                });
            }
        }
    } catch (error) {
        console.error('Error during authentication:', error);
        return res
            .status(500)
            .json({ authenticated: false, message: 'Internal server error' });
    }
};
