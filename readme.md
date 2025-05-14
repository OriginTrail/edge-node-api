# Installation

1. cp .env.example .env
2. create DB mentioned in .env
3. npm install
4. npx sequelize-cli db:migrate
5. setup Runtime node MYSQL operational db connection 
   1. ssh to your Runtime node server
   2. expose port 3306 on your server to your local IP address (firewall)
   3. enable MySQL remote connection by changing the bind-address from 127.0.0.0 to 0.0.0.0 in /etc/mysql/mysql.conf.d/mysqld.cnf
   4. restart mysql.service with "systemctl restart mysql.service"
   5. create new MYSQL user for API service 
      1. mysql -u root -p
      2. When asked for password, use the password you created during the node setup process
      3. Create user for remote access: \
        CREATE USER'username'@'%' IDENTIFIED BY'your_password';
      4. GRANT ALL PRIVILEGES ON*.*TO'username'@'%'WITH GRANT OPTION;
      5. FLUSH PRIVILEGES;
      6. update Edge node API .env with mysql connection details
6. make sure redis is running as configured in .env (default: 127.0.0.1:6379)
7. npm run start

## Dependencies
1. **Edge node Knowledge mining** - app is being used for input file processing and creating Knowledge asset content
2. **Edge node Authentication service** - app is being used as middleware for every API request to Edge node API - session/cookie is being validated. Also, app contains all important Edge node config params like Knowledge mining endpoint, DRAG endpoint, KMining pipeline id etc

App will be exposed on http://localhost:3002

***NOTE***: All Edge node API settings parameters are set in Edge node auth service app DB and this app will not work if Auth service is not running and properly set.

## OpenTelemetry

This service comes with OpenTelemetry support pre-installed. To enable it, set `OTEL_ENABLED=true` in .env variables.

OpenTelemetry is implemented using [@opentelemetry/auto-instrumentations-node](https://www.npmjs.com/package/@opentelemetry/auto-instrumentations-node) package, and can be further configured using env variables.
- Configuration: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/
- Set up exporters: https://opentelemetry.io/docs/specs/otel/protocol/exporter/
- Exporters + dashboard docker setup: https://hub.docker.com/r/grafana/otel-lgtm 
