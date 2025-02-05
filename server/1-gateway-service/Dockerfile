# Stage 1: Builder
FROM node:21-alpine3.18 as builder

WORKDIR /app

# Copy package files and npm config
COPY package*.json ./
COPY tsconfig.json ./
COPY .npmrc ./

# Inject NPM_TOKEN into .npmrc
ARG NPM_TOKEN
RUN sed -i "s/\${NPM_TOKEN}/$NPM_TOKEN/g" .npmrc && \
    npm install && \
    npm run build && \
    rm -f .npmrc  # Remove .npmrc after installing dependencies

# Stage 2: Production
FROM node:21-alpine3.18

WORKDIR /app

# Install PM2 and dependencies
RUN apk add --no-cache curl && \
    npm install -g pm2 npm@latest

# Copy production files
COPY package*.json ./
COPY tsconfig.json ./
RUN npm ci --production

# Copy built files from the builder stage
COPY --from=builder /app/build ./build

EXPOSE 4000
CMD [ "npm", "run", "start" ]
