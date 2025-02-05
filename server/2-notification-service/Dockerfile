# Stage 1: Builder stage
FROM node:21-alpine3.18 as builder

WORKDIR /app

# Copy package files and npm config
COPY package*.json ./
COPY tsconfig.json ./
COPY .npmrc ./

# Copy source and tools directories
COPY src ./src
COPY tools ./tools

# Inject NPM_TOKEN into .npmrc and install dependencies
ARG NPM_TOKEN
RUN sed -i "s/\${NPM_TOKEN}/$NPM_TOKEN/g" .npmrc && \
    npm install -g npm@latest && \
    npm ci && \
    npm run build && \
    rm -f .npmrc  # Remove .npmrc after installing dependencies

# Stage 2: Production stage
FROM node:21-alpine3.18

WORKDIR /app

# Install curl and PM2
RUN apk add --no-cache curl && \
    npm install -g pm2 npm@latest

# Copy package files and npm config
COPY package*.json ./
COPY tsconfig.json ./

# Install production dependencies
RUN npm ci --production

# Copy built files from the builder stage
COPY --from=builder /app/build ./build

# Expose the application port
EXPOSE 4001

# Start the application
CMD ["npm", "run", "start"]
