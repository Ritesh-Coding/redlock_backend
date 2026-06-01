FROM node:20-alpine
WORKDIR /app

# Install curl for healthcheck support
RUN apk add --no-cache curl

# Copy dependencies
COPY package*.json ./
RUN npm install --only=production

# Copy source code
COPY . .

EXPOSE 5000

ENV NODE_ENV=production
ENV PORT=5000

CMD ["node", "index.js"]
