FROM node:22-bookworm-slim

# libhwloc15 is a runtime dependency of the native Apache Iggy server the runner starts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates libhwloc15 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY . .
RUN npm ci --prefix foreign/typescript \
    && npm run build --prefix foreign/typescript \
    && npm ci --prefix bdd/typescript

WORKDIR /work/bdd/typescript
CMD ["npm", "test"]
