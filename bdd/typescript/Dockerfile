FROM node:22-bookworm-slim

WORKDIR /work
COPY . .
RUN npm ci --prefix foreign/typescript \
    && npm run build --prefix foreign/typescript \
    && npm ci --prefix bdd/typescript

WORKDIR /work/bdd/typescript
CMD ["npm", "test"]
