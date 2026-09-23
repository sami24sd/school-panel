# ایمیج مبتنی بر Debian (glibc)؛ از Alpine استفاده نکنید چون workerd (زیر Miniflare) روی musl اجرا نمی‌شود
FROM node:20-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production PORT=8080 DATA_DIR=/data

# ابتدا فقط وابستگی‌ها تا لایه‌ی npm ci کش شود
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY . .
RUN mkdir -p /data

EXPOSE 8080
CMD ["node", "server.mjs"]
