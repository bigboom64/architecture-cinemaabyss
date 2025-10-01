# --- Этап 1: установка зависимостей ---
FROM node:18-alpine AS builder

WORKDIR /app

# Копируем package.json и package-lock.json (если есть)
COPY package*.json ./

# Устанавливаем только production-зависимости
RUN npm install --only=production

# Копируем исходный код
COPY . .

# --- Этап 2: минимальный образ для запуска ---
FROM node:18-alpine

WORKDIR /app

# Копируем node_modules и исходники из builder
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/event-service.js ./event-service.js

# Экспонируем API порт (3001)
EXPOSE 3001

# Запускаем Event Service
CMD ["node", "event-service.js"]
