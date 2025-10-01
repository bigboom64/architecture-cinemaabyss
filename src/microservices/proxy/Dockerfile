# --- Этап 1: сборка зависимостей ---
FROM node:18-alpine AS builder

WORKDIR /app

# Копируем package.json и package-lock.json (если есть)
COPY package*.json ./

# Устанавливаем зависимости
RUN npm install --only=production

# Копируем исходный код
COPY . .

# --- Этап 2: минимальный рантайм ---
FROM node:18-alpine

WORKDIR /app

# Копируем только собранное приложение и node_modules из builder
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/proxy-server.js ./proxy-server.js

# Экспонируем порт прокси
EXPOSE 3000

# Запуск прокси-сервера
CMD ["node", "proxy-server.js"]
