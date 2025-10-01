// proxy-server.js

const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

// --- Конфигурация Сервисов ---
const MONOLITH_HOST = 'http://localhost:8080'; // Адрес вашего монолита
const MOVIE_MICROSERVICE_HOST = 'http://localhost:8081'; // Адрес вашего микросервиса movies

// --- ФИЧЕ-ФЛАГ для Strangler Fig ---
// Используется для постепенного переключения трафика.
// 0.0 - весь трафик на монолит.
// 1.0 - весь трафик на микросервис.
// 0.5 - 50% трафика на микросервис, 50% на монолит (пример).
const MOVIE_SERVICE_CUTOVER_PERCENT = parseFloat(process.env.MOVIE_CUTOVER_PERCENT) || 0.0;
console.log(`[PROXY] Процент перехода для /api/movies: ${MOVIE_SERVICE_CUTOVER_PERCENT * 100}%`);

// --- Маршруты и Настройки Прокси ---
const app = express();
const PORT = 3000;

/**
 * Функция для выбора целевого сервиса на основе фиче-флага.
 * @param {string} monolithHost - Хост монолита.
 * @param {string} microserviceHost - Хост микросервиса.
 * @param {number} cutoverPercent - Процент трафика для переключения на микросервис (0.0 до 1.0).
 * @returns {string} Выбранный хост.
 */
const chooseTarget = (monolithHost, microserviceHost, cutoverPercent) => {
    // Генерируем случайное число от 0 до 1
    const randomValue = Math.random();

    // Если случайное число меньше или равно проценту перехода,
    // отправляем на микросервис.
    if (randomValue <= cutoverPercent) {
        return microserviceHost;
    }

    // Иначе отправляем на монолит
    return monolithHost;
};

// 1. Проксирование Сервиса Movies с использованием Strangler Fig (Фиче-Флаг)
// Этот маршрут перехватывает все запросы к /api/movies
app.use('/api/movies', (req, res, next) => {
    // 1. Выбираем целевой хост
    const target = chooseTarget(MONOLITH_HOST, MOVIE_MICROSERVICE_HOST, MOVIE_SERVICE_CUTOVER_PERCENT);

    console.log(`[PROXY /api/movies] Запрос на ${req.method} ${req.originalUrl}. Цель: ${target}`);

    // 2. Создаем прокси-мидлвар с выбранной целью
    const moviesProxy = createProxyMiddleware({
        target: target,
        changeOrigin: true, // Изменяет заголовок Origin на целевой URL
        // pathRewrite: {
        //     // Если бы маршрут в микросервисе отличался от прокси-маршрута
        //     // '^/api/movies': '/v1/movies',
        // },
        onProxyReq: (proxyReq, req, res) => {
            // Добавление заголовков для трассировки, если нужно
            proxyReq.setHeader('X-Proxy-Target', target);
        },
        onError: (err, req, res) => {
             console.error(`[PROXY ERROR] Ошибка при проксировании запроса к ${target}${req.originalUrl}:`, err);
             res.status(503).send('Service Unavailable');
        }
    });

    // 3. Вызываем прокси-мидлвар для обработки запроса
    moviesProxy(req, res, next);
});

// 2. Проксирование остальных сервисов (пока еще на монолите)
// Маршруты /api/users, /api/payments, /api/subscriptions
const defaultProxyOptions = {
    target: MONOLITH_HOST,
    changeOrigin: true,
    onProxyReq: (proxyReq, req, res) => {
        proxyReq.setHeader('X-Proxy-Target', MONOLITH_HOST);
    },
    onError: (err, req, res) => {
        console.error(`[PROXY ERROR] Ошибка при проксировании запроса к монолиту ${req.originalUrl}:`, err);
        res.status(503).send('Service Unavailable');
   }
};

app.use('/api/users', createProxyMiddleware(defaultProxyOptions));
app.use('/api/payments', createProxyMiddleware(defaultProxyOptions));
app.use('/api/subscriptions', createProxyMiddleware(defaultProxyOptions));

// 3. Healthcheck и другие корневые маршруты
app.use('/', createProxyMiddleware(defaultProxyOptions));

// --- Запуск Сервера ---
app.listen(PORT, () => {
    console.log(`[PROXY] API Gateway запущен на порту ${PORT}.`);
    console.log(`[PROXY] Запросы к /api/movies распределяются между Monolith (${MONOLITH_HOST}) и Microservice (${MOVIE_MICROSERVICE_HOST})`);
    console.log(`[PROXY] Остальные запросы идут на Monolith (${MONOLITH_HOST})`);
});