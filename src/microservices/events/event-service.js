// event-service.js

const express = require('express');
const { Kafka } = require('kafkajs');

// --- Конфигурация ---
const KAFKA_BROKERS = ['localhost:9092']; // Замените на адрес вашего Kafka
const TOPICS = {
    USER: 'users-events',
    MOVIE: 'movies-events',
    PAYMENT: 'payments-events'
};
const PRODUCER_CLIENT_ID = 'foodcinema-event-producer';
const CONSUMER_GROUP_ID = 'foodcinema-event-consumer-group';
const API_PORT = 3001;

// --- Инициализация Kafka ---
const kafka = new Kafka({
    clientId: PRODUCER_CLIENT_ID,
    brokers: KAFKA_BROKERS,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: CONSUMER_GROUP_ID });

// --- Producer: Функция отправки сообщения ---
/**
 * Отправляет сообщение в указанный топик Kafka.
 * @param {string} topic - Имя топика.
 * @param {object} payload - Данные события.
 */
const sendMessage = async (topic, payload) => {
    const value = JSON.stringify({
        timestamp: new Date().toISOString(),
        ...payload
    });

    try {
        await producer.send({
            topic: topic,
            messages: [
                { value: value },
            ],
        });
        console.log(`[PRODUCER] Успешно отправлено событие в топик ${topic}: ${value.substring(0, 50)}...`);
        return true;
    } catch (error) {
        console.error(`[PRODUCER ERROR] Не удалось отправить сообщение в топик ${topic}:`, error);
        return false;
    }
};


// --- Consumer: Обработчик событий ---
const runConsumer = async () => {
    // 1. Подключение и подписка
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.USER, fromBeginning: true });
    await consumer.subscribe({ topic: TOPICS.MOVIE, fromBeginning: true });
    await consumer.subscribe({ topic: TOPICS.PAYMENT, fromBeginning: true });

    console.log(`[CONSUMER] Подписка на топики: ${Object.values(TOPICS).join(', ')}`);

    // 2. Обработка входящих сообщений
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                const key = message.key ? message.key.toString() : 'No Key';

                // --- Логика обработки событий (MVP: запись в лог) ---
                console.log(`\n--- [CONSUMER] Обработка События ---`);
                console.log(` Топик: ${topic}`);
                console.log(` Партиция: ${partition}`);
                console.log(` Ключ: ${key}`);
                console.log(` Смещение: ${message.offset}`);
                console.log(` Тип события: ${event.eventType || 'N/A'}`);
                console.log(` Данные:`, event);
                console.log(`------------------------------------\n`);

                // Здесь в реальном приложении была бы сложная логика:
                // - Обновление локальной базы данных (Saga pattern)
                // - Отправка уведомлений
                // - Вызов других микросервисов
                // - и т.д.
                // --------------------------------------------------------

            } catch (error) {
                console.error(`[CONSUMER ERROR] Ошибка обработки сообщения:`, error);
            }
        },
    });
};

// --- API (Express Server) ---
const app = express();
app.use(express.json());

// Роут для создания события User
app.post('/api/events/user', async (req, res) => {
    const success = await sendMessage(TOPICS.USER, {
        eventType: 'USER_CREATED',
        userId: req.body.userId || 101,
        username: req.body.username || 'test_user',
        email: req.body.email || 'user@example.com'
    });

    if (success) {
        res.status(202).json({ status: 'Accepted', message: `Event sent to ${TOPICS.USER}` });
    } else {
        res.status(500).json({ status: 'Error', message: 'Failed to send event' });
    }
});

// Роут для создания события Movie
app.post('/api/events/movie', async (req, res) => {
    const success = await sendMessage(TOPICS.MOVIE, {
        eventType: 'MOVIE_RATING_UPDATED',
        movieId: req.body.movieId || 505,
        newRating: req.body.newRating || 4.7
    });

    if (success) {
        res.status(202).json({ status: 'Accepted', message: `Event sent to ${TOPICS.MOVIE}` });
    } else {
        res.status(500).json({ status: 'Error', message: 'Failed to send event' });
    }
});

// Роут для создания события Payment
app.post('/api/events/payment', async (req, res) => {
    const success = await sendMessage(TOPICS.PAYMENT, {
        eventType: 'PAYMENT_SUCCESSFUL',
        paymentId: req.body.paymentId || 'TXN-9876',
        amount: req.body.amount || 9.99,
        userId: req.body.userId || 101
    });

    if (success) {
        res.status(202).json({ status: 'Accepted', message: `Event sent to ${TOPICS.PAYMENT}` });
    } else {
        res.status(500).json({ status: 'Error', message: 'Failed to send event' });
    }
});

// --- Инициализация и Запуск Сервиса ---
const initializeService = async () => {
    try {
        // Подключение Producer
        await producer.connect();
        console.log('[KAFKA] Producer подключен.');

        // Запуск Consumer
        await runConsumer();
        
        // Запуск API
        app.listen(API_PORT, () => {
            console.log(`[API] Event Service запущен на порту ${API_PORT}`);
            console.log(`[INFO] Теперь вы можете отправлять POST-запросы на /api/events/[user/movie/payment]`);
        });

    } catch (error) {
        console.error('[FATAL] Не удалось запустить Event Service:', error);
        process.exit(1);
    }
};

initializeService();

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n[SHUTDOWN] Отключение Kafka Producer и Consumer...');
    await producer.disconnect();
    await consumer.disconnect();
    console.log('[SHUTDOWN] Сервисы отключены. Выход.');
    process.exit(0);
});