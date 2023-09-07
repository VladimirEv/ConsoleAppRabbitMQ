using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleAppTestovoe
{
    //класс для получателей сообщений
    public class MessageReceiver : IDisposable
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;         //объект типа IModel, который представляет канал связи с RabbitMQ.
        private readonly string _queueName;
        //используется для хранения и отслеживания обработанных сообщений
        //HashSet<string> - это коллекция, предназначенная для хранения уникальных строковых значений
        //используется для хранения идентификаторов (или других уникальных ключей) обработанных сообщений
        private readonly HashSet<string> processedMessages = new HashSet<string>(); 


        public MessageReceiver(string queueName)
        {
            _queueName = queueName;

            var factory = new ConnectionFactory()
            {
                // Адрес сервера RabbitMQ (подключил через Docker; создал образ и контейнер)
                //строка подключения Docker: docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
                HostName = "localhost", 
                UserName = "guest",
                Password = "guest",
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            // Создаём обмен с именем 'messages_queue' и типом 'direct'
            _channel.ExchangeDeclare("messages_queue", ExchangeType.Direct, durable: true);

            // Объявляем очередь с именем, как в вашем коде
            _channel.QueueDeclare(queueName, durable: true, false, false, null);

            // Определяем, какие сообщения будет обрабатывать этот получатель
            var routingKey = queueName == "ReceiverA" ? "A" : "B";

            // Привязываем очередь к обмену с указанным ключом маршрутизации
            _channel.QueueBind(queueName, "messages_queue", routingKey);
        }


        // запускает получение сообщений из очереди
        public void StartReceiving()  
        {
            //объект, который слушает сообщения в очереди
            var consumer = new EventingBasicConsumer(_channel);

            //добавляется обработчик события Received, который вызывается, когда приходит
            //новое сообщение в очередь. В этом обработчике извлекается тело сообщения
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                //Проверим, было ли сообщение уже обработано
                if (!IsMessageProcessed(message))
                {
                    Console.WriteLine($"Получено сообщение '{message}' для получателя '{_queueName}'");

                    // Обрабатываем сообщение
                    Console.WriteLine($"Обработка сообщения:'{message}'.");

                    // Помечаем сообщение как обработанное
                    MarkMessageAsProcessed(message);
                }

                //говорит RabbitMQ о том, что сообщение с указанным delivery tag было успешно обработано
                //и может быть удалено из очереди;
                //ea.DeliveryTag - это уникальный идентификатор доставки (delivery tag) для сообщения, которое было успешно обработано;
                //ea здесь представляет объект BasicDeliverEventArgs, который содержит информацию о полученном сообщении, включая его delivery tag
                _channel.BasicAck(ea.DeliveryTag, false);
            };

            //Когда вызываем _channel.BasicConsume, потребитель начинает слушать очередь _queueName
            //и ожидает получения новых сообщений;
            //BasicConsume - это метод IModel, который инициирует потребление сообщений из очереди;
            //consumer - это объект, который реализует интерфейс IBasicConsumer и представляет собой потребителя сообщений.
            //Он содержит обработчики событий для обработки полученных сообщений, в данном случае,
            //обработчик события Received для выполнения логики при получении нового сообщения.
            _channel.BasicConsume(_queueName, false, consumer);
        }


        private bool IsMessageProcessed(string message)
        {
            lock (processedMessages)
            {
                return processedMessages.Contains(message);
            }
        }

        private void MarkMessageAsProcessed(string message)
        {
            lock (processedMessages)
            {
                processedMessages.Add(message);
            }
        }


        public void Dispose()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}
