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
        private readonly IModel _channel;
        private readonly string _queueName;
       
        public MessageReceiver(string queueName)
        {
            _queueName = queueName;

            var factory = new ConnectionFactory()
            {
                HostName = "localhost", // Адрес сервера RabbitMQ
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
            //новое сообщение в очередь. В этом обработчике извлекается тело сообщения,
            //декодируется в строку и выводится в консоль
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Получено сообщение '{message}' для получателя '{_queueName}'");


                _channel.BasicAck(ea.DeliveryTag, false);
            };

            _channel.BasicConsume(_queueName, false, consumer);
        }

        public void Dispose()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}
