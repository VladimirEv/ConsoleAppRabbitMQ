using RabbitMQ.Client;
using System;
using System.Text;

namespace ConsoleAppTestovoe
{
    //класс для отправителя сообщений
    public class MessageSender : IDisposable
    {
        //хранит соединение c RabbitMQ
        private readonly IConnection _connection;

        //хранит канал для взаимодействия с брокером
        private readonly IModel _channel;          

        public MessageSender()
        {
            var factory = new ConnectionFactory()
            {
                // Адрес сервера RabbitMQ (подключил через Docker; создал образ и контейнер)
                //строка подключения Docker: docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
                HostName = "localhost", 
                UserName = "guest",
                Password = "guest",
            };

            //создаём соединение для взаимодействия с RabbitMQ
            _connection = factory.CreateConnection();

            //создаём канал для взаимодействия с RabbitMQ (канал используется для отправки и получения сообщений)
            _channel = _connection.CreateModel();     
        }

        //метод, который используется для отправки сообщений в очередь
        public void SendMessage(string messageType, string message) 
        {
            //чтобы объявить очередь с именем "messages_queue".
            //Если очередь не существует, она будет создана.
            _channel.QueueDeclare("messages_queue", durable: true, false, false, null);  
            var properties = _channel.CreateBasicProperties();

            //чтобы сделать сообщение устойчивым (т.е., сохранять его на диске и гарантировать доставку после перезапуска сервера)
            properties.Persistent = true;   

            // Определите, какой получатель должен получить сообщение
            var routingKey = messageType == "A" ? "A" : "B";

            //отправляем сообщение в очередь;
            //сообщение передается как байтовый массив после его кодирования из строки
            _channel.BasicPublish("", "messages_queue", properties, Encoding.UTF8.GetBytes(message));  
            Console.WriteLine($"Отправлено сообщение '{message}' типа '{messageType}'");
        }


        //метод, реализующий интерфейс IDisposable
        //закрывает соединение и канал после использования
        //освобождает ресурсы после использования "MessageSender"
        public void Dispose() 
        {
            _channel.Close();
            _connection.Close();
        }
    }
}
