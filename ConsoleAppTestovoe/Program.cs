using System;

namespace ConsoleAppTestovoe
{
    public class Program
    {
        static void Main(string[] args)
        {
            using (var sender = new MessageSender())
            {
                // Отправьте сообщения
                sender.SendMessage("A", "Сообщение для A");
                sender.SendMessage("B", "Сообщение для B");
            }

            using (var receiverA = new MessageReceiver("ReceiverA"))
            using (var receiverB = new MessageReceiver("ReceiverB"))
            {
                // Запустите получателей
                receiverA.StartReceiving();
                receiverB.StartReceiving();

                Console.WriteLine("Нажмите Enter для завершения! Хорошего дня!");
                Console.ReadLine();
            }
        }
    }
}
