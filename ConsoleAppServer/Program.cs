using System;
using System.Text;
using System.Data.SqlClient;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading;

namespace ConsoleAppServer
{
    class Program
    {
        static void Main(string[] args)
        {        
            Thread reconnectThread = new Thread(new ThreadStart(ReconnectThread));
            reconnectThread.Start();
            Console.WriteLine("Для выхода нажмите хоть что-нибудь");
            Console.ReadLine();
            reconnectThread.Abort();
        }

        public static void ReconnectThread()
        {
            ClassForRabbit rabbit = new ClassForRabbit();
            //Подписка на брокер.
            if (rabbit.GetConnection())
              rabbit.DoSubscribe();
            while (true)
            {
                // Каждые 10 сек проверка на подписку.
                if (!rabbit.GetConnection())
                {
                    // Раз в секунду пытается подключится к брокеру.
                    while (rabbit.GetConnection()) 
                    {
                        Thread.Sleep(1000);
                    }
                    Console.WriteLine("Подключение к RabbitMQ восстановлено");
                    rabbit.DoSubscribe();
                }
                Thread.Sleep(10000);

            }
        }

            
        }        
     class ClassForRabbit
    {
        // Метод для проверки подключения к брокеру.
        // returns подключен/не подключен
        public bool GetConnection()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };
            try
            {
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        // Возвращаем состояние канала.                        
                        return channel.IsOpen;
                    }
                }
            }
            catch
            {
                Console.WriteLine("Подключение к RabbitMQ не установленно");
                return false;
            }

        }
        // Метод для подписки и прослушивания брокера.
        public void DoSubscribe()
        {
            try
            {
                ClassForkDataBase db = new ClassForkDataBase();
                var factory = new ConnectionFactory() { HostName = "localhost" };
                IConnection conn = factory.CreateConnection();

                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(queue: "example1",
                                             durable: true,
                                             exclusive: false,
                                             autoDelete: false,
                                             arguments: null);

                        channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                      

                        var consumer = new EventingBasicConsumer(channel);
                        consumer.Received += (sender, ea) =>
                        {
                            var body = ea.Body.ToArray();
                            var message = Encoding.UTF8.GetString(body);
                            db.AddStringAnsync(message);
                            Console.WriteLine("Полученное сообщение - {0}", message);                           
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        };
                        channel.BasicConsume(queue: "example1",
                                             autoAck: false,
                                             consumer: consumer);
                       
                        Console.ReadLine();
                    }
                }
            }
            catch
            {
                
            }
        }
    }   
        // Класс для работа с БД.
        class ClassForkDataBase
        {

            // Строка подключения к БД.           
            static string connectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=exampledb;Integrated Security=True";
            // Метод записи сообщения в бд. 
            // Принимает сообщения для запсии.    
            public async void AddStringAnsync(string message)
            {
                string date1 = DateTime.Now.ToShortTimeString();
                string sqlExpression = String.Format("INSERT INTO ServerDB (Message, Date) VALUES ('{0}', '{1}')", message, date1);
                try
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                    await connection.OpenAsync();
                    SqlCommand command = new SqlCommand(sqlExpression, connection);
                    await command.ExecuteNonQueryAsync();
                    }
                }
                catch
                {
                    Console.WriteLine("Нет подключения к БД.");
                }
        }

        }
    }


